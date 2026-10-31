"""``Database``: a table registry plus a connection pool, addressed by the
variable name a page's ``data.py`` binds it to (the ATTACH alias). Bare instances
(``sales = Database()``), no URL; served over the page's ``/<slug>/ws`` socket.
The same object bound in two pages stays one object with one pool.
"""

import asyncio
import inspect
import logging
import sys
import traceback
from contextlib import nullcontext
from pathlib import Path
from types import ModuleType
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import duckdb
import pyarrow as pa
from fastapi import FastAPI
from virtual_catalog.bridge import Policy
from n6k_server.pump import WsReject
from virtual_catalog.rpc_stream import RpcEntryFn, StreamingRpc, register_rpc_streams

from n6k_app.connections import ConnectionPool, ConnectionSpec
from n6k_app.formats import to_arrow, to_record_batches
from n6k_app.members import Rpc, Source, View
from n6k_app.session import attach_catalog, create_serving_conn, finalize_serving_conn
from n6k_app.session_state import Session
from n6k_app.table_registry import RpcSpec, TableRegistry, resolve_return_schema

_log = logging.getLogger(__name__)

# DuckDB rejects these as ATTACH aliases.
_RESERVED_CATALOGS = frozenset({"memory", "temp", "system", "main"})


def catalog_name_error(name: str | None) -> str | None:
    """Error message if *name* can't be a DuckDB ATTACH catalog, else None."""
    if not name:
        return "catalog name is empty"
    if name in _RESERVED_CATALOGS:
        return f"catalog {name!r} is reserved by DuckDB (main/memory/temp/system)"
    return None


# {name: (entry, schema)}; entry is a finite fn or a StreamingRpc.
RpcRegistry = dict[str, tuple[RpcEntryFn, pa.Schema]]


def _no_data_error(db: "Database") -> str:
    return (
        f"@db.data() is not available on a {type(db).__name__}: its catalog is a bridge over the upstream, "
        "which cannot hold provider tables. Use @db.view() for derived SQL, @db.rpc() for computed results, "
        "or bind a separate Database() for in-process data."
    )


class DatabaseSchemaHandle:
    def __init__(self, db: "Database", schema: str):
        self._db = db
        self._schema = schema

    def data(
        self,
        name: str | None = None,
        *,
        primary_keys: tuple[str, ...] | None = None,
        schema: pa.Schema | None = None,
    ) -> "Callable[[Callable[..., Any]], Source]":
        s = self._schema

        if not self._db.allows_data:
            raise TypeError(_no_data_error(self._db))

        def decorator(fn: Callable[..., Any]) -> Source:
            resolved = name if name is not None else fn.__name__
            arrow_schema = schema or resolve_return_schema(fn, None)
            self._db.table_registry.register_source(
                resolved,
                fn,
                schema=s,
                arrow_schema=arrow_schema,
                primary_keys=primary_keys,
            )
            return Source(self._db, resolved, s)

        return decorator

    def view(self, name: str | None = None) -> "Callable[[Callable[..., Any]], View]":
        schema = self._schema

        def decorator(fn: Callable[..., Any]) -> View:
            resolved = name if name is not None else fn.__name__
            sql = fn()
            if not isinstance(sql, str):
                raise TypeError(f"@db.{schema}.view('{resolved}') must return a SQL string, got {type(sql).__name__}")
            self._db.table_registry.register_view(resolved, sql.strip(), schema=schema)
            return View(self._db, resolved, schema)

        return decorator


class Database:
    """A data provider: table/RPC registrations plus a connection pool.

    Constructed bare with no side effects; the name it answers to is read back off
    the page namespace that binds it (``page_databases``), never stamped on.
    """

    allows_data = True

    def __init__(self) -> None:
        self.table_registry = TableRegistry()
        self.connection_pool = ConnectionPool()
        # No request-time state lives here — upstream-open state is on the pool.

    async def attach(self, con: duckdb.DuckDBPyConnection, *, catalog: str, session: Session) -> None:
        """Attach this database to ``con`` under ``catalog``, RPCs and all.

        ``catalog`` is the caller's ATTACH alias — the page socket passes the local
        name the database is served under. ``con`` is the connection about to be
        served, shared with every other database the same socket carries.
        """
        # Open the shared upstream lazily/once, off-thread so the probe never blocks
        # the loop. A recorded failure surfaces per session; other pages keep working.
        await asyncio.to_thread(self.connection_pool.ensure_started)
        if self._start_error is not None:
            raise self._connection_unavailable_error()
        loop = asyncio.get_running_loop()
        await asyncio.to_thread(
            attach_catalog,
            con,
            self.table_registry,
            self.connection_pool,
            catalog=catalog,
            main_loop=loop,
        )
        # The reactor resolves an RPC name against these before the catalog, and drives
        # a streaming one on `loop`. One Session per catalog, shared by its every call.
        await asyncio.to_thread(
            register_rpc_streams,
            con,
            catalog=catalog,
            registry=self.rpc_registry(session),
            main_loop=loop,
        )

    def data(
        self,
        name: str | None = None,
        *,
        primary_keys: tuple[str, ...] | None = None,
        schema: pa.Schema | None = None,
    ) -> "Callable[[Callable[..., Any]], Source]":
        """Register a data source in the main schema. Shorthand for @db.main.data()."""
        return DatabaseSchemaHandle(self, "main").data(name, primary_keys=primary_keys, schema=schema)

    def view(self, name: str | None = None) -> "Callable[[Callable[..., Any]], View]":
        """Register a view in the main schema. Shorthand for @db.main.view()."""
        return DatabaseSchemaHandle(self, "main").view(name)

    def rpc[R](
        self, name: str | None = None, *, returns: pa.Schema | None = None
    ) -> Callable[[Callable[..., R]], Rpc[R]]:
        """Register a standalone (parentless) RPC.

        Finite: return a table annotated ``-> Table[Schema]``, coerced to it.
        Streaming: ``yield`` from an async/sync generator annotated
        ``-> AsyncIterator[Schema]``; each yield is coerced and streamed, and the
        generator may never end. A streaming RPC must be consumed over a
        pipeline-able query with a streaming cursor.
        """

        def decorator(fn: Callable[..., R]) -> Rpc[R]:
            resolved = name if name is not None else fn.__name__
            schema = resolve_return_schema(fn, returns)
            self.table_registry.register_rpc(resolved, fn, parent_table=None, return_schema=schema)
            return Rpc(self, resolved)

        return decorator

    def __getattr__(self, name: str) -> DatabaseSchemaHandle:
        return DatabaseSchemaHandle(self, name)

    def rpc_registry(self, session: Session | None = None) -> RpcRegistry:
        """Driver ``RpcRegistry`` adapting each ``@rpc`` fn to the ``(scalars,
        table)`` contract: scalars bind by name, a ``Session`` param gets
        ``session``, a parent-bound RPC gets the parent table as its first arg.

        ``session`` defaults to a throwaway ``Session`` for callers (e.g. tests)
        outside a live connection.
        """
        session = session if session is not None else Session()
        reg: RpcRegistry = {}
        for name, spec in self.table_registry._rpcs.items():
            # register_rpc guarantees a schema; assert rather than silently skip.
            assert spec.return_schema is not None, f"RPC {name!r} reached rpc_registry() without a return schema"
            is_streaming = inspect.isasyncgenfunction(spec.fn) or inspect.isgeneratorfunction(spec.fn)
            if spec.native_db_type is not None:
                # Fail fast at handler build: a Connection param needs a matching upstream.
                self.connection_pool.sole_spec(spec.native_db_type)
            if is_streaming:
                reg[name] = (
                    StreamingRpc(self._streaming_rpc_fn(spec, session)),
                    spec.return_schema,
                )
            else:
                reg[name] = (self._rpc_fn(spec, session), spec.return_schema)
        return reg

    @staticmethod
    def _bind_rpc_kwargs(
        spec: RpcSpec, scalars: list[Any], session: Session, native_conn: Any = None
    ) -> dict[str, Any]:
        """Bind SQL call args to the RPC's scalar params by name, injecting
        ``Session`` and a real driver-native ``Connection`` if requested (each by
        name, so it can sit anywhere in the signature)."""
        kwargs = dict(zip(spec.scalar_params, scalars))
        if spec.session_param is not None:
            kwargs[spec.session_param] = session
        if spec.native_param is not None:
            kwargs[spec.native_param] = native_conn
        return kwargs

    def _rpc_fn(self, spec: RpcSpec, session: Session) -> Callable[..., pa.Table]:
        registry = self.table_registry
        pool = self.connection_pool

        def fn(scalars: list[Any], table: pa.Table | None) -> pa.Table:
            # A Connection param lends the rpc a real connection for the call (the
            # rpc owns its transaction); otherwise a no-op context. Gating on
            # native_db_type (set iff native_param is) also narrows it to str.
            cm = pool.native_checkout(spec.native_db_type) if spec.native_db_type is not None else nullcontext(None)
            with cm as native_conn:
                kwargs = self._bind_rpc_kwargs(spec, scalars, session, native_conn)
                # Coerce the return to the declared schema (accepts a plain DataFrame).
                if spec.parent_table:
                    parent_df = registry.load_table(spec.parent_table).to_pandas()
                    return to_arrow(spec.fn(parent_df, **kwargs), spec.return_schema)
                return to_arrow(spec.fn(**kwargs), spec.return_schema)

        return fn

    def _streaming_rpc_fn(
        self, spec: RpcSpec, session: Session
    ) -> Callable[[list[Any], Optional[pa.Table]], AsyncIterator[pa.RecordBatch]]:
        """Adapt a generator RPC (async or sync) to the streaming contract: each
        yield is coerced to record batches and streamed lazily (may never end)."""
        registry = self.table_registry
        pool = self.connection_pool
        schema = spec.return_schema
        assert schema is not None
        is_async = inspect.isasyncgenfunction(spec.fn)

        async def fn(scalars: list[Any], table: pa.Table | None) -> AsyncIterator[pa.RecordBatch]:
            # A Connection param lends the rpc a real connection held open across the
            # whole stream (the generator uses it while it yields); the rpc owns its
            # transaction. Otherwise a no-op context.
            cm = pool.native_checkout(spec.native_db_type) if spec.native_db_type is not None else nullcontext(None)
            with cm as native_conn:
                kwargs = self._bind_rpc_kwargs(spec, scalars, session, native_conn)
                if spec.parent_table:
                    parent_df = registry.load_table(spec.parent_table).to_pandas()
                    gen = spec.fn(parent_df, **kwargs)
                else:
                    gen = spec.fn(**kwargs)

                if is_async:
                    async for item in gen:
                        for batch in to_record_batches(item, schema):
                            yield batch
                    return

                # Sync generator: pull items off-thread so a blocking producer doesn't
                # stall the loop; close() in finally runs its cleanup on teardown.
                sentinel = object()
                try:
                    while True:
                        item = await asyncio.to_thread(next, gen, sentinel)
                        if item is sentinel:
                            return
                        for batch in to_record_batches(item, schema):
                            yield batch
                finally:
                    gen.close()

        return fn

    @property
    def _start_error(self) -> BaseException | None:
        """Upstream-open failure, owned by the pool."""
        return self.connection_pool._start_error

    def start(self) -> None:
        """A failure is recorded, not raised, so one unreachable upstream never
        crashes boot; ``attach`` surfaces it per session."""
        self.connection_pool.ensure_started()
        if self._start_error is not None:
            _log.warning(
                "A database could not open its upstream at start; its pages will "
                "surface this on connect while other databases keep serving."
            )

    def _connection_unavailable_error(self) -> RuntimeError:
        """Error surfaced to a session when start() failed, carrying the original
        traceback so the browser shows what actually went wrong."""
        assert self._start_error is not None
        tb = "".join(traceback.format_exception(self._start_error)).rstrip()
        return RuntimeError(
            "This database could not open its external connection at startup, so "
            "this page can't run queries. Fix the connection in its data.py and "
            f"reload.\n\n{tb}"
        )

    def close(self) -> None:
        self.connection_pool.close()


def _reject_rpc_collisions(databases: dict[str, "Database"]) -> None:
    """Refuse a set of databases that share an RPC name, before any of it is served.

    ``register_rpc_streams`` keys the driver's host-RPC table by function name
    alone, while the reactor resolves per catalog — so two databases on one
    connection that both define ``forecast`` collapse to whichever registered
    last, and a session asking its own catalog silently gets the other one's
    answer. Nothing downstream can tell that happened, which is why this refuses
    up front rather than serving. Lift it once the driver keys by
    ``(catalog, name)``.
    """
    owners: dict[str, list[str]] = {}
    for alias, db in databases.items():
        for name in db.table_registry.rpc_names:
            owners.setdefault(name, []).append(alias)
    if clashes := {name: sorted(a) for name, a in owners.items() if len(a) > 1}:
        detail = "; ".join(f"'{name}' in {aliases}" for name, aliases in sorted(clashes.items()))
        raise ValueError(
            f"These databases are served over one socket and define the same RPC name ({detail}). "
            f"The duckdb driver resolves a host RPC by name alone, so one would answer for the "
            f"other. Rename it, e.g. @db.rpc('sales_forecast')."
        )


async def open_serving_conn(databases: dict[str, "Database"]) -> duckdb.DuckDBPyConnection:
    """The empty connection ``serve_and_close_connection`` takes, once ``databases`` is
    known to be servable; ``serving_authenticator(databases, sessions)`` attaches them onto it.

    One connection rather than one per catalog because the reactor's workers open
    their own connections on this ``DatabaseInstance``, and a catalog attached to some
    other connection would be invisible to them. Ownership passes to the caller, which must
    hand it to ``serve_and_close_connection`` (it closes the connection) or close it itself.
    """
    for alias in databases:
        if problem := catalog_name_error(alias):
            raise ValueError(f"database bound as '{alias}' cannot be served: {problem}")
    _reject_rpc_collisions(databases)
    return await asyncio.to_thread(create_serving_conn)


def serving_sessions(databases: dict[str, "Database"], con: duckdb.DuckDBPyConnection) -> dict[str, Session]:
    """One ``Session`` per catalog for a serving connection, shared by its RPCs and actions."""
    return {alias: Session(catalog=alias, connection=con) for alias in databases}


def serving_authenticator(
    databases: dict[str, "Database"], sessions: dict[str, Session]
) -> Callable[..., Awaitable[list[str]]]:
    """``authenticate`` for an unauthenticated server: any token is accepted and every
    database is attached under the alias that binds it, the alias a client names in its
    ``FT_OPEN``. ``sessions`` is the connection's ``{alias: Session}``."""

    async def authenticate(_ws: Any, con: duckdb.DuckDBPyConnection, _token: str, **_params: str) -> list[str]:
        try:
            for alias, db in databases.items():
                await db.attach(con, catalog=alias, session=sessions[alias])
            bridged = any(db.connection_pool.has_connections for db in databases.values())
            await asyncio.to_thread(finalize_serving_conn, con, bridged=bridged)
        except Exception as exc:
            _log.exception("could not attach %s", sorted(databases))
            raise WsReject(code=1011, reason=str(exc)[:120]) from exc
        return list(databases)

    return authenticate


def policy(table: str, verb: str, using: str, check: str | None = None) -> Policy:
    """One grant on one upstream table, replayed as a ``bridge_set_policy`` call.

    ``table`` is ``schema.table`` on the upstream; ``verb`` is
    ``select``/``insert``/``update``/``delete``/``alter``; ``using`` says which
    rows the verb may reach and ``check`` what it may write. The per-verb rules
    are the extension's and it raises them at bind time.
    """
    parts = table.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"policy table {table!r} must be fully qualified as 'schema.table' (e.g. 'public.users')")
    if check is None:
        return Policy(table, verb, using)
    if verb == "insert":
        return Policy(table, verb, {"check": check})
    return Policy(table, verb, {"using": using, "check": check})


def primary_key(table: str, *columns: str) -> tuple[str, tuple[str, ...]]:
    """Declare ``table``'s key where the upstream catalog entry carries none.

    A real table's ``PRIMARY KEY`` is discovered, so this is for views and table
    functions, which ``update`` and ``delete`` otherwise cannot address.
    """
    if not columns:
        raise ValueError(f"primary_key({table!r}) needs at least one column")
    return table, columns


class _UpstreamDatabase(Database):
    """A ``Database`` whose source tables come from a live external upstream.

    Subclasses set ``_db_type``; the shared ``__init__`` builds a
    ``ConnectionSpec`` from ``policies=`` and registers it into this database's own
    connection pool. Its catalog is a bridge, so ``@view``/``@rpc`` carry over but
    ``@data`` does not — a provider table cannot live in a bridge catalog.
    """

    _db_type: str = ""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        policies: list[Policy],
        primary_keys: list[tuple[str, tuple[str, ...]]] | None = None,
        settings: dict[str, str] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__()
        if not policies:
            raise ValueError("policies cannot be empty — an upstream database with no grants serves nothing")
        spec = ConnectionSpec(
            name=name if name is not None else database,
            db_type=self._db_type,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            policies=[*policies, *(Policy(table, "primary_key", list(cols)) for table, cols in primary_keys or [])],
            settings=settings or {},
        )
        self.connection_pool.register(spec)

    allows_data = False


class PostgresDatabase(_UpstreamDatabase):
    """A live Postgres database, attached as a page catalog.

        pg = PostgresDatabase(
            host="localhost", database="n6k_demo", user="postgres",
            policies=[
                policy("public.users", "select", "true"),
                policy("public.orders", "select", "tenant_id = 42"),
                policy("public.orders", "update", "tenant_id = 42"),
            ],
        )

    Granted tables reach the page as ``pg.<schema>.<table>`` (e.g.
    ``pg.public.users``). ``@pg.view()`` / ``@pg.rpc()`` register derived results
    that land at ``pg.<name>``; ``@pg.data()`` does not exist on an upstream.
    """

    _db_type = "postgres"

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        database: str,
        policies: list[Policy],
        primary_keys: list[tuple[str, tuple[str, ...]]] | None = None,
        settings: dict[str, str] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            policies=policies,
            primary_keys=primary_keys,
            settings=settings,
            name=name,
        )


class MysqlDatabase(_UpstreamDatabase):
    """A live MySQL database, attached as a page catalog (see ``PostgresDatabase``).

    MySQL has no schema layer distinct from the database, so a policy table's
    schema segment is the MySQL database name, e.g. ``policy("shop.orders", ...)``.
    """

    _db_type = "mysql"

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str,
        policies: list[Policy],
        primary_keys: list[tuple[str, tuple[str, ...]]] | None = None,
        settings: dict[str, str] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            policies=policies,
            primary_keys=primary_keys,
            settings=settings,
            name=name,
        )


def page_databases(module: ModuleType) -> dict[str, "Database"]:
    """The ``{varname: Database}`` a data.py binds at top level (defined or
    imported), keyed by local variable name = the ATTACH alias. An aliased
    instance keeps its first name; the same object may differ by name per page."""
    seen: set[int] = set()
    out: dict[str, Database] = {}
    for name, obj in vars(module).items():
        if isinstance(obj, Database) and id(obj) not in seen:
            seen.add(id(obj))
            out[name] = obj
    return out


def page_databases_by_slug(app: FastAPI, app_pkg: str) -> dict[str, dict[str, "Database"]]:
    """``{slug: {varname: Database}}`` for every discovered page, read from each
    page's already-imported data module (``<app_pkg>.<slug>.data``)."""
    out: dict[str, dict[str, Database]] = {}
    for ep_key in app.state._n6k_entry_points:
        slug = ep_key.split("/", 1)[1] if "/" in ep_key else ep_key
        leaf = "data" if slug == "index" else f"{slug.replace('/', '.')}.data"
        module = sys.modules.get(f"{app_pkg}.{leaf}")
        if module is None:
            continue
        if dbs := page_databases(module):
            out[slug] = dbs
    return out


def catalogs_by_slug(app_dir: "Path", app_pkg: str) -> dict[str, list[str]]:
    """``{slug: [alias, ...]}`` — every endpoint serving catalogs, and which.

    The build's capability table: the aliases each ``/<slug>/ws`` socket serves, so the
    build can resolve a page's ``attach:`` and fail on a typo rather than a runtime 4404
    close from ``page_ws``.

    Keyed by *data* directory (a ``data.py`` with no ``page.*`` still has a socket).
    No identity de-dup, unlike ``page_databases``: one Database under two names is
    served under both, so both must be listed or a valid ``attach:`` gets rejected.

    TODO(review): the app root is keyed by ``"index"``, which collides with a
    literal ``index/`` dir and is ``/`` over HTTP but ``/index/ws`` over WS.
    Worth unpicking separately.
    """
    from nextpy_server.discover import _find_data_files, _slug_for_data_file

    out: dict[str, list[str]] = {}
    for data_file in _find_data_files(app_dir):
        slug = _slug_for_data_file(data_file, app_dir)
        leaf = "data" if slug == "index" else f"{slug.replace('/', '.')}.data"
        module = sys.modules.get(f"{app_pkg}.{leaf}")
        if module is None:
            continue
        out[slug] = [name for name, obj in vars(module).items() if isinstance(obj, Database)]
    return out

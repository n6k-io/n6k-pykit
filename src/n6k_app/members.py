"""What a ``data.py`` decorator hands back: a typed handle on one relation.

``@db.data()`` / ``@db.rpc()`` / ``@db.view()`` / ``@<source>.derive()`` each bind
their name to a :class:`Member`, so a page imports the relation itself rather than
a handle on the database:

    # data.py
    db = Database()
    BASE_REVENUE = 100_000.0

    @db.rpc()
    def simulate(sims: int) -> Table[Cone]: ...

    # page.py
    from .data import BASE_REVENUE, simulate

    @page
    def forecast(c: Components) -> None:
        c.LineChart(query=simulate.ref(sims=10_000), x="month", y="revenue")

Every member offers both data paths: ``.ref(**args)`` for a query the browser runs
against its own DuckDB, and ``()`` to materialize server-side during render.

``.ref()`` needs the ATTACH alias, which is not a property of the member — it is
the variable name *the page's* ``data.py`` binds the Database to, and one Database
may answer to different names on different pages. So it is resolved per render
from the catalog map, via :func:`bind_catalogs`.
"""

import inspect
import math
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Iterator, cast

import pandas as pd
import pyarrow as pa

from n6k_app.table_registry import RpcSpec, resolve_return_schema

if TYPE_CHECKING:
    from n6k_app.database import Database

__all__ = [
    "Member",
    "Ref",
    "Rpc",
    "Source",
    "View",
    "bind_catalogs",
]


def _sql_lit(value: Any) -> str:
    """A Python value as a DuckDB SQL literal."""
    if value is None:
        return "NULL"
    # bool before int: bool is an int subclass, and str(True) is not SQL.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, int) or isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        # Bare NaN/Infinity is a DuckDB syntax error.
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"{value!r} has no SQL literal form.")
        return repr(value)
    if isinstance(value, (datetime, date, time)):
        return "'" + value.isoformat() + "'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_sql_lit(v) for v in value) + "]"
    raise TypeError(f"cannot pass {type(value).__name__} as a SQL argument: {value!r}")


class Ref(str):
    """A reference to a data.py source/rpc as a client-side query
    (`SELECT * FROM <alias>.<member>(args)`). Subclasses `str` so it stays a
    distinct type yet serializes straight into string-typed component props."""

    sql: str

    def __new__(cls, sql: str) -> "Ref":
        obj = super().__new__(cls, sql)
        obj.sql = sql
        return obj


# The rendering page's {alias: Database}. Set for the duration of one render (see
# bind_catalogs); a member resolves its own alias out of it by object identity.
_catalogs: ContextVar[dict[str, "Database"] | None] = ContextVar("n6k_catalogs", default=None)


@contextmanager
def bind_catalogs(catalogs: dict[str, "Database"]) -> Iterator[None]:
    """Make *catalogs* — a page's ``{alias: Database}`` — the map ``Member.table``
    resolves against, for the duration of the block.

    This is what lets one Database answer to different aliases on different pages:
    the alias is a fact about the page being rendered, never about the member.
    """
    token = _catalogs.set(catalogs)
    try:
        yield
    finally:
        _catalogs.reset(token)


class Member:
    """One relation on a `Database` — a source, derivation, view or rpc."""

    def __init__(self, db: "Database", name: str, schema: str = "main"):
        self._db = db
        self._name = name
        self._schema = schema

    @property
    def name(self) -> str:
        """The relation's own name, unqualified."""
        return self._name

    @property
    def table(self) -> str:
        """This relation's SQL name, qualified by the rendering page's alias.

        Usable anywhere a component prop wants a table name (`data=`), and the
        base of every `.ref()` query.
        """
        alias = self._alias()
        return f"{alias}.{self._name}" if self._schema == "main" else f"{alias}.{self._schema}.{self._name}"

    def _alias(self) -> str:
        """The ATTACH alias this member's Database is served under, on the page
        currently rendering."""
        catalogs = _catalogs.get()
        if catalogs is None:
            raise RuntimeError(
                f"{self._name!r} has no catalog alias here: an alias only exists while a page "
                f"renders, because it is the name that page's data.py binds the Database to. "
                f"Use {self._name}() to read the data directly instead of {self._name}.table / "
                f"{self._name}.ref()."
            )
        for alias, db in catalogs.items():
            if db is self._db:
                return alias
        known = ", ".join(catalogs) or "none"
        raise RuntimeError(
            f"{self._name!r} belongs to a Database this page does not serve, so it has no "
            f"catalog alias (this page serves: {known}). A page serves the databases its own "
            f"data.py names — bind it there, e.g. `from ..other.data import db as sales`."
        )

    def ref(self, **args: Any) -> Ref:
        """A client-side query for this relation, run in the browser's DuckDB."""
        target = self.table
        if args:
            target += "(" + ", ".join(_sql_lit(v) for v in self._bind(args)) + ")"
        return Ref(f"SELECT * FROM {target}")

    def _bind(self, args: dict[str, Any]) -> list[Any]:
        raise TypeError(f"{self._name}.ref() takes no arguments; it is not an rpc.")

    def __call__(self, **args: Any) -> Any:
        raise NotImplementedError


class Source(Member):
    """A ``@db.data()`` table or a ``@<source>.derive()`` derivation.

    Callable: ``sales()`` materializes the table as a DataFrame so an RPC can read
    a source without stashing rows in a module global.
    """

    def __call__(self, **args: Any) -> pd.DataFrame:
        """Materialize this table (source or derive) as a DataFrame."""
        if args:
            raise TypeError(f"{self._name}() is a data source and takes no arguments.")
        return self._db.table_registry.load_table(self._name).to_pandas()

    def on_insert[F: Callable[..., object]](self, fn: F) -> F:
        self._db.table_registry.set_write_fn(self._name, insert_fn=fn)
        return fn

    def on_update[F: Callable[..., object]](self, fn: F) -> F:
        self._db.table_registry.set_write_fn(self._name, update_fn=fn)
        return fn

    def on_delete[F: Callable[..., object]](self, fn: F) -> F:
        self._db.table_registry.set_write_fn(self._name, delete_fn=fn)
        return fn

    def derive(self, name: str | None = None) -> Callable[[Callable[..., Any]], "Source"]:
        def decorator(fn: Callable[..., Any]) -> Source:
            resolved = name if name is not None else fn.__name__
            self._db.table_registry.register_derivation(self._name, resolved, fn, schema=self._schema)
            return Source(self._db, resolved, self._schema)

        return decorator

    def view(self, name: str | None = None) -> Callable[[Callable[..., Any]], "View"]:
        def decorator(fn: Callable[..., Any]) -> View:
            resolved = name if name is not None else fn.__name__
            sql = fn()
            if not isinstance(sql, str):
                raise TypeError(f"@<data>.view('{resolved}') must return a SQL string, got {type(sql).__name__}")
            self._db.table_registry.register_view(resolved, sql.strip(), schema=self._schema)
            return View(self._db, resolved, self._schema)

        return decorator

    def rpc[R](
        self, name: str | None = None, *, returns: pa.Schema | None = None
    ) -> Callable[[Callable[..., R]], "Rpc[R]"]:
        def decorator(fn: Callable[..., R]) -> Rpc[R]:
            resolved = name if name is not None else fn.__name__
            schema = resolve_return_schema(fn, returns)
            self._db.table_registry.register_rpc(resolved, fn, self._name, return_schema=schema)
            return Rpc(self._db, resolved, self._schema)

        return decorator


class View(Member):
    """A ``@db.view()`` SQL view. Queryable, but nothing to materialize in Python."""

    def __call__(self, **args: Any) -> Any:
        raise RuntimeError(
            f"{self._name} is a SQL view that runs inside DuckDB — there is nothing to "
            f"materialize in Python. Use {self._name}.ref() to query it."
        )


class Rpc[R](Member):
    """A ``@db.rpc()`` table function.

    ``.ref(**args)`` binds the arguments into a query the browser runs; calling it
    runs the function here and now, returning whatever it returns.
    """

    def _bind(self, args: dict[str, Any]) -> list[Any]:
        """`args` as positional SQL call arguments, in the rpc's declared order.

        A table function binds positionally, but `**kwargs` preserve *call* order —
        so we reorder by the declared signature rather than splatting `.values()`.
        """
        spec = self._spec()
        declared = spec.scalar_params
        unknown = [k for k in args if k not in declared]
        if unknown:
            raise TypeError(f"{self._name}() got unexpected keyword argument(s) {unknown}; expected {list(declared)}.")

        params = inspect.signature(spec.fn).parameters
        bound: list[Any] = []
        skipped: list[str] = []
        for name in declared:
            if name not in args:
                if params[name].default is inspect.Parameter.empty:
                    raise TypeError(f"{self._name}() missing required argument {name!r}.")
                skipped.append(name)  # defaulted: fine unless something follows
                continue
            if skipped:
                # A positional call can't leave a hole: passing `year` after omitting
                # a defaulted `col` before it would slide `year` into `col`.
                raise TypeError(
                    f"{self._name}() cannot pass {name!r} while omitting {skipped!r} before it; "
                    f"pass the skipped argument(s) explicitly."
                )
            bound.append(args[name])
        return bound

    def _spec(self) -> RpcSpec:
        spec = self._db.table_registry.get_rpc(self._name)
        if spec is None:  # unreachable: the decorator registers before constructing us
            raise LookupError(f"rpc {self._name!r} is no longer registered on its database.")
        return spec

    def __call__(self, **args: Any) -> R:
        """Run the rpc server-side, right now, and return its result unchanged."""
        spec = self._spec()
        if spec.session_param is not None or spec.native_param is not None:
            injected = spec.session_param or spec.native_param
            raise RuntimeError(
                f"{self._name}() declares an injected parameter ({injected!r}) that only exists "
                f"on a live connection, so it can't be called directly. Use "
                f"{self._name}.ref(...) to run it through the page's session."
            )
        self._bind(args)  # reject unknown/missing args with the same message as .ref()
        if spec.parent_table:
            parent_df = self._db.table_registry.load_table(spec.parent_table).to_pandas()
            return cast(R, spec.fn(parent_df, **args))
        return cast(R, spec.fn(**args))

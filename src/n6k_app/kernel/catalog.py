"""Expose the kernel's in-memory DataFrames as the n6k catalog ``kernel``.

The ``python`` RPC execs into the globals and ``sh`` runs a subprocess: arbitrary code,
the same capability the exec channel grants. Access control is the socket, not these ops.
"""

import asyncio
import concurrent.futures
import logging
import os
import platform
import subprocess
import sys
from collections.abc import Callable, Mapping
from typing import Any, Optional

import duckdb
import pyarrow as pa

from virtual_catalog.filters import Filters, filter_and_project
from n6k_server.extension import load_n6k_server
from virtual_catalog.extension import load_vcatprovider
from virtual_catalog.provider import (
    Provider,
    TableNotFound,
    invalidate_provider_tables,
    register_provider,
)
from virtual_catalog.rpc_stream import register_rpc_streams

log = logging.getLogger(__name__)

CATALOG = "kernel"
DEFAULT_SCHEMA = "main"
METADATA_SCHEMA = "metadata"

PYTHON_RPC_SCHEMA = pa.schema([pa.field("ok", pa.bool_())])
_SH_RPC_FIELDS: "list[pa.Field[Any]]" = [
    pa.field("returncode", pa.int64()),
    pa.field("stdout", pa.string()),
    pa.field("stderr", pa.string()),
]
SH_RPC_SCHEMA = pa.schema(_SH_RPC_FIELDS)


def _is_dataframe(value: Any) -> bool:
    """True for a pandas DataFrame, without importing pandas: the runtime venv may not have it."""
    cls = type(value)
    module = getattr(cls, "__module__", "") or ""
    return cls.__name__ == "DataFrame" and module.split(".", 1)[0] == "pandas"


def _to_arrow(df: Any) -> pa.Table:
    return pa.Table.from_pandas(df)


def _is_noise(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def _type_name(value: Any) -> str:
    cls = type(value)
    module = getattr(cls, "__module__", "") or ""
    qual = getattr(cls, "__qualname__", cls.__name__)
    if module in ("", "builtins"):
        return qual
    return f"{module}.{qual}"


def _safe_summary(value: Any, limit: int = 120) -> str:
    """A short description that never raises."""
    try:
        if _is_dataframe(value):
            shape = getattr(value, "shape", None)
            if shape is not None:
                return f"DataFrame{tuple(shape)}"
        if isinstance(value, (str, bytes, list, tuple, dict, set, frozenset)):
            return f"{type(value).__name__} len={len(value)}"
        text = repr(value)
    except Exception:
        return "<unrepresentable>"
    text = text.replace("\n", " ")
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


class Namespaces:
    """Later namespaces win on name collision. A client's cached catalog still needs
    ``CatalogInvalidator`` to see new DataFrames."""

    def __init__(self, *namespaces: Mapping[str, Any]) -> None:
        self.namespaces = namespaces

    def frames(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for ns in self.namespaces:
            for name, value in ns.items():
                if _is_dataframe(value):
                    out[name] = value
        return out

    def frame(self, table: str) -> Any:
        for ns in reversed(self.namespaces):
            value = ns.get(table)
            if value is not None and _is_dataframe(value):
                return value
        raise TableNotFound(table)

    def collect_vars(self) -> list[dict[str, Any]]:
        last = len(self.namespaces) - 1
        seen: dict[str, dict[str, Any]] = {}
        for idx, ns in enumerate(self.namespaces):
            scope = "local" if idx == last else "global"
            for name, value in ns.items():
                if _is_noise(name):
                    continue
                seen[name] = {
                    "name": name,
                    "type": _type_name(value),
                    "scope": scope,
                    "is_dataframe": _is_dataframe(value),
                    "summary": _safe_summary(value),
                }
        return list(seen.values())


class DataFrameProvider(Provider):
    """One table per DataFrame in the namespaces."""

    def __init__(self, namespaces: Namespaces) -> None:
        self._ns = namespaces

    async def list_tables(self) -> list[str]:
        tables = list(self._ns.frames())
        log.debug("main.list_tables -> %s", tables)
        return tables

    async def schema(self, name: str) -> pa.Schema:
        log.debug("main.schema(%r)", name)
        return _to_arrow(self._ns.frame(name)).schema

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        arrow = filter_and_project(_to_arrow(self._ns.frame(name)), columns, filters)
        log.debug("main.scan(%r, columns=%s) -> %d rows", name, columns, arrow.num_rows)
        return arrow


class MetadataProvider(Provider):
    """``kernel.metadata``: rebuilt on every request."""

    def __init__(self, namespaces: Namespaces) -> None:
        self._ns = namespaces

    def _builders(self) -> dict[str, Callable[[], pa.Table]]:
        return {"kernel": self._build_kernel_table, "vars": self._build_vars_table}

    def _table(self, name: str) -> pa.Table:
        builder = self._builders().get(name)
        if builder is None:
            raise TableNotFound(name)
        return builder()

    async def list_tables(self) -> list[str]:
        return list(self._builders())

    async def schema(self, name: str) -> pa.Schema:
        log.debug("metadata.schema(%r)", name)
        return self._table(name).schema

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        arrow = filter_and_project(self._table(name), columns, filters)
        log.debug("metadata.scan(%r, columns=%s) -> %d rows", name, columns, arrow.num_rows)
        return arrow

    def _build_kernel_table(self) -> pa.Table:
        fields: list[pa.Field[Any]] = [
            pa.field("language", pa.string()),
            pa.field("python_version", pa.string()),
            pa.field("python_version_short", pa.string()),
            pa.field("implementation", pa.string()),
            pa.field("executable", pa.string()),
            pa.field("platform", pa.string()),
            pa.field("system", pa.string()),
            pa.field("machine", pa.string()),
            pa.field("pid", pa.int64()),
            pa.field("cwd", pa.string()),
        ]
        schema = pa.schema(fields)
        data: dict[str, list[Any]] = {
            "language": ["python"],
            "python_version": [sys.version],
            "python_version_short": [platform.python_version()],
            "implementation": [platform.python_implementation()],
            "executable": [sys.executable],
            "platform": [platform.platform()],
            "system": [platform.system()],
            "machine": [platform.machine()],
            "pid": [os.getpid()],
            "cwd": [os.getcwd()],
        }
        return pa.table(data, schema=schema)

    def _build_vars_table(self) -> pa.Table:
        fields: list[pa.Field[Any]] = [
            pa.field("name", pa.string()),
            pa.field("type", pa.string()),
            pa.field("scope", pa.string()),
            pa.field("is_dataframe", pa.bool_()),
            pa.field("summary", pa.string()),
        ]
        schema = pa.schema(fields)
        rows = self._ns.collect_vars()
        data: dict[str, list[Any]] = {
            "name": [r["name"] for r in rows],
            "type": [r["type"] for r in rows],
            "scope": [r["scope"] for r in rows],
            "is_dataframe": [r["is_dataframe"] for r in rows],
            "summary": [r["summary"] for r in rows],
        }
        return pa.table(data, schema=schema)


def rpc_registry(namespaces: Namespaces, invalidator: Optional["CatalogInvalidator"] = None) -> dict[str, Any]:
    """Exceptions reach the client as ERR (``register_rpc_streams``); ``sh`` reports a non-zero
    exit in ``returncode`` and does not raise."""

    def run_python(scalars: list[Any], table: Optional[pa.Table]) -> pa.Table:
        code = scalars[0] if scalars else ""
        target = namespaces.namespaces[0] if namespaces.namespaces else {}
        exec(compile(code, "<n6k_rpc_python>", "exec"), target)  # type: ignore[arg-type]
        if invalidator is not None:
            invalidator.invalidate([DEFAULT_SCHEMA])
        return pa.table({"ok": [True]}, schema=PYTHON_RPC_SCHEMA)

    def run_sh(scalars: list[Any], table: Optional[pa.Table]) -> pa.Table:
        command = scalars[0] if scalars else ""
        proc = subprocess.run(command, shell=True, capture_output=True, text=True)
        return pa.table(
            {
                "returncode": [proc.returncode],
                "stdout": [proc.stdout],
                "stderr": [proc.stderr],
            },
            schema=SH_RPC_SCHEMA,
        )

    return {
        "python": (run_python, PYTHON_RPC_SCHEMA),
        "sh": (run_sh, SH_RPC_SCHEMA),
    }


class CatalogInvalidator:
    """Thread-safe handle for telling the attached n6k client its cached catalog
    is stale.

    The catalog serves on the kernel's asyncio loop while cells run on worker
    threads. After a cell changes the namespace, a worker calls
    ``invalidate()``: the provider catalog refills on its next lookup and the
    client gets a CATALOG_INVALIDATED push, so the next ``SHOW TABLES``
    refetches — without it, DataFrames created after attach stay invisible.

    A no-op until the catalog is serving (``bind``); an unconnected client
    refetches on its next attach anyway. ``con`` is a cursor of the served
    connection, since the serving CALL holds the connection itself.
    """

    def __init__(self, catalog: str = CATALOG) -> None:
        self.catalog = catalog
        self._bound: list[tuple[asyncio.AbstractEventLoop, duckdb.DuckDBPyConnection]] = []

    def bind(self, loop: asyncio.AbstractEventLoop, con: duckdb.DuckDBPyConnection) -> None:
        self._bound = [*self._bound, (loop, con)]

    def unbind(self, con: duckdb.DuckDBPyConnection) -> None:
        self._bound = [b for b in self._bound if b[1] is not con]

    def invalidate(self, schemas: list[str]) -> list[concurrent.futures.Future[None]]:
        """Schedule the invalidation on every bound catalog's loop; empty when unbound. Safe to
        call from any thread; never raises into the caller."""
        return [
            f for loop, con in self._bound if (f := self._invalidate_one(loop, con, self.catalog, schemas)) is not None
        ]

    @staticmethod
    def _invalidate_one(
        loop: asyncio.AbstractEventLoop, con: duckdb.DuckDBPyConnection, catalog: str, schemas: list[str]
    ) -> Optional[concurrent.futures.Future[None]]:
        def _push() -> None:
            with con.cursor() as cur:
                cur.execute(
                    "CALL n6k_serve_push_invalidate(?, ?)",
                    [catalog, *schemas],
                ).fetchall()

        async def _invalidate() -> None:
            try:
                await invalidate_provider_tables(con, catalog=catalog)
                await asyncio.to_thread(_push)
                log.debug("pushed catalog invalidation for schemas %s", schemas)
            except Exception:
                # A lost push is harmless: the next access refetches.
                log.debug("catalog invalidation failed", exc_info=True)

        try:
            return asyncio.run_coroutine_threadsafe(_invalidate(), loop)
        except RuntimeError:
            # Loop closed (catalog shut down); the next connect refetches. A stopped but
            # unclosed loop raises nothing and leaves the future pending.
            log.debug("catalog invalidate skipped: loop not running")
            return None


async def open_catalog(
    *namespaces: Mapping[str, Any],
    invalidator: Optional["CatalogInvalidator"] = None,
    catalog: str = CATALOG,
) -> duckdb.DuckDBPyConnection:
    """Must be awaited on the loop that serves the providers."""
    ns = Namespaces(*namespaces)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k_server(con)
    load_vcatprovider(con)
    con.execute(f"ATTACH ':memory:' AS {catalog} (TYPE vcatprovider)")
    register_provider(con, catalog=catalog, schema_name=DEFAULT_SCHEMA, provider=DataFrameProvider(ns))
    register_provider(con, catalog=catalog, schema_name=METADATA_SCHEMA, provider=MetadataProvider(ns))
    # Off the loop: registration runs SQL that calls back into the providers,
    # which are marshalled to this loop and would deadlock it.
    await asyncio.to_thread(
        register_rpc_streams,
        con,
        catalog=catalog,
        registry=rpc_registry(ns, invalidator),
        main_loop=asyncio.get_running_loop(),
    )
    return con

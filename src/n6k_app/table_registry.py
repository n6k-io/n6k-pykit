"""Lazy-loading table registry.

Knows every table/view name at startup but defers materialization
until first access (RegistryProvider.scan loads on demand).
"""

import inspect
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa

from n6k_app.drivers.base import driver_for_annotation
from n6k_app.formats import to_arrow
from n6k_app.schema import return_schema_of
from n6k_app.session_state import Session

_log = logging.getLogger(__name__)


@dataclass
class TableSpec:
    name: str
    schema: str = "main"
    loader: Callable[..., Any] | None = None
    parent: str | None = None
    derive_fn: Callable[..., Any] | None = None
    arrow_schema: pa.Schema | None = None
    primary_keys: tuple[str, ...] | None = None
    insert_fn: Callable[..., Any] | None = None
    update_fn: Callable[..., Any] | None = None
    delete_fn: Callable[..., Any] | None = None


@dataclass
class ViewSpec:
    name: str
    sql: str
    schema: str = "main"


@dataclass
class RpcSpec:
    name: str
    fn: Callable[..., Any]
    parent_table: str | None = None
    return_schema: pa.Schema | None = None
    # session_param: name of the `Session` param (injected per-connection), or None.
    # native_param: name of the driver-native `Connection` param (injected per call),
    # or None; native_db_type is the driver's db_type ("postgres") that param binds to.
    # scalar_params: params bound from SQL call args (excludes Session/Connection and,
    # for a parent-bound RPC, the leading parent-DataFrame param).
    session_param: str | None = None
    native_param: str | None = None
    native_db_type: str | None = None
    scalar_params: tuple[str, ...] = ()


def _is_session_annotation(annotation: object) -> bool:
    """True if a parameter is annotated `Session`. Handles stringized annotations
    (`from __future__ import annotations`): "Session" / "n6k_app.Session"."""
    if annotation is Session:
        return True
    if isinstance(annotation, str):
        return annotation.split(".")[-1].strip() == "Session"
    return False


def _rpc_param_binding(
    fn: Callable[..., Any], parent_table: str | None
) -> tuple[str | None, str | None, str | None, tuple[str, ...]]:
    """Split an RPC's parameters into the injected `Session`, a driver-native
    `Connection` (each optional), and the ordered scalar parameters that bind from
    the SQL call arguments. A native Connection is spotted by asking the driver
    registry which upstream, if any, claims the param's annotation type."""
    params = list(inspect.signature(fn).parameters.values())
    session_param: str | None = None
    native_param: str | None = None
    native_db_type: str | None = None
    scalar_params: list[str] = []
    for i, p in enumerate(params):
        if parent_table is not None and i == 0:
            continue  # the parent DataFrame, supplied by n6k, not a SQL arg
        if _is_session_annotation(p.annotation):
            if session_param is not None:
                raise ValueError(
                    f"RPC '{fn.__name__}' declares more than one Session parameter "
                    f"('{session_param}' and '{p.name}'); at most one is allowed."
                )
            session_param = p.name
        elif (driver := driver_for_annotation(p.annotation)) is not None:
            if native_param is not None:
                raise ValueError(
                    f"RPC '{fn.__name__}' declares more than one native Connection parameter "
                    f"('{native_param}' and '{p.name}'); at most one is allowed."
                )
            native_param = p.name
            native_db_type = driver.db_type
        else:
            scalar_params.append(p.name)
    return session_param, native_param, native_db_type, tuple(scalar_params)


class TableRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tables: dict[str, TableSpec] = {}
        self._views: dict[str, ViewSpec] = {}
        self._rpcs: dict[str, RpcSpec] = {}
        self.cached_tables: dict[str, pa.Table] = {}

    def _ensure_unique(self, name: str) -> None:
        """Reject a name already taken by a table or view — they share one SQL
        namespace, so a duplicate would silently shadow the first and serve wrong data."""
        if name in self._tables or name in self._views:
            kind = "table" if name in self._tables else "view"
            raise ValueError(
                f"'{name}' is already registered as a {kind} in this database; " f"table and view names must be unique."
            )

    def register_source(
        self,
        name: str,
        fn: Callable[..., Any],
        schema: str = "main",
        arrow_schema: pa.Schema | None = None,
        primary_keys: tuple[str, ...] | None = None,
    ) -> None:
        self._ensure_unique(name)
        self._tables[name] = TableSpec(
            name=name,
            schema=schema,
            loader=fn,
            arrow_schema=arrow_schema,
            primary_keys=primary_keys,
        )

    def register_derivation(self, parent: str, name: str, fn: Callable[..., Any], schema: str = "main") -> None:
        self._ensure_unique(name)
        self._tables[name] = TableSpec(name=name, schema=schema, parent=parent, derive_fn=fn)

    def register_view(self, name: str, sql: str, schema: str = "main") -> None:
        self._ensure_unique(name)
        self._views[name] = ViewSpec(name=name, sql=sql, schema=schema)

    def register_rpc(
        self, name: str, fn: Callable[..., Any], parent_table: str | None = None, return_schema: pa.Schema | None = None
    ) -> None:
        if return_schema is None:
            raise ValueError(
                f"RPC '{name}' has no return schema, so n6k can't advertise its output "
                f"columns to DuckDB and the RPC would be unusable. Declare one by annotating "
                f"the return type:\n"
                f"    finite  ->  def {name}(...) -> Table[YourSchema]:      # returns a table\n"
                f"    stream  ->  def {name}(...) -> AsyncIterator[YourSchema]:  # yields rows\n"
                f"where YourSchema subclasses n6k_app.Schema. As an escape hatch you can pass an "
                f"explicit arrow schema instead: @rpc(returns=pa.schema(...))."
            )
        if name in self._rpcs:
            raise ValueError(f"RPC '{name}' is already registered in this database; RPC names must be unique.")
        session_param, native_param, native_db_type, scalar_params = _rpc_param_binding(fn, parent_table)
        self._rpcs[name] = RpcSpec(
            name=name,
            fn=fn,
            parent_table=parent_table,
            return_schema=return_schema,
            session_param=session_param,
            native_param=native_param,
            native_db_type=native_db_type,
            scalar_params=scalar_params,
        )

    def get_rpc(self, name: str) -> RpcSpec | None:
        return self._rpcs.get(name)

    @property
    def rpc_names(self) -> list[str]:
        return list(self._rpcs.keys())

    @property
    def schemas(self) -> set[str]:
        result: set[str] = set()
        for spec in self._tables.values():
            result.add(spec.schema)
        for vspec in self._views.values():
            result.add(vspec.schema)
        return result

    def set_write_fn(
        self,
        name: str,
        *,
        insert_fn: Callable[..., Any] | None = None,
        update_fn: Callable[..., Any] | None = None,
        delete_fn: Callable[..., Any] | None = None,
    ) -> None:
        spec = self._tables[name]
        if insert_fn:
            spec.insert_fn = insert_fn
        if update_fn:
            spec.update_fn = update_fn
        if delete_fn:
            spec.delete_fn = delete_fn

    def table_specs(self) -> dict[str, TableSpec]:
        return dict(self._tables)

    def view_specs(self) -> dict[str, ViewSpec]:
        return dict(self._views)

    def resolve_schema(self, name: str) -> pa.Schema:
        """Get the Arrow schema for a table, eager-loading if no annotation was provided."""
        spec = self._tables[name]
        if spec.arrow_schema is not None:
            return spec.arrow_schema

        arrow_table = self.load_table(name)
        return _normalize_schema(arrow_table.schema)

    def load_table(self, name: str) -> pa.Table:
        """Load and cache a table (thread-safe). Sources call the loader; derived
        tables load the parent and apply ``derive_fn``. No DuckDB connection involved."""
        if name in self.cached_tables:
            return self.cached_tables[name]

        spec = self._tables.get(name)
        if spec is None:
            raise KeyError(f"Unknown table: {name}")

        with self._lock:
            if name in self.cached_tables:
                return self.cached_tables[name]

            if spec.parent and spec.derive_fn is not None:
                parent_arrow = self.load_table(spec.parent)
                parent_df = parent_arrow.to_pandas()
                _log.info("Loading derived table '%s' from '%s'", name, spec.parent)
                arrow_table = to_arrow(spec.derive_fn(parent_df))
            elif spec.loader is not None:
                _log.info("Loading data source '%s'", name)
                arrow_table = to_arrow(spec.loader())
            else:
                raise ValueError(f"Table '{name}' has no loader or derive function")

            self.cached_tables[name] = arrow_table
            spec.arrow_schema = _normalize_schema(arrow_table.schema)
            return arrow_table


_ARROW_TYPE_MAP: dict[pa.DataType, pa.DataType] = {
    pa.large_utf8(): pa.utf8(),
    pa.large_binary(): pa.binary(),
}


def _normalize_schema(schema: pa.Schema) -> pa.Schema:
    """Normalize Arrow types that DuckDB doesn't recognize (e.g. large_string -> string)."""
    fields = []
    for fld in schema:
        mapped = _ARROW_TYPE_MAP.get(fld.type, fld.type)
        fields.append(pa.field(fld.name, mapped))
    return pa.schema(fields)


def resolve_return_schema(fn: Callable[..., Any], explicit: pa.Schema | None) -> pa.Schema | None:
    if explicit is not None:
        return explicit
    return return_schema_of(fn)

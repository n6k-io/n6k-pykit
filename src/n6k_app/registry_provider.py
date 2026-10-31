"""Driver ``Provider`` backed by a ``TableRegistry``, one per schema.

Bridges the synchronous, lazy ``TableRegistry`` to the driver's async
``Provider`` contract; registered per schema in ``session.attach_catalog``.
SQL views are NOT exposed here — they become real DuckDB ``CREATE VIEW``s. Each
write op is gated by the table's ``insert``/``update``/``delete`` fn.

Write-fn contract (defined here — the DSL leaves it open):
  - ``insert_fn(rows: pa.Table) -> int``
  - ``update_fn(rows: pa.Table, keys: list[str]) -> int``
  - ``delete_fn(keys: pa.Table) -> int``
"""

import asyncio
from typing import Optional

import pyarrow as pa

from n6k_protocol.filters import Filters, filter_and_project
from n6k_server.provider import PK_METADATA_KEY, Provider, TableNotFound

from n6k_app.table_registry import TableRegistry, TableSpec


class RegistryProvider(Provider):
    def __init__(self, registry: TableRegistry, schema: str) -> None:
        self._registry = registry
        self._schema = schema

    def _spec(self, name: str) -> TableSpec:
        spec = self._registry.table_specs().get(name)
        if spec is None or spec.schema != self._schema:
            raise TableNotFound(name)
        return spec

    async def list_tables(self) -> list[str]:
        return [n for n, s in self._registry.table_specs().items() if s.schema == self._schema]

    async def schema(self, name: str) -> pa.Schema:
        spec = self._spec(name)
        sch = await asyncio.to_thread(self._registry.resolve_schema, name)
        if spec.primary_keys:
            sch = sch.with_metadata({PK_METADATA_KEY: ",".join(spec.primary_keys).encode("utf-8")})
        return sch

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        self._spec(name)
        # load_table materialises tables purely in Python — no DuckDB round-trip,
        # so no provider re-entrancy. The provider filters itself; DuckDB does not
        # re-filter provider scans.
        table = await asyncio.to_thread(self._registry.load_table, name)
        return filter_and_project(table, columns, filters)

    async def insert(self, name: str, rows: pa.Table) -> int:
        fn = self._spec(name).insert_fn
        if fn is None:
            raise TableNotFound(name)
        return int(await asyncio.to_thread(fn, rows))

    async def update(self, name: str, rows: pa.Table, keys: list[str]) -> int:
        fn = self._spec(name).update_fn
        if fn is None:
            raise TableNotFound(name)
        return int(await asyncio.to_thread(fn, rows, keys))

    async def delete(self, name: str, keys: pa.Table) -> int:
        fn = self._spec(name).delete_fn
        if fn is None:
            raise TableNotFound(name)
        return int(await asyncio.to_thread(fn, keys))

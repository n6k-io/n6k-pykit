"""RegistryProvider: a TableRegistry served through the driver's Provider model.

Queries run via asyncio.to_thread: the provider's async scan marshals back to the
loop, so a query run synchronously on the loop thread would deadlock.
"""

import asyncio
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa

from n6k_server.extension import load_virtual_catalog_provider
from n6k_server.provider import register_provider

from n6k_app.registry_provider import RegistryProvider
from n6k_app.table_registry import TableRegistry


async def _q(con: duckdb.DuckDBPyConnection, sql: str) -> list[Any]:
    return await asyncio.to_thread(lambda: con.execute(sql).fetchall())


def _conn(reg: TableRegistry) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_virtual_catalog_provider(con)
    con.execute("ATTACH ':memory:' AS db (TYPE virtual_catalog_provider)")
    con.execute("USE db")
    for schema in sorted(reg.schemas or {"main"}):
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "db"."{schema}"')
        register_provider(con, catalog="db", schema_name=schema, provider=RegistryProvider(reg, schema))
    return con


def _registry() -> TableRegistry:
    reg = TableRegistry()
    reg.register_source(
        "demo",
        lambda: pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]}),
        primary_keys=("id",),
    )
    return reg


async def test_scan_source_table_qualified_and_unqualified() -> None:
    con = _conn(_registry())
    try:
        # Three-part (catalog-qualified) and two-part (via USE) both resolve.
        assert await _q(con, 'SELECT id, name FROM "db"."main"."demo" ORDER BY id') == [
            (1, "alice"),
            (2, "bob"),
            (3, "charlie"),
        ]
        assert await _q(con, 'SELECT name FROM "main"."demo" WHERE id = 2') == [("bob",)]
    finally:
        con.close()


async def test_derived_table_scans_without_conn_reentrancy() -> None:
    reg = _registry()
    reg.register_derivation("demo", "names", lambda df: df[["name"]])
    con = _conn(reg)
    try:
        assert await _q(con, 'SELECT name FROM "db"."main"."names" ORDER BY name') == [
            ("alice",),
            ("bob",),
            ("charlie",),
        ]
    finally:
        con.close()


async def test_insert_invokes_registered_write_fn() -> None:
    reg = _registry()
    inserted: list[pa.Table] = []

    def _record(rows: pa.Table) -> int:
        inserted.append(rows)
        return rows.num_rows

    reg.set_write_fn("demo", insert_fn=_record)

    con = _conn(reg)
    try:
        await _q(con, 'INSERT INTO "db"."main"."demo" VALUES (4, \'dan\')')
        assert inserted, "insert_fn was not invoked"
        assert inserted[0].num_rows == 1
    finally:
        con.close()

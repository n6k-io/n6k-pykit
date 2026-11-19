"""Register table functions on an in-memory vcatprovider catalog ``db`` and query them.

Queries go through ``asyncio.to_thread``: streaming functions run on the calling loop, so a
query issued from that loop's thread would deadlock.
"""

import asyncio
from typing import Any, Iterable

import duckdb
from virtual_catalog.extension import load_vcatprovider
from virtual_catalog.table_functions import TableFunction, register_table_functions


async def serve(functions: Iterable[TableFunction]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_vcatprovider(con)
    con.execute("ATTACH ':memory:' AS db")
    await asyncio.to_thread(
        register_table_functions,
        con,
        catalog="db",
        functions=list(functions),
        main_loop=asyncio.get_running_loop(),
    )
    return con


async def query(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return await asyncio.to_thread(lambda: con.execute(sql).fetchall())


def signature(con: duckdb.DuckDBPyConnection, name: str) -> tuple[list[str], list[str]]:
    row = con.execute(
        "SELECT parameters, parameter_types FROM duckdb_functions() WHERE database_name = 'db' AND function_name = ?",
        [name],
    ).fetchone()
    assert row is not None, f"{name} is not registered"
    return row[0], row[1]

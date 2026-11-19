"""Streaming ``@db.rpc()``.

A generator-returning RPC (async or sync) streams: its row schema is read from an
``AsyncIterator[...]`` / ``Iterator[...]`` return annotation, and each yielded value is
coerced to the schema. Driving them over the wire belongs to ``test_socket_app.py``.
"""

from typing import AsyncIterator, Iterator

import pandas as pd
import pyarrow as pa

from n6k_app import Database
from n6k_app.schema import Schema, Table, schema_from_annotation
from tests.n6k_app.sql_rpc import query, serve, signature


class Clusters(Schema):
    x = pa.float64()
    y = pa.float64()


def _names(annotation: object) -> list[str]:
    schema = schema_from_annotation(annotation)
    assert schema is not None, f"{annotation} resolved to no schema"
    return list(schema.names)


def test_streaming_return_annotation_resolves_schema() -> None:
    assert _names(AsyncIterator[Clusters]) == ["x", "y"]
    assert _names(Iterator[Table[Clusters]]) == ["x", "y"]


async def test_async_generator_rpc_streams_rows() -> None:
    db = Database()

    # The bare `AsyncIterator[Schema]` form.
    # A Schema subclass is a marker, not a row type, so a yielded payload never
    # matches it — hence the ignore. `AsyncIterator[Table[Schema]]` (below) is the
    # same thing to n6k and does type-check; both are kept covered here.
    @db.rpc()
    async def stream_pts(n: int) -> AsyncIterator[Clusters]:
        for i in range(n):
            yield {"x": float(i), "y": float(i * 2)}  # type: ignore[misc]

    con = await serve(db.table_functions())
    assert await query(con, "SELECT x, y FROM db.stream_pts(3) ORDER BY x") == [(0.0, 0.0), (1.0, 2.0), (2.0, 4.0)]


async def test_sync_generator_rpc_streams_rows() -> None:
    db = Database()

    @db.rpc()
    def stream_df(n: int) -> Iterator[Table[Clusters]]:
        for i in range(n):
            yield pd.DataFrame({"x": [float(i)], "y": [float(i)]})

    con = await serve(db.table_functions())
    assert await query(con, "SELECT count(*) FROM db.stream_df(4)") == [(4,)]


async def test_finite_rpc_with_default_and_named_args() -> None:
    db = Database()

    @db.rpc(returns=pa.schema([("id", pa.int64())]))
    def finite(limit: int = 2) -> pd.DataFrame:
        return pd.DataFrame({"id": list(range(limit))})

    con = await serve(db.table_functions())
    assert signature(con, "finite") == (["limit"], ["BIGINT"])
    assert await query(con, "SELECT count(*) FROM db.finite()") == [(2,)]
    assert await query(con, "SELECT count(*) FROM db.finite(3)") == [(3,)]
    assert await query(con, 'SELECT count(*) FROM db.finite("limit" => 4)') == [(4,)]

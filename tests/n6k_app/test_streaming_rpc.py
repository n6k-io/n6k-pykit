"""Streaming ``@db.rpc()``.

A generator-returning RPC (async or sync) is adapted to the driver's
``StreamingRpc`` contract: its row schema is read from an ``AsyncIterator[...]`` /
``Iterator[...]`` return annotation, and each yielded value is coerced to record
batches and streamed lazily.

What is asserted here is the adaptation — the registry entry's shape and what its
callable yields. Driving those entries over the wire belongs to
``test_socket_app.py``, which serves them through the real reactor.
"""

from typing import Any, AsyncIterator, Iterator, Optional

import pandas as pd
import pyarrow as pa

from n6k_app import Database
from n6k_app.schema import Schema, Table, schema_from_annotation
from virtual_catalog.rpc_stream import RpcEntryFn, StreamingRpc


class Clusters(Schema):
    x = pa.float64()
    y = pa.float64()


def _names(annotation: object) -> list[str]:
    schema = schema_from_annotation(annotation)
    assert schema is not None, f"{annotation} resolved to no schema"
    return list(schema.names)


async def _drain(entry: RpcEntryFn, scalars: list[Any], table: Optional[pa.Table] = None) -> int:
    """Rows the streaming entry yields, pulled the way the driver pulls them."""
    assert isinstance(entry, StreamingRpc)
    rows = 0
    async for batch in entry.fn(scalars, table):
        rows += batch.num_rows
    return rows


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

    entry, schema = db.rpc_registry()["stream_pts"]
    assert isinstance(entry, StreamingRpc)  # detected as streaming, not materialized
    assert schema.names == ["x", "y"]
    assert await _drain(entry, [3]) == 3


async def test_sync_generator_rpc_streams_rows() -> None:
    db = Database()

    @db.rpc()
    def stream_df(n: int) -> Iterator[Table[Clusters]]:
        for i in range(n):
            yield pd.DataFrame({"x": [float(i)], "y": [float(i)]})

    entry, _ = db.rpc_registry()["stream_df"]
    assert await _drain(entry, [4]) == 4


async def test_streaming_rpc_yields_record_batches() -> None:
    """The driver hops a thread per yielded value, so each must be a batch, not a row."""
    db = Database()

    @db.rpc()
    async def one(n: int) -> AsyncIterator[Table[Clusters]]:
        yield {"x": 1.0, "y": 2.0}

    entry, schema = db.rpc_registry()["one"]
    assert isinstance(entry, StreamingRpc)
    batches = [b async for b in entry.fn([1], None)]
    assert [type(b) for b in batches] == [pa.RecordBatch]
    assert batches[0].schema == schema


def test_finite_rpc_unchanged() -> None:
    db = Database()

    @db.rpc(returns=pa.schema([("id", pa.int64())]))
    def finite(limit: int = 2) -> pd.DataFrame:
        return pd.DataFrame({"id": list(range(limit))})

    entry, schema = db.rpc_registry()["finite"]
    assert not isinstance(entry, StreamingRpc)  # finite path preserved
    out = entry([3], None)
    assert isinstance(out, pa.Table)
    assert out.num_rows == 3

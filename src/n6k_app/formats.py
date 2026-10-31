from typing import Any

import pyarrow as pa


def to_arrow(obj: Any, schema: pa.Schema | None = None) -> pa.Table:
    """Normalize a pandas DataFrame (or Arrow table) to a ``pa.Table``.

    When ``schema`` is given the result is coerced to it (columns selected/ordered
    by field name and cast), so a callback can return a plain DataFrame.
    """
    if isinstance(obj, pa.Table):
        return obj.cast(schema) if schema is not None else obj
    if schema is not None:
        return pa.Table.from_pandas(obj, schema=schema, preserve_index=False)
    return pa.Table.from_pandas(obj)


def to_record_batches(obj: Any, schema: pa.Schema) -> list[pa.RecordBatch]:
    """Coerce one value yielded by a streaming RPC into record batches matching
    ``schema``. Each call's batches become wire chunks, so the generator controls
    granularity by what it yields (RecordBatch/Table cast; dict → 1-row batch;
    list[dict] → one batch; else via :func:`to_arrow`).
    """
    if isinstance(obj, pa.RecordBatch):
        return pa.Table.from_batches([obj]).cast(schema).to_batches()
    if isinstance(obj, pa.Table):
        return obj.cast(schema).to_batches()
    if isinstance(obj, dict):
        return pa.Table.from_pylist([obj], schema=schema).to_batches()
    if isinstance(obj, list):
        return pa.Table.from_pylist(obj, schema=schema).to_batches()
    return to_arrow(obj, schema).to_batches()

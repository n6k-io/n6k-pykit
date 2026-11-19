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

"""Declarative table schemas for data sources and RPCs.

Subclass :class:`Schema`, declare columns as ``name = pa.<type>()``, and annotate
a function's return as ``Table[MySchema]``::

    class Clusters(Schema):
        x = pa.float64()
        labels = pa.string()

    @db.rpc()
    def get_clusters(...) -> Table[Clusters]:
        return df  # plain DataFrame; coerced to Clusters' schema

The framework reads the arrow schema off the annotation and coerces the returned
rows to match, so the callback never builds a ``pa.Table`` by hand.
"""

import collections.abc as _abc
import typing
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Callable, get_args, get_origin

import pandas as pd
import pyarrow as pa

# Streaming RPC return origins (`-> AsyncIterator[Clusters]` etc.), unwrapped to
# the element type like `Table[Clusters]`.
_ITER_ORIGINS = (_abc.AsyncIterator, _abc.AsyncGenerator, _abc.Iterator, _abc.Generator)


class SchemaMeta(type):
    """Collects ``name = pa.<type>()`` class attributes into ordered fields:
    base-class fields first (so a subclass extends), then the body's own."""

    __arrow_fields__: tuple[pa.Field[Any], ...]

    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, typing.Any]) -> "SchemaMeta":
        cls = super().__new__(mcls, name, bases, namespace)
        fields: list[pa.Field[Any]] = []
        for base in bases:
            fields.extend(getattr(base, "__arrow_fields__", ()))
        for key, value in namespace.items():
            if isinstance(value, pa.DataType):
                fields.append(pa.field(key, value))
        cls.__arrow_fields__ = tuple(fields)
        return cls


class Schema(metaclass=SchemaMeta):
    """Base for declarative schemas — declare columns as ``name = pa.<type>()``."""

    __arrow_fields__: tuple[pa.Field[Any], ...] = ()

    @classmethod
    def to_arrow(cls) -> pa.Schema:
        return pa.schema(list(cls.__arrow_fields__))


# What a table-producing callback may return/yield; coerced to the declared schema.
type _TablePayload = pd.DataFrame | pa.Table | pa.RecordBatch | Mapping[str, Any] | Sequence[Mapping[str, Any]]

# ``Table[MySchema]``: an :pep:`593` ``Annotated`` marker that type-checks as
# ``_TablePayload`` while carrying ``MySchema`` as metadata n6k reads at runtime
# (``get_origin``/``get_args``) to coerce rows — see ``schema_from_annotation``.
type Table[S: Schema] = Annotated[_TablePayload, S]


def schema_from_annotation(annotation: object) -> pa.Schema | None:
    """Extract a ``pa.Schema`` from a return annotation, or ``None``.

    Understands a bare ``pa.Schema``, a :class:`Schema` subclass, ``Table[S]``,
    and a streaming iterator of any of those.
    """
    if annotation is None:
        return None
    if isinstance(annotation, pa.Schema):
        return annotation
    if isinstance(annotation, type) and issubclass(annotation, Schema):
        return annotation.to_arrow()
    origin = get_origin(annotation)
    if origin is Table:
        args = get_args(annotation)
        if args and isinstance(args[0], type) and issubclass(args[0], Schema):
            return args[0].to_arrow()
    if origin in _ITER_ORIGINS:
        args = get_args(annotation)
        if args:
            return schema_from_annotation(args[0])
    return None


def return_schema_of(fn: Callable[..., Any]) -> pa.Schema | None:
    """Resolve a callable's return annotation to a ``pa.Schema``, or ``None``.

    Reads the raw annotation first (so a ``pa.Schema`` *instance* still works),
    falling back to ``get_type_hints`` only for a stringized annotation.
    """
    ret = fn.__annotations__.get("return") if hasattr(fn, "__annotations__") else None
    if isinstance(ret, str):
        ret = typing.get_type_hints(fn).get("return")
    return schema_from_annotation(ret)

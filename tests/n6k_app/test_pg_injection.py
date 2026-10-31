"""Injecting a real psycopg ``Connection`` into a finite ``@pg.rpc()``.

No test opens a real Postgres: the pool only dials on ``ensure_started()``, and the
checkout is faked where a live connection would be.
"""

import asyncio
import contextlib
from typing import Any, Iterator

import pandas as pd
import psycopg
import pyarrow as pa
import pytest

from n6k_app.database import Database, PostgresDatabase, policy
from n6k_app.drivers.base import driver_for_annotation
from n6k_app.session_state import Session
from n6k_app.table_registry import _rpc_param_binding
from virtual_catalog.rpc_stream import StreamingRpc

ROW = pa.schema([("v", pa.int64())])


def _pg_db() -> PostgresDatabase:
    return PostgresDatabase(database="d", policies=[policy("public.t", "select", "true")])


def test_driver_claims_connection_annotation() -> None:
    driver = driver_for_annotation(psycopg.Connection)
    assert driver is not None and driver.db_type == "postgres"
    assert driver_for_annotation(Session) is None
    assert driver_for_annotation(int) is None
    assert driver_for_annotation("Connection") is None


def test_param_binding_splits_connection_from_scalars() -> None:
    def fn(a: int, conn: psycopg.Connection, b: str) -> None: ...

    session_param, native_param, native_db_type, scalars = _rpc_param_binding(fn, None)
    assert native_param == "conn"
    assert native_db_type == "postgres"
    assert session_param is None
    assert scalars == ("a", "b")


def test_param_binding_combines_session_and_connection() -> None:
    def fn(a: int, session: Session, conn: psycopg.Connection) -> None: ...

    session_param, native_param, native_db_type, scalars = _rpc_param_binding(fn, None)
    assert session_param == "session"
    assert native_param == "conn"
    assert native_db_type == "postgres"
    assert scalars == ("a",)


def test_two_connection_params_error() -> None:
    def fn(c1: psycopg.Connection, c2: psycopg.Connection) -> None: ...

    with pytest.raises(ValueError, match="more than one native Connection"):
        _rpc_param_binding(fn, None)


def test_scalar_params_hide_connection() -> None:
    db = _pg_db()

    @db.rpc(returns=ROW)
    def r(v: int, conn: psycopg.Connection) -> pd.DataFrame:
        return pd.DataFrame({"v": [v]})

    spec = db.table_registry.get_rpc("r")
    assert spec is not None
    assert spec.native_param == "conn"
    assert spec.native_db_type == "postgres"
    assert spec.scalar_params == ("v",)


def test_finite_rpc_injects_connection() -> None:
    db = _pg_db()
    stub = object()

    @db.rpc(returns=ROW)
    def r(v: int, conn: psycopg.Connection) -> pd.DataFrame:
        assert conn is stub
        return pd.DataFrame({"v": [v]})

    @contextlib.contextmanager
    def fake_checkout(db_type: str) -> Iterator[Any]:
        assert db_type == "postgres"
        yield stub

    db.connection_pool.native_checkout = fake_checkout  # type: ignore[method-assign]
    entry, _ = db.rpc_registry()["r"]
    assert not isinstance(entry, StreamingRpc)
    out = entry([7], None)
    assert isinstance(out, pa.Table)
    assert out.column("v").to_pylist() == [7]


def test_exception_propagates_through_checkout() -> None:
    db = _pg_db()
    seen: dict[str, BaseException] = {}

    @db.rpc(returns=ROW)
    def r(conn: psycopg.Connection) -> pd.DataFrame:
        raise ValueError("boom")

    @contextlib.contextmanager
    def fake_checkout(db_type: str) -> Iterator[Any]:
        try:
            yield object()
        except Exception as e:  # the real pool resets/rolls back the connection here
            seen["exc"] = e
            raise

    db.connection_pool.native_checkout = fake_checkout  # type: ignore[method-assign]
    entry, _ = db.rpc_registry()["r"]
    assert not isinstance(entry, StreamingRpc)
    with pytest.raises(ValueError, match="boom"):
        entry([], None)
    assert isinstance(seen.get("exc"), ValueError)


def test_bare_database_connection_errors() -> None:
    db = Database()

    @db.rpc(returns=ROW)
    def r(conn: psycopg.Connection) -> pd.DataFrame:
        return pd.DataFrame({"v": [1]})

    with pytest.raises(RuntimeError, match="postgres upstream"):
        db.rpc_registry()


def test_streaming_rpc_injects_connection() -> None:
    db = _pg_db()
    stub = object()

    @db.rpc(returns=ROW)
    def r(conn: psycopg.Connection) -> Iterator[pd.DataFrame]:
        assert conn is stub
        yield pd.DataFrame({"v": [1]})

    @contextlib.contextmanager
    def fake_checkout(db_type: str) -> Iterator[Any]:
        assert db_type == "postgres"
        yield stub

    db.connection_pool.native_checkout = fake_checkout  # type: ignore[method-assign]
    db.rpc_registry()  # builds without raising — streaming + Connection is supported

    spec = db.table_registry.get_rpc("r")
    assert spec is not None
    fn = db._streaming_rpc_fn(spec, Session())

    async def drive() -> list[pa.RecordBatch]:
        return [batch async for batch in fn([], None)]

    batches = asyncio.run(drive())
    assert batches and batches[0].column("v").to_pylist() == [1]

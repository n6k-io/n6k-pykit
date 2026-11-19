"""Injecting a real psycopg ``Connection`` into a finite ``@pg.rpc()``.

No test opens a real Postgres: the pool only dials on ``ensure_started()``, and the
checkout is faked where a live connection would be.
"""

import contextlib
from typing import Any, Iterator

import duckdb
import pandas as pd
import psycopg
import pyarrow as pa
import pytest

from n6k_app.database import Database, PostgresDatabase, policy
from n6k_app.drivers.base import driver_for_annotation
from n6k_app.session_state import Session
from n6k_app.table_registry import _rpc_param_binding
from tests.n6k_app.sql_rpc import query, serve, signature

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


async def test_finite_rpc_injects_connection() -> None:
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
    con = await serve(db.table_functions())
    assert signature(con, "r") == (["v"], ["BIGINT"])
    assert await query(con, "SELECT v FROM db.r(7)") == [(7,)]


async def test_exception_propagates_through_checkout() -> None:
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
    con = await serve(db.table_functions())
    with pytest.raises(duckdb.Error, match="boom"):
        await query(con, "SELECT * FROM db.r()")
    assert isinstance(seen.get("exc"), ValueError)


def test_bare_database_connection_errors() -> None:
    db = Database()

    @db.rpc(returns=ROW)
    def r(conn: psycopg.Connection) -> pd.DataFrame:
        return pd.DataFrame({"v": [1]})

    with pytest.raises(RuntimeError, match="postgres upstream"):
        db.table_functions()


async def test_streaming_rpc_holds_connection_for_the_whole_stream() -> None:
    db = _pg_db()
    stub = object()
    events: list[str] = []

    @db.rpc(returns=ROW)
    def r(conn: psycopg.Connection) -> Iterator[pd.DataFrame]:
        assert conn is stub
        for i in range(2):
            events.append(f"yield:{i}")
            yield pd.DataFrame({"v": [i]})

    @contextlib.contextmanager
    def fake_checkout(db_type: str) -> Iterator[Any]:
        assert db_type == "postgres"
        events.append("checkout")
        yield stub
        events.append("return")

    db.connection_pool.native_checkout = fake_checkout  # type: ignore[method-assign]
    con = await serve(db.table_functions())
    assert await query(con, "SELECT v FROM db.r() ORDER BY v") == [(0,), (1,)]
    assert events == ["checkout", "yield:0", "yield:1", "return"]

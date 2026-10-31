"""Smoke tests for create_app's native-protocol wiring and the RPC adapter; the
framed WS protocol itself is covered by the driver's suite.
"""

import asyncio

import pandas as pd
import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from starlette.routing import WebSocketRoute

from n6k_app import Database, PostgresDatabase, create_app, policy
from n6k_app.database import catalog_name_error, open_serving_conn, serving_authenticator, serving_sessions
from n6k_server.pump import WsReject
from virtual_catalog.rpc_stream import StreamingRpc

# A localhost port with nothing listening: the bridge's reachability check fails
# fast (connection refused) instead of blocking on a timeout.
_DEAD_PORT = 59999


def _bridged_to_dead_db() -> Database:
    """A PostgresDatabase whose upstream can never connect."""
    return PostgresDatabase(
        host="127.0.0.1",
        port=_DEAD_PORT,
        database="nope",
        user="nobody",
        policies=[policy("public.whatever", "select", "true")],
    )


def test_websocket_routes_are_page_scoped() -> None:
    # A database has no URL of its own: every catalog is served over a per-page
    # socket, addressed by the variable name in the FT_OPEN frame. Bare `/ws` is the
    # root page, which `{slug:path}/ws` cannot match.
    app = create_app()
    ws_paths = [r.path for r in app.routes if isinstance(r, WebSocketRoute)]
    assert ws_paths == ["/{slug:path}/ws", "/ws"], f"unexpected websocket routes; got {ws_paths}"
    assert not hasattr(app.state, "databases")


@pytest.mark.parametrize("path", ["/dashboard/ws", "/ws"])
def test_data_ws_completes_the_handshake(path: str) -> None:
    # The regression this guards: with no `/ws` route Starlette rejects the upgrade
    # with 403, which the duckdb extension surfaces as "Expecting status 101".
    with TestClient(create_app()).websocket_connect(path):
        pass


@pytest.mark.parametrize("name", ["main", "memory", "temp", "system"])
def test_reserved_catalog_names_are_rejected(name: str) -> None:
    # DuckDB refuses to ATTACH under these aliases, so a database bound under one is
    # rejected before attaching rather than failing mid-ATTACH.
    assert "reserved by DuckDB" in (catalog_name_error(name) or "")


def test_rpc_registry_adapts_registered_rpc() -> None:
    db = Database()

    @db.data("nums")
    def nums() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3]})

    @nums.rpc(returns=pa.schema([("id", pa.int64())]))
    def top(df: pd.DataFrame, limit: int = 1) -> pd.DataFrame:
        return df.head(limit)

    reg = db.rpc_registry()
    assert "top" in reg
    fn, schema = reg["top"]
    assert not isinstance(fn, StreamingRpc)  # a finite rpc registers as a plain callable
    # Driver contract: fn(scalars, table) -> pa.Table; scalars passed positionally.
    out = fn([2], None)
    assert isinstance(out, pa.Table)
    assert out.num_rows == 2


def test_start_records_connection_failure_instead_of_raising() -> None:
    # Otherwise a single broken page takes down the whole app at boot.
    db = _bridged_to_dead_db()

    db.start()  # must not raise

    assert db._start_error is not None
    msg = str(db._connection_unavailable_error())
    assert "could not open its external connection" in msg
    assert "Traceback" in msg


def test_start_failure_is_isolated_to_its_own_database() -> None:
    good = Database()
    good.data("t")(lambda: pd.DataFrame({"id": [1]}))
    bad = _bridged_to_dead_db()

    good.start()
    bad.start()

    assert good._start_error is None
    assert bad._start_error is not None


def test_attach_raises_for_failed_database() -> None:
    # Surfaces per session as a connection error, not silently as an empty catalog.
    db = _bridged_to_dead_db()
    db.start()

    async def authenticate() -> list[str]:
        con = await open_serving_conn({"bad": db})
        try:
            return await serving_authenticator({"bad": db}, serving_sessions({"bad": db}, con))(None, con, "")
        finally:
            con.close()

    with pytest.raises(WsReject, match="could not open its external connection") as rejected:
        asyncio.run(authenticate())
    assert rejected.value.code == 1011

"""``n6k database`` over HTTP: one ``/ws`` serving a single file's databases (binary
n6k) and its ``@action``s (JSON-RPC), and nothing else."""

import json
import sys
from pathlib import Path
from typing import Any, Iterator

import pytest
from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import FT_HELLO, FT_HELLO_ACK, FT_OPEN, FT_OPEN_ACK, N6K_PROTOCOL_VERSION
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from n6k_app.database_app import database_app

_FILE = """\
from typing import Annotated

import pandas as pd
from n6k_app import Database, Session, action

sales = Database()


@sales.data()
def orders():
    return pd.DataFrame({"id": [1, 2, 3]})


@action
def order_count(session: Annotated[Session, sales]) -> list:
    return [len(orders()), session.catalog]
"""


def _extensions_loadable() -> bool:
    import duckdb
    from n6k_server.extension import load_n6k_server
    from virtual_catalog.extension import load_vcatprovider

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        load_n6k_server(con)
        load_vcatprovider(con)
    except Exception:
        return False
    finally:
        con.close()
    return True


pytestmark = pytest.mark.skipif(not _extensions_loadable(), reason="needs the n6k_server extension")


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    py_file = tmp_path / "shop.py"
    py_file.write_text(_FILE)
    with TestClient(database_app(py_file)) as client:
        yield client
    for mod in [m for m in sys.modules if m.startswith("_n6k_")]:
        del sys.modules[mod]


def _hello_frame() -> bytes:
    return pack_frame(
        {"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc", "mcp"]}
    )


def _end(ws: Any) -> None:
    """A second HELLO is a protocol error: the server closes and the serve unwinds."""
    ws.send_bytes(_hello_frame())
    with pytest.raises(WebSocketDisconnect):
        while True:
            ws.receive_bytes()


def _recv_text(ws: Any) -> dict[str, Any]:
    while True:
        message = ws.receive()
        if message.get("text") is not None:
            return dict(json.loads(message["text"]))


def test_an_action_answers_over_jsonrpc(client: TestClient) -> None:
    with client.websocket_connect("/ws") as ws:
        ws.send_bytes(_hello_frame())
        assert unpack_frame(ws.receive_bytes())[0]["t"] == FT_HELLO_ACK
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "order_count"}))
        assert _recv_text(ws) == {"jsonrpc": "2.0", "id": 1, "result": [3, "sales"]}
        _end(ws)


def test_a_catalog_from_the_file_opens(client: TestClient) -> None:
    with client.websocket_connect("/ws") as ws:
        ws.send_bytes(_hello_frame())
        assert unpack_frame(ws.receive_bytes())[0]["t"] == FT_HELLO_ACK
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "sales"}))
        while True:
            header, _ = unpack_frame(ws.receive_bytes())
            if header["t"] == FT_OPEN_ACK:
                break
        _end(ws)


def test_only_ws_is_served(client: TestClient) -> None:
    assert client.get("/").status_code == 404
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/shop/ws"):
            pass


def test_a_file_without_a_database_is_refused(tmp_path: Path) -> None:
    py_file = tmp_path / "empty.py"
    py_file.write_text("x = 1\n")
    with pytest.raises(ValueError, match="defines no Database"):
        database_app(py_file)

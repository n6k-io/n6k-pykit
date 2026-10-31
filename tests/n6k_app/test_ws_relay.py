"""``n6k ws``: each WebSocket dials its own connection to a listening ``n6k database``
worker, with binary (n6k) and text (JSON-RPC) messages relayed both ways."""

import asyncio
import json
import threading
import time
import uuid
from pathlib import Path
from typing import Iterator

import pytest
from starlette.testclient import TestClient

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import FT_HELLO, FT_HELLO_ACK, FT_OPEN, FT_OPEN_ACK, N6K_PROTOCOL_VERSION
from n6k_app import socket_app
from n6k_app.ws_relay import build_relay_app
from tests.n6k_app.test_socket_app import _TWO_DB_FILE, _extensions_loadable

pytestmark = pytest.mark.skipif(
    not _extensions_loadable(),
    reason="virtual_catalog/n6k_server could not be loaded; set N6K_EXTENSION_DIR to a local build",
)


@pytest.fixture
def worker_socket(tmp_path: Path) -> Iterator[str]:
    """A ``socket_app._listen`` worker on its own loop thread, cancelled on teardown."""
    py_file = tmp_path / "data.py"
    py_file.write_text(_TWO_DB_FILE)
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    loop = asyncio.new_event_loop()
    task = loop.create_task(socket_app._listen(str(py_file), path))
    thread = threading.Thread(target=lambda: loop.run_until_complete(asyncio.wait([task])))
    thread.start()
    deadline = time.monotonic() + 10.0
    while not Path(path).exists():
        assert time.monotonic() < deadline, f"worker never listened on {path}"
        time.sleep(0.025)
    try:
        yield path
    finally:
        loop.call_soon_threadsafe(task.cancel)
        thread.join(timeout=10.0)
        loop.close()


def test_concurrent_websockets_each_get_their_own_worker_connection(worker_socket: str) -> None:
    hello = {"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc"]}
    with TestClient(build_relay_app(worker_socket)) as client:
        with client.websocket_connect("/ws") as a, client.websocket_connect("/prefix/ws") as b:
            for ws in (a, b):
                ws.send_bytes(pack_frame(hello))
                assert unpack_frame(ws.receive_bytes())[0]["t"] == FT_HELLO_ACK

            a.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "sales"}))
            b.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "inventory"}))
            assert unpack_frame(a.receive_bytes())[0]["t"] == FT_OPEN_ACK
            assert unpack_frame(b.receive_bytes())[0]["t"] == FT_OPEN_ACK

            b.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "order_count"}))
            assert json.loads(b.receive_text()) == {"jsonrpc": "2.0", "id": 1, "result": [3, "sales"]}


def test_an_unreachable_worker_closes_the_websocket() -> None:
    with TestClient(build_relay_app(f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock")) as client:
        with client.websocket_connect("/ws") as ws:
            assert ws.receive()["code"] == 1011

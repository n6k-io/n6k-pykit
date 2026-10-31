"""The real ``n6k database`` socket worker (``socket_app._listen``), in-process over a
real Unix socket. Needs the extensions, so the module skips when they aren't loadable.
"""

import asyncio
import json
import socket
import struct
import uuid
from pathlib import Path
from typing import Any, Callable

import duckdb
import pyarrow as pa
import pytest

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    N6K_PROTOCOL_VERSION,
    OP_QUERY,
    OP_SCHEMA_LIST,
    STREAM_FRAME_JSONRPC,
    STREAM_FRAME_N6K,
)

from n6k_app import socket_app

# Two databases under distinct variable names; the variable name is the catalog
# alias a client opens.
_TWO_DB_FILE = """\
from typing import Annotated

import pandas as pd
from n6k_app import Database, Session, action

sales = Database()
inventory = Database()


@sales.data()
def orders():
    return pd.DataFrame({"id": [1, 2, 3]})


@inventory.data()
def stock():
    return pd.DataFrame({"id": [9]})


@action
def order_count(session: Annotated[Session, sales]) -> list:
    return [len(orders()), session.catalog]
"""


def _extensions_loadable() -> bool:
    from n6k_server.extension import load_n6k_server
    from virtual_catalog.extension import load_vcatprovider

    try:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_vcatprovider(con)
        load_n6k_server(con)
        con.close()
    except Exception:
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _extensions_loadable(),
    reason="virtual_catalog/n6k_server could not be loaded; set N6K_EXTENSION_DIR to a local build",
)


class _ClientConn:
    """Client end of the worker's socket: writes/reads ``[4-byte len][1-byte type][payload]``
    and collects inbound frames on a background task so responses can be awaited."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self.received: list[tuple[dict[str, Any], bytes]] = []
        self.texts: list[str] = []
        self._pump = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            while True:
                head = await self._reader.readexactly(5)
                n, frame_type = struct.unpack(">IB", head)
                payload = await self._reader.readexactly(n)
                if frame_type == STREAM_FRAME_JSONRPC:
                    self.texts.append(payload.decode())
                    continue
                header, body = unpack_frame(payload)
                self.received.append((header, bytes(body)))
        except asyncio.IncompleteReadError, asyncio.CancelledError:
            return

    async def send(self, header: dict[str, Any]) -> None:
        payload = pack_frame(header)
        self._writer.write(struct.pack(">IB", len(payload), STREAM_FRAME_N6K) + payload)
        await self._writer.drain()

    async def send_text(self, text: str) -> None:
        payload = text.encode()
        self._writer.write(struct.pack(">IB", len(payload), STREAM_FRAME_JSONRPC) + payload)
        await self._writer.drain()

    async def await_text(self, timeout: float = 10.0) -> str:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if self.texts:
                return self.texts.pop(0)
            await asyncio.sleep(0.005)
        raise AssertionError(f"no text frame received within {timeout}s")

    async def await_frame(self, predicate: Callable[[dict[str, Any]], bool], timeout: float = 10.0) -> dict[str, Any]:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            for header, _ in self.received:
                if predicate(header):
                    return header
            await asyncio.sleep(0.005)
        raise AssertionError(f"expected frame not received within {timeout}s; got={[h for h, _ in self.received]}")

    def arrow_rows(self, req_id: int, ns: int) -> int:
        """Rows in the response to ``(ns, req_id)``; its Arrow stream is split across
        frames, so the bodies concatenate."""
        stream = b"".join(
            body
            for header, body in self.received
            if header.get("id") == req_id
            and header.get("ns") == ns
            and header["t"] in (FT_RESP_SCHEMA, FT_RESP_CHUNK)
            and body
        )
        return sum(batch.num_rows for batch in pa.ipc.open_stream(pa.py_buffer(stream)))

    async def close(self) -> None:
        self._writer.close()
        self._pump.cancel()


def _open(ns: int, catalog: str) -> dict[str, Any]:
    return {"t": FT_OPEN, "ns": ns, "catalog": catalog}


def _schema_list_req(ns: int, req_id: int) -> dict[str, Any]:
    return {"t": FT_REQ, "op": OP_SCHEMA_LIST, "id": req_id, "ns": ns}


def _scan_req(ns: int, req_id: int, catalog: str, table: str) -> dict[str, Any]:
    return {
        "t": FT_REQ,
        "op": OP_QUERY,
        "id": req_id,
        "ns": ns,
        "sql": f'SELECT * FROM "{catalog}"."main"."{table}"',
    }


async def _worker_and_client(tmp_path: Path) -> tuple[_ClientConn, asyncio.Task[None]]:
    """The client end past FT_HELLO, plus the listening worker task."""
    py_file = tmp_path / "data.py"
    py_file.write_text(_TWO_DB_FILE)
    # pytest's tmp_path on macOS exceeds the AF_UNIX path limit; /tmp is reliably short.
    socket_path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    worker = asyncio.create_task(socket_app._listen(str(py_file), socket_path))
    return await _dial(socket_path), worker


async def _stop(worker: asyncio.Task[None]) -> None:
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker


async def test_one_socket_serves_all_databases_selected_by_catalog(tmp_path: Path) -> None:
    """Each FT_OPEN's alias selects its own database on a shared socket."""
    client, worker = await _worker_and_client(tmp_path)
    try:
        await client.send(_open(1, "sales"))
        await client.send(_open(2, "inventory"))
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 2)

        # Same req id on both sessions; each must resolve to its own database.
        await client.send(_scan_req(1, 1, "sales", "orders"))
        await client.send(_scan_req(2, 1, "inventory", "stock"))
        await client.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 1)
        await client.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 2)

        assert client.arrow_rows(req_id=1, ns=1) == 3  # sales.orders
        assert client.arrow_rows(req_id=1, ns=2) == 1  # inventory.stock
    finally:
        await client.close()
        await _stop(worker)


async def test_each_session_lists_only_its_own_catalog(tmp_path: Path) -> None:
    """SCHEMA_LIST is answered from the session's own catalog, not the connection's."""
    client, worker = await _worker_and_client(tmp_path)
    try:
        await client.send(_open(1, "sales"))
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await client.send(_schema_list_req(1, 7))
        chunk = await client.await_frame(lambda h: h["t"] == FT_RESP_CHUNK and h.get("ns") == 1)
        assert chunk["schemas"] == ["main"]
    finally:
        await client.close()
        await _stop(worker)


async def test_unknown_catalog_rejects_that_session_only(tmp_path: Path) -> None:
    """An OPEN for a catalog the file does not define is a clean per-session
    OPEN_ERR, and a sibling session on the same socket keeps serving."""
    client, worker = await _worker_and_client(tmp_path)
    try:
        await client.send(_open(1, "nope"))
        err = await client.await_frame(lambda h: h["t"] in (FT_OPEN_ERR, FT_ERR) and h.get("ns") == 1)
        assert "nope" in err["exception_message"]

        await client.send(_open(2, "sales"))
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 2)
        await client.send(_scan_req(2, 1, "sales", "orders"))
        await client.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 2)
        assert client.arrow_rows(req_id=1, ns=2) == 3
    finally:
        await client.close()
        await _stop(worker)


async def test_an_unknown_jsonrpc_method_gets_method_not_found(tmp_path: Path) -> None:
    """A JSON-RPC frame for a method the file does not define is answered -32601, and n6k
    frames on the same socket keep working."""
    client, worker = await _worker_and_client(tmp_path)
    try:
        await client.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "missing"}))
        reply = json.loads(await client.await_text())
        assert reply["id"] == 1 and reply["error"]["code"] == -32601
        await client.send(_open(1, "sales"))
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
    finally:
        await client.close()
        await _stop(worker)


async def test_an_action_answers_over_the_socket(tmp_path: Path) -> None:
    """A file's ``@action`` is a JSON-RPC method on the socket; its injected Session is the
    one for the database it names, and the members it imports resolve."""
    client, worker = await _worker_and_client(tmp_path)
    try:
        await client.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "order_count"}))
        assert json.loads(await client.await_text()) == {"jsonrpc": "2.0", "id": 1, "result": [3, "sales"]}
    finally:
        await client.close()
        await _stop(worker)


async def _dial(socket_path: str) -> _ClientConn:
    for _ in range(200):
        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
            break
        except FileNotFoundError, ConnectionRefusedError:
            await asyncio.sleep(0.025)
    else:
        raise AssertionError(f"could not dial {socket_path}")
    client = _ClientConn(reader, writer)
    await client.send(
        {"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc", "mcp"]}
    )
    await client.await_frame(lambda h: h["t"] == FT_HELLO_ACK)
    return client


async def test_listen_serves_concurrent_clients_and_replaces_a_stale_socket(tmp_path: Path) -> None:
    py_file = tmp_path / "data.py"
    py_file.write_text(_TWO_DB_FILE)
    socket_path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(socket_path)
    stale.close()
    assert Path(socket_path).exists()

    server = asyncio.create_task(socket_app._listen(str(py_file), socket_path))
    try:
        a = await _dial(socket_path)
        b = await _dial(socket_path)
        assert Path(socket_path).stat().st_mode & 0o777 == 0o600

        await a.send(_open(1, "sales"))
        await b.send(_open(1, "inventory"))
        await a.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await b.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)

        await a.close()

        await b.send(_scan_req(1, 1, "inventory", "stock"))
        await b.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 1)
        assert b.arrow_rows(req_id=1, ns=1) == 1

        c = await _dial(socket_path)
        await c.send(_open(1, "sales"))
        await c.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await c.send(_scan_req(1, 1, "sales", "orders"))
        await c.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 1)
        assert c.arrow_rows(req_id=1, ns=1) == 3
        await b.close()
        await c.close()
    finally:
        server.cancel()
        with pytest.raises(asyncio.CancelledError):
            await server
    assert not Path(socket_path).exists()


async def test_listen_refuses_a_live_socket(tmp_path: Path) -> None:
    py_file = tmp_path / "data.py"
    py_file.write_text(_TWO_DB_FILE)
    socket_path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    live = await asyncio.start_unix_server(lambda r, w: None, path=socket_path)
    try:
        with pytest.raises(ValueError, match="in use"):
            await socket_app._listen(str(py_file), socket_path)
    finally:
        live.close()
        Path(socket_path).unlink(missing_ok=True)

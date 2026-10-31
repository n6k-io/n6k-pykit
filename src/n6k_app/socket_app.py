"""Serve a file's databases over a Unix-domain socket the worker listens on
(``run_listen``). Framing: ``StreamTransport``.

The dialing parent authenticates each connection; the worker goes straight to serving.
Every database the file defines is attached to one connection handed to the
``n6k_server`` extension; each client FT_OPEN's ``catalog`` alias selects which of them
the session reads. JSON-RPC frames call the file's ``@action``s.
"""

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from n6k_protocol.protocol import STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K
from n6k_server.pump import STREAM_HEADER

from n6k_app.discover import load_database_file
from n6k_app.serve import serve_databases

logger = logging.getLogger(__name__)


class StreamTransport:
    """The ``WebSocketLike`` the pump serves, over an asyncio stream framed by ``STREAM_HEADER``:
    type ``STREAM_FRAME_N6K`` for binary and ``STREAM_FRAME_JSONRPC`` for text. The header
    supplies the message boundaries and types a WebSocket gives the pump for free."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._closed = False

    async def receive(self) -> dict[str, Any]:
        """The next frame as an ASGI websocket message; end of stream is a disconnect."""
        header = await self._read_exact(STREAM_HEADER.size)
        if header is None:
            return {"type": "websocket.disconnect", "code": 1000}
        length, frame_type = STREAM_HEADER.unpack(header)
        if frame_type not in (STREAM_FRAME_N6K, STREAM_FRAME_JSONRPC):
            raise ValueError(f"unknown stream frame type 0x{frame_type:02x}")
        payload = await self._read_exact(length)
        if payload is None:
            return {"type": "websocket.disconnect", "code": 1000}
        if frame_type == STREAM_FRAME_JSONRPC:
            return {"type": "websocket.receive", "text": payload.decode()}
        return {"type": "websocket.receive", "bytes": payload}

    async def _read_exact(self, n: int) -> bytes | None:
        if n == 0:
            return b""
        try:
            return await self._reader.readexactly(n)
        except asyncio.IncompleteReadError:
            return None

    async def send_bytes(self, data: bytes) -> None:
        self._writer.write(STREAM_HEADER.pack(len(data), STREAM_FRAME_N6K) + data)
        await self._writer.drain()

    async def send_text(self, data: str) -> None:
        payload = data.encode()
        self._writer.write(STREAM_HEADER.pack(len(payload), STREAM_FRAME_JSONRPC) + payload)
        await self._writer.drain()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except Exception:
            logger.debug("socket close on already-closed transport", exc_info=True)


def _claim_socket_path(path: str) -> None:
    """Remove a stale socket file at ``path``; raise if a server is live on it."""
    if not os.path.exists(path):
        return
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(path)
    except ConnectionRefusedError, FileNotFoundError:
        os.unlink(path)
        return
    finally:
        probe.close()
    raise ValueError(f"{path} is in use by another server")


async def serve_unix(path: str, on_conn: Callable[[StreamTransport], Awaitable[None]]) -> None:
    """Listen on ``path`` (mode 0600) and run ``on_conn`` per client until cancelled."""
    _claim_socket_path(path)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        transport = StreamTransport(reader, writer)
        try:
            await on_conn(transport)
        except Exception:
            logger.exception("socket client failed")
        finally:
            await transport.close()

    server = await asyncio.start_unix_server(handle, path)
    os.chmod(path, 0o600)
    logger.info("listening on %s", path)
    try:
        await server.serve_forever()
    finally:
        server.close()
        Path(path).unlink(missing_ok=True)


async def _listen(py_file: str, path: str) -> None:
    databases, actions = load_database_file(py_file)
    name = Path(py_file).stem
    for db in databases.values():
        db.start()
    try:
        await serve_unix(path, lambda transport: serve_databases(transport, name, databases, actions))
    finally:
        for db in databases.values():
            db.close()


def run_listen(py_file: str, path: str) -> None:
    try:
        asyncio.run(_listen(py_file, path))
    except KeyboardInterrupt:
        pass

"""WebSocket ⇄ Unix-socket relay for testing the ``n6k database`` socket worker.

The worker (``--listen``) speaks n6k stream framing over a Unix socket,
``[4-byte big-endian length][1-byte type][payload]``; the ``duckdb`` binary attaches
over a WebSocket. Each WebSocket gets its own connection to the worker, and the relay
pumps frames between them, mapping the type byte to and from WebSocket binary (n6k)
and text (JSON-RPC) messages::

    n6k database --listen ./db.sock mydb.py   # terminal 1
    n6k ws ./db.sock --port 7823              # terminal 2
    # then in duckdb:  ATTACH 'n6k://127.0.0.1:7823' AS x (TYPE n6k);
"""

import asyncio
import logging

import uvicorn
from n6k_protocol.protocol import STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K
from n6k_server.pump import STREAM_HEADER
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


async def _ws_to_unix(ws: WebSocket, writer: asyncio.StreamWriter) -> None:
    while True:
        try:
            message = await ws.receive()
        except WebSocketDisconnect, RuntimeError:
            return
        if message["type"] == "websocket.disconnect":
            return
        if message.get("bytes") is not None:
            data, frame_type = message["bytes"], STREAM_FRAME_N6K
        else:
            data, frame_type = message.get("text", "").encode(), STREAM_FRAME_JSONRPC
        writer.write(STREAM_HEADER.pack(len(data), frame_type) + data)
        await writer.drain()


async def _unix_to_ws(reader: asyncio.StreamReader, ws: WebSocket) -> None:
    while True:
        try:
            header = await reader.readexactly(STREAM_HEADER.size)
            length, frame_type = STREAM_HEADER.unpack(header)
            payload = await reader.readexactly(length) if length else b""
        except asyncio.IncompleteReadError:
            return
        if frame_type not in (STREAM_FRAME_N6K, STREAM_FRAME_JSONRPC):
            logger.error("closing: unknown stream frame type 0x%02x from the worker", frame_type)
            return
        try:
            if frame_type == STREAM_FRAME_JSONRPC:
                await ws.send_text(payload.decode())
            else:
                await ws.send_bytes(payload)
        except WebSocketDisconnect, RuntimeError:
            return


def build_relay_app(socket_path: str) -> Starlette:
    async def relay(ws: WebSocket) -> None:
        await ws.accept()
        try:
            reader, writer = await asyncio.open_unix_connection(socket_path)
        except OSError as exc:
            logger.error("could not dial unix:%s: %s", socket_path, exc)
            await ws.close(code=1011, reason="worker unreachable")
            return
        logger.info("duckdb attached at %s; dialed unix:%s", ws.url.path, socket_path)
        t_in = asyncio.create_task(_ws_to_unix(ws, writer))
        t_out = asyncio.create_task(_unix_to_ws(reader, ws))
        try:
            await asyncio.wait({t_in, t_out}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (t_in, t_out):
                t.cancel()
            writer.close()
            try:
                await ws.close()
            except Exception:
                pass
            logger.info("relay session ended")

    return Starlette(
        routes=[
            WebSocketRoute("/ws", relay),
            WebSocketRoute("/{prefix:path}/ws", relay),
        ],
    )


def run_relay(socket_path: str, host: str, port: int, log_level: str = "info") -> None:
    app = build_relay_app(socket_path)
    uvicorn.run(app, host=host, port=port, log_level=log_level)

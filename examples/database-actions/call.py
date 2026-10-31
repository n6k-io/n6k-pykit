"""Call an action on an `n6k database` socket over JSON-RPC, or list its MCP tools.

uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws order_count
uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws items_after '{"min_id": 2}'
uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws tools/list
"""

import asyncio
import json
import sys
from typing import Any

import websockets
from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import FT_HELLO, FT_HELLO_ACK, N6K_PROTOCOL_VERSION


async def _request(ws: Any, req_id: int, method: str, params: Any = None) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        message["params"] = params
    await ws.send(json.dumps(message))
    while True:
        reply = await ws.recv()
        if isinstance(reply, str):
            return dict(json.loads(reply))


async def main(url: str, method: str, params: Any) -> None:
    async with websockets.connect(url) as ws:
        await ws.send(
            pack_frame(
                {
                    "t": FT_HELLO,
                    "token": "",
                    "protocol_version": N6K_PROTOCOL_VERSION,
                    "capabilities": ["jsonrpc", "mcp"],
                }
            )
        )
        header, _ = unpack_frame(await ws.recv())
        if header["t"] != FT_HELLO_ACK:
            raise SystemExit(f"handshake failed: {header}")
        print("granted:", header.get("capabilities"))

        if method.startswith("tools/"):
            client = {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "call.py", "version": "0"},
            }
            await _request(ws, 0, "initialize", client)
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}))

        print(json.dumps(await _request(ws, 1, method, params), indent=2))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    asyncio.run(main(sys.argv[1], sys.argv[2], json.loads(sys.argv[3]) if len(sys.argv) > 3 else None))

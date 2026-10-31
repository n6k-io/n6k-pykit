"""``n6k database`` over HTTP: a single ``/ws`` serving one file's databases and actions."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket

from n6k_app.discover import load_database_file
from n6k_app.serve import serve_databases


def database_app(py_file: str | Path) -> Starlette:
    name = Path(py_file).stem
    databases, actions = load_database_file(py_file)

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        for db in databases.values():
            db.start()
        try:
            yield
        finally:
            for db in databases.values():
                db.close()

    async def ws_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        await serve_databases(ws, name, databases, actions)

    return Starlette(lifespan=lifespan, routes=[WebSocketRoute("/ws", ws_endpoint)])

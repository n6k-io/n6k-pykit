"""Serve a file's databases over a Unix-domain socket the worker listens on
(``run_listen``). Framing: ``n6k_server.stream.StreamTransport``.

The dialer authenticates each connection; the worker serves via ``serve.serve_databases``.
"""

import asyncio
from pathlib import Path

from n6k_server.stream import serve_unix

from n6k_app.discover import load_database_file
from n6k_app.serve import serve_databases


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

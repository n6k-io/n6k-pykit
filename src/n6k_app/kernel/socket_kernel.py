"""``n6k kernel``: listen on a path and serve the kernel to every client that connects.

Each connection carries the ``kernel`` catalog (n6k binary frames) and the exec API
(JSON-RPC text frames)."""

import asyncio
import logging
import os
import sys
from typing import Any

import duckdb
from n6k_server.jsonrpc import JsonRpcApp
from n6k_server.pump import serve_and_close_connection

from n6k_app.kernel.capture import Capture
from n6k_app.kernel.catalog import CATALOG, open_catalog
from n6k_app.kernel.kernel import Kernel
from n6k_app.socket_app import StreamTransport, serve_unix

log = logging.getLogger(__name__)


async def serve_connection(kernel: Kernel, transport: StreamTransport) -> None:
    served_cursor: duckdb.DuckDBPyConnection | None = None
    try:
        con = await open_catalog(
            kernel.global_vars, kernel.local_vars, invalidator=kernel.invalidator, catalog=kernel.catalog
        )

        def authenticate(_ws: Any, served: duckdb.DuckDBPyConnection, _token: str, **_params: str) -> list[str]:
            nonlocal served_cursor
            served_cursor = served.cursor()
            kernel.invalidator.bind(asyncio.get_running_loop(), served_cursor)
            return [kernel.catalog]

        await serve_and_close_connection(
            transport,
            con,
            authenticate,
            {},
            text_app=JsonRpcApp(kernel.rpc),
            on_serve=kernel.on_serve,
        )
    finally:
        kernel.disconnect(transport)
        if served_cursor is not None:
            kernel.invalidator.unbind(served_cursor)


def _start_kernel(catalog: str) -> Kernel:
    kernel = Kernel(catalog)
    seed = os.environ.get("N6K_SEED")
    if seed:
        kernel.apply_seed(seed)
    real_stdout = sys.stdout
    sys.stdout = Capture("stdout", real_stdout)
    sys.stderr = Capture("stderr", real_stdout)
    return kernel


async def listen(kernel: Kernel, path: str) -> None:
    await serve_unix(path, lambda transport: serve_connection(kernel, transport))


def run_kernel_listen(path: str, catalog: str = CATALOG) -> None:
    try:
        asyncio.run(listen(_start_kernel(catalog), path))
    except KeyboardInterrupt:
        pass

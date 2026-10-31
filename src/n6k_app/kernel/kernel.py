"""The kernel: one namespace, its worker threads, and the JSON-RPC exec API.

The namespace outlives connections; cell output that passes the gate is sent to every
connected client and recorded in the cell state, even with no client connected."""

import asyncio
import json
import logging
import os
import sys
import threading
from concurrent.futures import Future
from itertools import count
from typing import Any, Optional

from n6k_server.context import Connection
from n6k_server.jsonrpc import INTERNAL_ERROR, INVALID_PARAMS, JsonRpc, JsonRpcError
from n6k_server.pump import LiveServe

from n6k_app.kernel.capture import CELL, display
from n6k_app.kernel.catalog import CATALOG, DEFAULT_SCHEMA, CatalogInvalidator
from n6k_app.kernel.gate import OutputGate
from n6k_app.kernel.state import KernelState
from n6k_app.kernel.worker import Worker

log = logging.getLogger(__name__)

CELL_BUSY = -32001

_Done = tuple[dict[str, bool], list[Future[bool]]]


class Kernel:
    def __init__(self, catalog: str = CATALOG) -> None:
        self.catalog = catalog
        self.global_vars: dict[str, Any] = {"display": display}
        self.local_vars: dict[str, Any] = {}
        self.invalidator = CatalogInvalidator(catalog)
        self._workers: dict[str, Worker] = {}
        self._active: dict[str, tuple[Worker, int]] = {}
        self._waiters: dict[int, asyncio.Future[_Done]] = {}
        self._targets: list[tuple[Any, asyncio.AbstractEventLoop, Connection]] = []
        self.gate = OutputGate()
        self.state = KernelState()
        self._lock = threading.Lock()
        self._seq = 0
        self._runs = count(1)
        self.rpc = JsonRpc()
        self.rpc.method("process.execute")(self.execute)
        self.rpc.method("process.interrupt")(self.interrupt)
        self.rpc.method("process.cells")(self.cells)
        self.rpc.method("process.cell_state")(self.cell_state)
        self.rpc.method("process.forget")(self.forget)

    def apply_seed(self, seed: str) -> None:
        """Run ``seed`` (inline code or a file path) into the globals."""
        code = seed
        if os.path.exists(seed):
            with open(seed) as f:
                code = f.read()
        log.info("applying N6K_SEED")
        exec(compile(code, "<n6k_seed>", "exec"), self.global_vars)

    def emit(self, method: str, params: dict[str, Any], run: Optional[int] = None) -> None:
        """Callable from any thread."""
        if run is None:
            run = getattr(CELL, "run", None)
        size = 0
        if method == "process.display":
            params = _json_safe_display(params)
            size = len(json.dumps(params["data"]))
        elif method == "process.stream":
            size = len(json.dumps(params["data"]))
        with self._lock:
            dropped = self._broadcast(self.gate.check(method, params, run, size))
        for name in dropped:
            log.debug("dropping %s: loop not running", name)

    def _broadcast(self, notifications: list[tuple[str, dict[str, Any]]]) -> list[str]:
        """Holding ``_lock``: number, apply and schedule ``notifications``. No I/O here: a
        stderr write would re-enter ``emit`` through ``Capture`` and deadlock."""
        dropped = []
        for method, params in notifications:
            self._seq += 1
            params = {**params, "seq": self._seq}
            self.state.apply(method, params)
            sent = []
            for _ws, loop, connection in self._targets:
                try:
                    sent.append(asyncio.run_coroutine_threadsafe(connection.notify(method, params), loop))
                except RuntimeError:
                    dropped.append(method)
            if method == "process.status" and params["state"] == "completed":
                self._finish(params, sent)
        return dropped

    def _finish(self, params: dict[str, Any], sent: list[Future[bool]]) -> None:
        _worker, run = self._active.pop(params["cell_id"])
        waiter = self._waiters.get(run)
        if waiter is None:
            return
        result = {"success": params["success"], "interrupted": params["interrupted"]}
        try:
            waiter.get_loop().call_soon_threadsafe(_resolve, waiter, (result, sent))
        except RuntimeError:
            pass

    async def execute(self, cell_id: str, code: str, thread: str = "main") -> dict[str, bool]:
        with self._lock:
            status = self.state.status(cell_id)
        if status in ("queued", "running"):
            raise JsonRpcError(CELL_BUSY, f"cell {cell_id!r} is {status}")
        worker = self._workers.get(thread)
        if worker is None:
            worker = Worker(self.emit, self.global_vars, self.local_vars, self._bindings_changed)
            worker.start()
            self._workers[thread] = worker
        run = next(self._runs)
        waiter: asyncio.Future[_Done] = asyncio.get_running_loop().create_future()
        self._waiters[run] = waiter
        try:
            self._active[cell_id] = (worker, run)
            self.emit("process.status", {"cell_id": cell_id, "state": "queued", "code": code, "thread": thread}, run)
            worker.add_work(cell_id, code, run)
            result, sent = await waiter
            # The reply must follow ``process.status`` completed on this connection.
            await asyncio.gather(*(asyncio.wrap_future(f) for f in sent), return_exceptions=True)
            return result
        finally:
            del self._waiters[run]

    def cells(self) -> dict[str, Any]:
        with self._lock:
            return {"seq": self._seq, "cells": self.state.cells()}

    def cell_state(self, cell_id: str) -> dict[str, Any]:
        with self._lock:
            cell = self.state.cell(cell_id)
            seq = self._seq
        if cell is None:
            raise JsonRpcError(INVALID_PARAMS, f"unknown cell {cell_id!r}")
        return {"seq": seq, "cell": cell}

    def forget(self, cell_id: str) -> None:
        with self._lock:
            status = self.state.status(cell_id)
            if status is None:
                raise JsonRpcError(INVALID_PARAMS, f"unknown cell {cell_id!r}")
            if status != "completed":
                raise JsonRpcError(INVALID_PARAMS, f"cell {cell_id!r} is {status}")
            self.gate.forget(cell_id)
            dropped = self._broadcast([("process.forgotten", {"cell_id": cell_id})])
        for name in dropped:
            log.debug("dropping %s: loop not running", name)

    def interrupt(self, cell_id: str) -> None:
        active = self._active.get(cell_id)
        if active is None:
            with self._lock:
                status = self.state.status(cell_id)
            if status is None:
                raise JsonRpcError(INVALID_PARAMS, f"unknown cell {cell_id!r}")
            return
        worker, run = active
        try:
            worker.interrupt(run)
        except (ValueError, SystemError) as exc:
            raise JsonRpcError(INTERNAL_ERROR, f"could not interrupt {cell_id!r}: {exc}") from exc

    def _bindings_changed(self) -> None:
        self.invalidator.invalidate([DEFAULT_SCHEMA])

    async def on_serve(self, ws: Any, handle: LiveServe) -> None:
        self._targets = [*self._targets, (ws, asyncio.get_running_loop(), handle.connection)]
        await handle.connection.notify(
            "process.ready", {"language": "python", "version": sys.version, "executable": sys.executable}
        )

    def disconnect(self, ws: Any) -> None:
        self._targets = [t for t in self._targets if t[0] is not ws]


def _json_safe_display(params: dict[str, Any]) -> dict[str, Any]:
    """``params`` with ``data`` as a JSON copy, so later mutation of the displayed object
    changes neither the state nor what is sent; data that is not JSON becomes its repr."""
    data = params["data"]
    if isinstance(data, str):
        return params
    try:
        return {**params, "data": json.loads(json.dumps(data))}
    except TypeError, ValueError:
        return {**params, "mime": "text/plain", "data": repr(data)}


def _resolve(waiter: asyncio.Future[Any], value: Any) -> None:
    if not waiter.done():
        waiter.set_result(value)

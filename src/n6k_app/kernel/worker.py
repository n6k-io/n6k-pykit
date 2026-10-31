"""One named execution thread: runs its cells in order against the shared namespace."""

import ast
import ctypes
import logging
import tempfile
import threading
import time
import traceback
from collections.abc import Iterator, MutableMapping
from queue import Queue
from typing import Any, Callable, Optional

from n6k_app.kernel.capture import CELL, Emit

log = logging.getLogger(__name__)


def add_display(mod: ast.Module) -> None:
    """Wrap a trailing expression as ``display(expr, title="expr")``."""
    if not mod.body or not isinstance(mod.body[-1], ast.Expr):
        return

    expr = mod.body[-1]
    value = expr.value
    # An explicit display(...) returns its displayId, which would be re-displayed.
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "display":
        return
    loc: dict[str, Any] = {"lineno": value.lineno, "col_offset": value.col_offset}
    expr.value = ast.Call(
        func=ast.Name(id="display", ctx=ast.Load(), **loc),
        args=[value],
        keywords=[ast.keyword(arg="title", value=ast.Constant(value=ast.unparse(value), **loc), **loc)],
        **loc,
    )


class LocalProxy(MutableMapping[str, Any]):
    """Records which top-level names a cell bound or deleted."""

    def __init__(self, inner: dict[str, Any]) -> None:
        self.inner = inner
        self.added: set[str] = set()
        self.removed: set[str] = set()

    def __setitem__(self, key: str, value: Any) -> None:
        self.added.add(key)
        self.inner[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.inner[key]

    def __delitem__(self, key: str) -> None:
        self.removed.add(key)
        del self.inner[key]

    def __contains__(self, key: object) -> bool:
        return key in self.inner

    def __len__(self) -> int:
        return len(self.inner)

    def __iter__(self) -> Iterator[str]:
        return iter(self.inner)


class Worker(threading.Thread):
    def __init__(
        self,
        emit: Emit,
        global_vars: dict[str, Any],
        local_vars: dict[str, Any],
        on_bindings_changed: Optional[Callable[[], None]] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.emit = emit
        self.global_vars = global_vars
        self.local_vars = local_vars
        self.on_bindings_changed = on_bindings_changed
        self.work_queue: Queue[tuple[str, str, int]] = Queue()
        self._interrupt_lock = threading.Lock()
        self._interruptible = False
        self._run: Optional[int] = None
        self._finished = 0
        self._cancelled: set[int] = set()
        self._interrupted: set[int] = set()

    def add_work(self, cell_id: str, code: str, run: int) -> None:
        self.work_queue.put((cell_id, code, run))

    def interrupt(self, run: int) -> None:
        """Raise KeyboardInterrupt in ``run``'s code at its next bytecode boundary, or keep a
        ``run`` that has not reached its code from running it; blocking native calls are not
        interrupted. A finished ``run`` is ignored. Raises ValueError or SystemError when
        ``PyThreadState_SetAsyncExc`` fails.

        At most one is pending per cell, and it can only land inside the cell's ``try``."""
        with self._interrupt_lock:
            if run <= self._finished:
                return
            if run != self._run or not self._interruptible or self.ident is None:
                self._cancelled.add(run)
                return
            self._interruptible = False
            tid = ctypes.c_ulong(self.ident)
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(KeyboardInterrupt))
            if res == 0:
                raise ValueError("invalid thread id")
            if res != 1:
                # More than one thread state was hit: revert, per the C API docs.
                ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
                raise SystemError("PyThreadState_SetAsyncExc failed")
            self._interrupted.add(run)

    def _stop_interrupts(self) -> None:
        """Close the window, wait out an ``interrupt`` that already saw it open, then drop
        its exception if it has not fired yet.

        Never takes the lock: the pending exception can fire at any bytecode, and firing
        just after an acquire would leak it. Every step is safe to repeat, and at most one
        exception can be pending, so retrying after it fires is enough."""
        while True:
            try:
                self._interruptible = False
                while self._interrupt_lock.locked():
                    time.sleep(0)
                if self.ident is not None:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(self.ident), None)
                return
            except KeyboardInterrupt:
                continue

    def run(self) -> None:
        CELL.emit = self.emit
        while True:
            cell_id, code, run = self.work_queue.get()
            self.execute(cell_id, code, run)

    def execute(self, cell_id: str, code: str, run: int) -> None:
        CELL.cell_id = cell_id
        CELL.run = run
        with self._interrupt_lock:
            self._run = run
            cancelled = run in self._cancelled
        local_proxy = LocalProxy(self.local_vars)
        success = False
        if not cancelled:
            success = self._exec(cell_id, code, run, local_proxy)
        with self._interrupt_lock:
            self._finished = run
            interrupted = not success and (run in self._cancelled or run in self._interrupted)
            self._cancelled.discard(run)
            self._interrupted.discard(run)
        self.emit(
            "process.status",
            {
                "cell_id": cell_id,
                "state": "completed",
                "success": success,
                "interrupted": interrupted,
                "added_vars": sorted(local_proxy.added),
            },
        )
        if self.on_bindings_changed is not None and (local_proxy.added or local_proxy.removed):
            self.on_bindings_changed()

    def _exec(self, cell_id: str, code: str, run: int, local_proxy: LocalProxy) -> bool:
        self.emit("process.status", {"cell_id": cell_id, "state": "running"})
        with tempfile.NamedTemporaryFile("w", suffix=".py", prefix=cell_id) as fd:
            fd.write(code)
            fd.close()
            try:
                tree = ast.parse(code, filename=fd.name, mode="exec")
                if not code.strip().endswith(";"):
                    add_display(tree)
                compiled = compile(tree, fd.name, "exec")
                with self._interrupt_lock:
                    cancelled = run in self._cancelled
                    self._interruptible = not cancelled
                if cancelled:
                    raise KeyboardInterrupt
                try:
                    exec(compiled, self.global_vars, local_proxy)
                finally:
                    self._stop_interrupts()
                return True
            except BaseException:
                log.debug("cell %s failed", cell_id, exc_info=True)
                self.emit("process.stream", {"cell_id": cell_id, "target": "stderr", "data": traceback.format_exc()})
                return False

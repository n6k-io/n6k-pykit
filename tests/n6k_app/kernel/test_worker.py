import ast
import io
import queue
import threading
from typing import Any

import pytest

from n6k_app.kernel.capture import CELL, Capture, display
from n6k_app.kernel.worker import Worker, add_display


def _rewrite(code: str) -> str:
    tree = ast.parse(code, mode="exec")
    add_display(tree)
    # compile guards against missing lineno/col_offset on synthesized nodes.
    compile(tree, "<test>", "exec")
    return ast.unparse(tree)


def test_last_expression_is_wrapped_with_title() -> None:
    assert _rewrite("fig") == "display(fig, title='fig')"
    assert _rewrite("df.head(10)") == "display(df.head(10), title='df.head(10)')"


def test_explicit_display_is_not_double_wrapped() -> None:
    src = "display(fig, displayId='chart')"
    assert _rewrite(src) == "display(fig, displayId='chart')"


def test_non_expression_last_statement_is_untouched() -> None:
    assert _rewrite("x = 5") == "x = 5"


def test_empty_module_is_noop() -> None:
    assert _rewrite("") == ""


def test_only_last_expression_of_many_is_wrapped() -> None:
    out = _rewrite("a = 1\nb = 2\na + b")
    assert out == "a = 1\nb = 2\ndisplay(a + b, title='a + b')"


class _Emitted:
    def __init__(self) -> None:
        self.q: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()

    def __call__(self, method: str, params: dict[str, Any]) -> None:
        self.q.put((method, params))

    def until_completed(self, timeout: float = 10.0) -> list[tuple[str, dict[str, Any]]]:
        out = []
        while True:
            method, params = self.q.get(timeout=timeout)
            out.append((method, params))
            if method == "process.status" and params["state"] == "completed":
                return out


def test_capture_without_cell_writes_fallback() -> None:
    fallback = io.StringIO()
    t = threading.Thread(target=lambda: print("hi", file=Capture("stdout", fallback)))
    t.start()
    t.join()
    assert fallback.getvalue() == "hi\n"


def test_display_outside_cell_raises() -> None:
    def run() -> None:
        with pytest.raises(RuntimeError):
            display(1)

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert not hasattr(CELL, "cell_id")


def _worker(emitted: _Emitted, g: dict[str, Any], loc: dict[str, Any], changed: Any = None) -> Worker:
    w = Worker(emitted, g, loc, changed)
    w.start()
    return w


def test_cell_streams_displays_and_completes() -> None:
    emitted = _Emitted()
    g: dict[str, Any] = {"display": display, "out": Capture("stdout", io.StringIO())}
    loc: dict[str, Any] = {}
    changed = threading.Event()
    w = _worker(emitted, g, loc, changed.set)
    w.add_work("c1", "x = 41\nprint('hi', file=out)\nx + 1", 1)
    msgs = emitted.until_completed()
    assert msgs[0] == ("process.status", {"cell_id": "c1", "state": "running"})
    assert msgs[1] == ("process.stream", {"cell_id": "c1", "target": "stdout", "data": "hi"})
    method, disp = msgs[-2]
    assert method == "process.display"
    assert disp["cell_id"] == "c1" and disp["mime"] == "text/plain" and disp["data"] == "42"
    assert disp["title"] == "x + 1"
    assert msgs[-1] == (
        "process.status",
        {"cell_id": "c1", "state": "completed", "success": True, "interrupted": False, "added_vars": ["x"]},
    )
    assert loc["x"] == 41
    assert changed.wait(5)


def test_failing_cell_streams_traceback() -> None:
    emitted = _Emitted()
    w = _worker(emitted, {"display": display}, {})
    w.add_work("c2", "1 / 0", 1)
    msgs = emitted.until_completed()
    stderr = [p for m, p in msgs if m == "process.stream" and p["target"] == "stderr"]
    assert stderr and "ZeroDivisionError" in stderr[0]["data"]
    assert msgs[-1][1]["success"] is False
    assert msgs[-1][1]["interrupted"] is False


def test_keyboard_interrupt_raised_by_the_cell_is_not_interrupted() -> None:
    emitted = _Emitted()
    w = _worker(emitted, {"display": display}, {})
    w.add_work("c", "raise KeyboardInterrupt", 1)
    assert emitted.until_completed()[-1][1]["interrupted"] is False


def test_interrupt_stops_running_cell_and_worker_survives() -> None:
    emitted = _Emitted()
    running = threading.Event()
    w = _worker(emitted, {"display": display, "running": running}, {})
    w.add_work("loop", "running.set()\nwhile True:\n    pass", 1)
    assert running.wait(5)
    w.interrupt(1)
    msgs = emitted.until_completed()
    assert any("KeyboardInterrupt" in p.get("data", "") for _, p in msgs)
    assert msgs[-1][1]["success"] is False and msgs[-1][1]["interrupted"] is True
    w.add_work("after", "1;", 2)
    assert emitted.until_completed()[-1][1] == {
        "cell_id": "after",
        "state": "completed",
        "success": True,
        "interrupted": False,
        "added_vars": [],
    }


def test_interrupted_queued_cell_never_starts() -> None:
    emitted = _Emitted()
    running = threading.Event()
    stop = threading.Event()
    w = _worker(emitted, {"display": display, "running": running, "stop": stop}, {})
    w.add_work("first", "running.set()\nstop.wait(10);", 1)
    w.add_work("queued", "x = 1", 2)
    assert running.wait(5)
    w.interrupt(2)
    stop.set()
    assert emitted.until_completed()[-1][1]["success"] is True
    assert emitted.until_completed() == [
        (
            "process.status",
            {"cell_id": "queued", "state": "completed", "success": False, "interrupted": True, "added_vars": []},
        )
    ]


def test_interrupt_of_another_run_leaves_the_running_cell_alone() -> None:
    emitted = _Emitted()
    running = threading.Event()
    stop = threading.Event()
    w = _worker(emitted, {"display": display, "running": running, "stop": stop}, {})
    w.add_work("c", "running.set()\nstop.wait(10);", 2)
    assert running.wait(5)
    w.interrupt(1)
    w.interrupt(3)
    stop.set()
    assert emitted.until_completed()[-1][1]["success"] is True


def test_interrupt_on_idle_worker_is_a_noop() -> None:
    emitted = _Emitted()
    w = _worker(emitted, {"display": display}, {})
    w.interrupt(1)
    w.add_work("c", "1;", 2)
    assert emitted.until_completed()[-1][1]["success"] is True


def test_every_cell_completes_under_constant_interrupts() -> None:
    """Interrupts hammered across cell boundaries never cost a cell its ``completed``
    and never kill the worker."""
    emitted = _Emitted()
    w = _worker(emitted, {"display": display}, {})
    stop = threading.Event()

    def hammer() -> None:
        while not stop.is_set():
            w.interrupt(w._run or 0)

    t = threading.Thread(target=hammer)
    t.start()
    try:
        cells = [f"c{i}" for i in range(100)]
        for run, cell_id in enumerate(cells, 1):
            w.add_work(cell_id, "x = sum(range(1000));", run)
        completed = []
        while len(completed) < len(cells):
            method, params = emitted.q.get(timeout=10)
            if method == "process.status" and params["state"] == "completed":
                completed.append(params["cell_id"])
        assert completed == cells
    finally:
        stop.set()
        t.join()
    w.add_work("last", "1;", len(cells) + 1)
    assert emitted.until_completed()[-1][1] == {
        "cell_id": "last",
        "state": "completed",
        "success": True,
        "interrupted": False,
        "added_vars": [],
    }

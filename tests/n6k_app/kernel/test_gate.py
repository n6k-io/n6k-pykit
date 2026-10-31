from typing import Any

from n6k_app.kernel.gate import CELL_CAP, OutputGate


def _queued(gate: OutputGate, run: int, cell_id: str = "c", code: str = "") -> None:
    assert gate.check("process.status", {"cell_id": cell_id, "state": "queued", "code": code, "thread": "main"}, run)


def _stream(gate: OutputGate, run: int, size: int) -> list[tuple[str, dict[str, Any]]]:
    return gate.check("process.stream", {"cell_id": "c", "target": "stdout", "data": "x"}, run, size)


def _display(gate: OutputGate, run: int, size: int, method: str, display_id: str = "d") -> list[tuple[str, Any]]:
    params = {"cell_id": "c", "displayId": display_id, "method": method, "mime": "text/plain", "data": "x"}
    return gate.check("process.display", params, run, size)


def test_default_cap_is_10_mib() -> None:
    assert CELL_CAP == 10 * 2**20


def test_code_is_not_capped() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1, code="x" * 100)
    assert _stream(gate, 1, 10)


def test_stale_run_and_unknown_cell_are_dropped() -> None:
    gate = OutputGate()
    _queued(gate, 1)
    _queued(gate, 2)
    assert _stream(gate, 1, 1) == []
    assert (
        gate.check("process.status", {"cell_id": "c", "state": "completed", "success": True, "added_vars": []}, 1) == []
    )
    assert _stream(gate, 2, 1)
    assert gate.check("process.status", {"cell_id": "other", "state": "running"}, 2) == []


def test_stream_past_cap_is_dropped_and_truncated_sent_once() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1)
    assert _stream(gate, 1, 6)
    assert _stream(gate, 1, 5) == [("process.truncated", {"cell_id": "c"})]
    assert _stream(gate, 1, 5) == []
    assert _stream(gate, 1, 4)
    assert gate.check("process.status", {"cell_id": "c", "state": "completed", "success": True, "added_vars": []}, 1)


def test_append_counts_with_streams_and_replace_frees_it() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1)
    assert _stream(gate, 1, 4)
    assert _display(gate, 1, 3, "append")
    assert _display(gate, 1, 3, "prepend")
    assert _display(gate, 1, 1, "append") == [("process.truncated", {"cell_id": "c"})]
    assert _display(gate, 1, 1, "replace")
    assert _stream(gate, 1, 5)


def test_new_displays_count_toward_the_cap() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1)
    assert _display(gate, 1, 4, "replace", "a")
    assert _display(gate, 1, 4, "replace", "b")
    assert _display(gate, 1, 4, "replace", "c") == [("process.truncated", {"cell_id": "c"})]
    assert _display(gate, 1, 6, "replace", "b")


def test_replace_part_over_cap_is_dropped() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1)
    assert _display(gate, 1, 10, "replace", "a")
    assert _display(gate, 1, 11, "replace", "a") == [("process.truncated", {"cell_id": "c"})]
    assert _display(gate, 1, 10, "replace", "a")


def test_requeue_resets_size_and_truncated() -> None:
    gate = OutputGate(cap=10)
    _queued(gate, 1)
    assert _stream(gate, 1, 10)
    assert _stream(gate, 1, 1) == [("process.truncated", {"cell_id": "c"})]
    _queued(gate, 2)
    assert _stream(gate, 2, 10)
    assert _stream(gate, 2, 1) == [("process.truncated", {"cell_id": "c"})]


def test_forget_drops_later_output() -> None:
    gate = OutputGate()
    _queued(gate, 1)
    gate.forget("c")
    assert _stream(gate, 1, 1) == []

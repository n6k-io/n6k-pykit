from typing import Any

from n6k_app.kernel.state import KernelState


def _state(*notifications: tuple[str, dict[str, Any]]) -> KernelState:
    state = KernelState()
    state.apply("process.status", {"cell_id": "c", "state": "queued", "code": "pass", "thread": "main"})
    for method, params in notifications:
        state.apply(method, {"cell_id": "c", **params})
    return state


def _stream(target: str, data: str) -> tuple[str, dict[str, Any]]:
    return "process.stream", {"target": target, "data": data}


def _display(display_id: str, method: str, data: Any, **extra: str) -> tuple[str, dict[str, Any]]:
    return "process.display", {"displayId": display_id, "method": method, "mime": "m", "data": data, **extra}


def test_status_and_completion() -> None:
    state = _state()
    assert state.cells() == [{"cell_id": "c", "thread": "main", "state": "queued", "success": None, "truncated": False}]
    state.apply("process.status", {"cell_id": "c", "state": "running"})
    assert state.status("c") == "running"
    state.apply(
        "process.status",
        {"cell_id": "c", "state": "completed", "success": True, "interrupted": False, "added_vars": ["x"]},
    )
    assert state.cell("c") == {
        "cell_id": "c",
        "thread": "main",
        "code": "pass",
        "state": "completed",
        "success": True,
        "interrupted": False,
        "added_vars": ["x"],
        "outputs": [],
        "truncated": False,
    }


def test_streams_merge_per_target_and_interleave_with_displays() -> None:
    state = _state(
        _stream("stdout", "a"),
        _stream("stdout", "b"),
        _stream("stderr", "e"),
        _display("d1", "replace", "x"),
        _stream("stderr", "f"),
        _display("d1", "append", "y"),
        _stream("stderr", "g"),
    )
    cell = state.cell("c")
    assert cell is not None
    assert cell["outputs"] == [
        {"type": "stream", "target": "stdout", "data": "ab"},
        {"type": "stream", "target": "stderr", "data": "e"},
        {"type": "display", "displayId": "d1", "parts": [{"mime": "m", "data": "x"}, {"mime": "m", "data": "y"}]},
        {"type": "stream", "target": "stderr", "data": "fg"},
    ]


def test_display_replace_append_prepend() -> None:
    state = _state(
        _display("d", "append", {"a": 1}, title="t"),
        _display("d", "append", [2]),
        _display("d", "prepend", 0, icon="i"),
    )
    cell = state.cell("c")
    assert cell is not None
    assert cell["outputs"][0]["parts"] == [
        {"mime": "m", "data": 0},
        {"mime": "m", "data": {"a": 1}},
        {"mime": "m", "data": [2]},
    ]
    assert cell["outputs"][0]["title"] == "t" and cell["outputs"][0]["icon"] == "i"
    state.apply("process.display", {"cell_id": "c", **_display("d", "replace", "z")[1]})
    cell = state.cell("c")
    assert cell is not None
    assert cell["outputs"][0]["parts"] == [{"mime": "m", "data": "z"}]


def test_cell_is_a_copy() -> None:
    state = _state(_display("d", "replace", {"a": [1]}), _stream("stdout", "a"))
    cell = state.cell("c")
    assert cell is not None
    cell["outputs"][0]["parts"][0]["data"]["a"].append(2)
    state.apply("process.stream", {"cell_id": "c", "target": "stdout", "data": "b"})
    again = state.cell("c")
    assert again is not None
    assert again["outputs"][0]["parts"][0]["data"] == {"a": [1]}
    assert cell["outputs"][1]["data"] == "a"


def test_requeue_resets_and_forgotten_removes() -> None:
    state = _state(_stream("stdout", "a"), _display("d", "replace", "x"), ("process.truncated", {}))
    state.apply("process.status", {"cell_id": "c", "state": "queued", "code": "new", "thread": "t"})
    state.apply("process.display", {"cell_id": "c", **_display("d", "append", "y")[1]})
    cell = state.cell("c")
    assert cell is not None
    assert cell["code"] == "new" and cell["thread"] == "t" and cell["truncated"] is False
    assert cell["outputs"] == [{"type": "display", "displayId": "d", "parts": [{"mime": "m", "data": "y"}]}]
    state.apply("process.forgotten", {"cell_id": "c"})
    assert state.cell("c") is None and state.status("c") is None and state.cells() == []


def test_truncated_sets_flag() -> None:
    state = _state(("process.truncated", {}))
    assert state.cells()[0]["truncated"] is True

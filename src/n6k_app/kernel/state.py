"""Every cell's code, status and output, built only from broadcast notifications: the view
of a client connected since the kernel started."""

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class CellState:
    cell_id: str
    thread: str
    code: str
    state: str = "queued"
    success: Optional[bool] = None
    interrupted: Optional[bool] = None
    added_vars: list[str] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    truncated: bool = False


class KernelState:
    def __init__(self) -> None:
        self._cells: dict[str, CellState] = {}
        self._displays: dict[str, dict[str, dict[str, Any]]] = {}

    def apply(self, method: str, params: dict[str, Any]) -> None:
        cell_id = params["cell_id"]
        if method == "process.status" and params["state"] == "queued":
            self._cells[cell_id] = CellState(cell_id, params["thread"], params["code"])
            self._displays[cell_id] = {}
            return
        if method == "process.forgotten":
            del self._cells[cell_id]
            del self._displays[cell_id]
            return
        cell = self._cells[cell_id]
        if method == "process.status":
            cell.state = params["state"]
            if cell.state == "completed":
                cell.success = params["success"]
                cell.interrupted = params["interrupted"]
                cell.added_vars = params["added_vars"]
        elif method == "process.truncated":
            cell.truncated = True
        elif method == "process.stream":
            last = cell.outputs[-1] if cell.outputs else None
            if last is not None and last["type"] == "stream" and last["target"] == params["target"]:
                last["chunks"].append(params["data"])
            else:
                cell.outputs.append({"type": "stream", "target": params["target"], "chunks": [params["data"]]})
        elif method == "process.display":
            _apply_display(cell, self._displays[cell_id], params)

    def cells(self) -> list[dict[str, Any]]:
        return [
            {
                "cell_id": c.cell_id,
                "thread": c.thread,
                "state": c.state,
                "success": c.success,
                "truncated": c.truncated,
            }
            for c in self._cells.values()
        ]

    def cell(self, cell_id: str) -> Optional[dict[str, Any]]:
        """A JSON-ready copy, safe to serialize after the caller releases the kernel lock."""
        cell = self._cells.get(cell_id)
        if cell is None:
            return None
        out = asdict(cell)
        out["outputs"] = [
            {"type": "stream", "target": o["target"], "data": "".join(o["chunks"])} if o["type"] == "stream" else o
            for o in out["outputs"]
        ]
        return out

    def status(self, cell_id: str) -> Optional[str]:
        cell = self._cells.get(cell_id)
        return None if cell is None else cell.state


def _apply_display(cell: CellState, displays: dict[str, dict[str, Any]], params: dict[str, Any]) -> None:
    part = {"mime": params["mime"], "data": params["data"]}
    display_id = params["displayId"]
    entry = displays.get(display_id)
    if entry is None:
        entry = displays[display_id] = {"type": "display", "displayId": display_id, "parts": [part]}
        cell.outputs.append(entry)
    elif params["method"] == "append":
        entry["parts"].append(part)
    elif params["method"] == "prepend":
        entry["parts"].insert(0, part)
    else:
        entry["parts"] = [part]
    for key in ("title", "icon"):
        if key in params:
            entry[key] = params[key]

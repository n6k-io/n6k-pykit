"""Decides which cell notifications are broadcast: drops output from stale runs and any
stream or display that would take its cell past the cap. Later output that still fits is
sent; ``process.truncated`` is sent on the first drop of a run only.

A cell's size is its stream data plus each display's current data, so a replace frees
what it overwrites."""

from dataclasses import dataclass, field
from typing import Any, Optional

CELL_CAP = 10 * 2**20

Notification = tuple[str, dict[str, Any]]


@dataclass
class _Cell:
    run: int
    size: int = 0
    truncated: bool = False
    displays: dict[str, int] = field(default_factory=dict)


class OutputGate:
    def __init__(self, cap: int = CELL_CAP) -> None:
        self.cap = cap
        self._cells: dict[str, _Cell] = {}

    def check(self, method: str, params: dict[str, Any], run: Optional[int], size: int = 0) -> list[Notification]:
        """The notifications to broadcast for one emit; ``size`` is the JSON length of its output data."""
        cell_id = params["cell_id"]
        if method == "process.status" and params["state"] == "queued":
            assert run is not None
            self._cells[cell_id] = _Cell(run)
            return [(method, params)]
        cell = self._cells.get(cell_id)
        if cell is None or cell.run != run:
            return []
        if method == "process.stream":
            if cell.size + size > self.cap:
                return self._drop(cell_id, cell)
            cell.size += size
        elif method == "process.display":
            display_id = params["displayId"]
            old = cell.displays.get(display_id, 0)
            new = old + size if display_id in cell.displays and params["method"] in ("append", "prepend") else size
            if cell.size - old + new > self.cap:
                return self._drop(cell_id, cell)
            cell.size += new - old
            cell.displays[display_id] = new
        return [(method, params)]

    def forget(self, cell_id: str) -> None:
        self._cells.pop(cell_id, None)

    @staticmethod
    def _drop(cell_id: str, cell: _Cell) -> list[Notification]:
        if cell.truncated:
            return []
        cell.truncated = True
        return [("process.truncated", {"cell_id": cell_id})]

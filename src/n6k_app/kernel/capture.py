"""Cell output: ``sys.stdout``/``sys.stderr`` stand-ins and ``display()``.

A worker sets ``CELL.cell_id``, ``CELL.run`` and ``CELL.emit`` on its own thread before
running a cell; output from a thread without them (e.g. one user code spawned) goes to
the real stream."""

import threading
from io import StringIO
from itertools import count
from pprint import pformat
from typing import Any, Callable, Optional, TextIO

Emit = Callable[[str, dict[str, Any]], None]

CELL = threading.local()

_display_ids = count(1)


def _current() -> Optional[tuple[str, Emit]]:
    cell_id = getattr(CELL, "cell_id", None)
    emit = getattr(CELL, "emit", None)
    if cell_id is None or emit is None:
        return None
    return cell_id, emit


class Capture:
    def __init__(self, target: str, fallback: TextIO) -> None:
        self.target = target
        self.fallback = fallback

    def write(self, data: str) -> int:
        cell = _current()
        if cell is None:
            return self.fallback.write(data)
        cell_id, emit = cell
        emit("process.stream", {"cell_id": cell_id, "target": self.target, "data": data})
        return len(data)

    def flush(self) -> None:
        pass


def display(
    obj: Any,
    displayId: Optional[str] = None,
    method: str = "replace",
    mime: Optional[str] = None,
    title: Optional[str] = None,
    icon: Optional[str] = None,
) -> Optional[str]:
    """
    The output is anchored by ``displayId``, scoped to the current cell. The first call with a given id
    (or with no id) creates the output; later calls with the same id update it
    according to ``method``.

    :arg displayId: anchor for this output. When ``None`` (the default) a fresh
               id is generated and the object is shown as a new output. Pass an
               id returned by a previous call to update that output in place.
    :arg method: how to apply ``obj`` to an existing output: ``replace`` (the
               default) overwrites it, ``append`` adds after its content,
               ``prepend`` adds before it. Ignored when the id does not yet
               exist (the output is simply created).
    :arg mime: explicit mime-type for ``obj``; no detection is done. When
               ``None`` (the default), the mime-type is auto-detected from the
               object.
    :returns: the output id (``str``), or ``None`` when ``obj`` is ``None``.
    :raises RuntimeError: outside a running cell.
    """
    if obj is None:
        return None

    cell = _current()
    if cell is None:
        raise RuntimeError("display() called outside a running cell")
    cell_id, emit = cell

    if displayId is None:
        displayId = f"d{next(_display_ids)}"

    if mime is not None:
        data = obj
    else:
        typeinfo = type(obj).__module__, type(obj).__name__
        if typeinfo == ("matplotlib.figure", "Figure"):
            stream = StringIO()
            obj.savefig(stream, format="svg")
            data, mime = stream.getvalue(), "image/svg+xml"
        elif hasattr(obj, "_repr_html_"):
            data, mime = obj._repr_html_(), "text/html"
        else:
            data, mime = pformat(obj), "text/plain"

    params: dict[str, Any] = {
        "cell_id": cell_id,
        "displayId": displayId,
        "method": method,
        "mime": mime,
        "data": data,
    }
    if title is not None:
        params["title"] = title
    if icon is not None:
        params["icon"] = icon
    emit("process.display", params)
    return displayId

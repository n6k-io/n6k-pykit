"""Per-connection session state for RPCs.

A ``Session`` is an attribute bag scoped to one client connection, created when
the session opens and handed to every RPC call on it; values persist across calls
on that connection and are isolated from others. An RPC opts in by annotating a
parameter ``Session``, injected by type like FastAPI's ``Request`` (other params
bind positionally, so ``session`` may sit anywhere)::

    @db.rpc()
    def set_interval(ms, session: Session) -> Table[Cfg]:
        session.interval_ms = int(ms)          # this connection only
        return pd.DataFrame([{"interval_ms": session.interval_ms}])

It also carries read-only context: ``session.catalog`` (the client's ATTACH
alias) and ``session.connection`` (the live DuckDB connection). Open a cursor
(``session.connection.cursor()``) for RPC queries so work stays isolated from
concurrent session ops.
"""

from typing import Any

import duckdb


class Session:
    """Per-connection state by attribute, plus read-only framework context
    (``catalog``, ``connection``). An unset name raises ``AttributeError``."""

    __slots__ = ("catalog", "connection", "_data")

    _CONTEXT = ("catalog", "connection", "_data")

    def __init__(self, *, catalog: str | None = None, connection: "duckdb.DuckDBPyConnection | None" = None) -> None:
        # Bypass our own __setattr__ (which forbids these names) to seed context.
        object.__setattr__(self, "catalog", catalog)
        object.__setattr__(self, "connection", connection)
        object.__setattr__(self, "_data", {})

    def __getattr__(self, name: str) -> Any:
        # Only reached when slot lookup fails, so the slots never route here — no recursion.
        try:
            return object.__getattribute__(self, "_data")[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name: str, value: Any) -> None:
        if name in Session._CONTEXT:
            raise AttributeError(f"'{name}' is read-only session context and can't be reassigned")
        object.__getattribute__(self, "_data")[name] = value

    def __delattr__(self, name: str) -> None:
        if name in Session._CONTEXT:
            raise AttributeError(f"'{name}' is read-only session context and can't be deleted")
        try:
            del object.__getattribute__(self, "_data")[name]
        except KeyError:
            raise AttributeError(name) from None

    def get(self, name: str, default: Any = None) -> Any:
        return object.__getattribute__(self, "_data").get(name, default)

    def __contains__(self, name: str) -> bool:
        return name in object.__getattribute__(self, "_data")

    def __repr__(self) -> str:
        cat = object.__getattribute__(self, "catalog")
        return f"Session(catalog={cat!r}, {object.__getattribute__(self, '_data')!r})"

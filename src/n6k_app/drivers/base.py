"""Upstream driver registry.

A ``Driver`` encapsulates everything specific to one upstream kind (Postgres,
MySQL, …): how it ATTACHes into the pool DB, whether it can hand a ``@rpc`` a
real driver-native connection, and which annotation names that connection type.
Drivers are registered in ``__init__``, so ``ConnectionPool`` and the
RPC binding stay generic — adding an upstream is a new module, not an edit to a
dozen ``if db_type == …`` sites.

``ConnectionSpec`` is imported only for typing (the runtime cycle would be
connections → drivers → connections); ``attach``/``dsn`` just read its attributes.
"""

from __future__ import annotations

import logging
import socket
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import duckdb

from n6k_app.db_log import execute_and_log

if TYPE_CHECKING:
    from n6k_app.connections import ConnectionSpec

_log = logging.getLogger(__name__)


@runtime_checkable
class NativePool(Protocol):
    """A pool of real, driver-native connections (e.g. psycopg) that an ``@rpc``
    checks one out of per call. ``checkout`` lends a raw connection for the call and
    returns it to the pool afterward — it does NOT manage the transaction; the rpc
    owns commit/rollback. Returning resets the connection so state can't leak into
    the next checkout."""

    def checkout(self) -> Any:  # a context manager yielding a live connection
        ...

    def close(self) -> None: ...


class Driver:
    """Base upstream driver. A concrete driver sets ``db_type``/``extension``/
    ``default_port`` and implements ``dsn``; it overrides ``open_native_pool`` and
    ``claims_annotation`` only if it supports real ``@rpc`` connections."""

    db_type: str = ""
    extension: str = ""  # DuckDB extension name (INSTALL/LOAD, ATTACH … TYPE <extension>)
    default_port: int = 0

    def dsn(self, spec: ConnectionSpec) -> str:
        """The ATTACH DSN string for this upstream."""
        raise NotImplementedError

    def attach(self, pool_db: duckdb.DuckDBPyConnection, spec: ConnectionSpec, pool_catalog: str) -> None:
        """Probes reachability first: a clear error beats a slow DuckDB hang."""
        check_reachable(spec.host, spec.port)
        execute_and_log("pool", pool_db, f"INSTALL {self.extension}")
        execute_and_log("pool", pool_db, f"LOAD {self.extension}")
        for name, value in spec.settings.items():
            execute_and_log("pool", pool_db, f"SET {name} = {value}")
        execute_and_log("pool", pool_db, f"ATTACH '{self.dsn(spec)}' AS \"{pool_catalog}\" (TYPE {self.extension})")
        _log.info(
            "Connected to %s://%s:%d/%s — %d granted table(s)",
            self.db_type,
            spec.host,
            spec.port,
            spec.database,
            _granted_count(spec),
        )

    def open_native_pool(self, spec: ConnectionSpec) -> NativePool | None:
        """Open a native-connection pool for this upstream, or return None if the
        driver has no native support / its client library isn't installed."""
        return None

    def claims_annotation(self, annotation: object) -> bool:
        """True if ``annotation`` is this driver's native ``Connection`` type — the
        signal that an ``@rpc`` param wants a real connection injected."""
        return False


_REGISTRY: dict[str, Driver] = {}


def register_driver(driver: Driver) -> None:
    _REGISTRY[driver.db_type] = driver


def get_driver(db_type: str) -> Driver:
    try:
        return _REGISTRY[db_type]
    except KeyError:
        raise ValueError(f"Unsupported connection type: {db_type}") from None


def driver_for_annotation(annotation: object) -> Driver | None:
    """The driver whose native ``Connection`` type ``annotation`` is, or None."""
    for driver in _REGISTRY.values():
        if driver.claims_annotation(annotation):
            return driver
    return None


def _granted_count(spec: ConnectionSpec) -> int:
    return len({policy.table for policy in spec.policies if policy.setting != "primary_key"})


def check_reachable(host: str, port: int, timeout: float = 2.0) -> None:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as e:
        raise ConnectionError(f"Cannot reach {host}:{port} — {e}") from e

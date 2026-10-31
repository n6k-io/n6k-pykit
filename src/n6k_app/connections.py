"""Connection pool bridging external databases into DuckDB sessions.

One "pool DB" holds the external ATTACHments (MySQL/Postgres); each session db
bridges to it, so sessions query external tables without external access.
"""

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator

import duckdb
from virtual_catalog.bridge import Policy, bridge

from n6k_app.drivers.base import NativePool, get_driver

_log = logging.getLogger("n6k_app.connections")


@dataclass
class ConnectionSpec:
    name: str
    db_type: str  # "postgres" | "mysql"
    host: str
    port: int
    user: str
    password: str
    database: str
    # One `bridge_set_policy` call each, over ``schema.table`` names on the upstream.
    # Schema is preserved through the bridge, so a table surfaces as
    # ``<catalog>.<schema>.<table>``.
    policies: list[Policy] = field(default_factory=list)
    settings: dict[str, str] = field(default_factory=dict)


class ConnectionPool:
    def __init__(self) -> None:
        self._pool_db: duckdb.DuckDBPyConnection | None = None
        self._specs: dict[str, ConnectionSpec] = {}
        self._lock = threading.Lock()
        # Recorded (not raised) on upstream-open failure, so one broken upstream
        # never crashes boot; Database.attach surfaces it per session.
        self._start_error: BaseException | None = None
        # Absent when the driver has no native `@rpc` Connection support or its
        # client library isn't installed.
        self._native_pools: dict[str, NativePool] = {}

    @property
    def has_connections(self) -> bool:
        return len(self._specs) > 0

    def register(self, spec: ConnectionSpec) -> None:
        if spec.name in self._specs:
            raise ValueError(f"Duplicate connection name '{spec.name}'")
        self._specs[spec.name] = spec

    def ensure_started(self) -> None:
        """Open the shared upstream ATTACHment(s) exactly once (idempotent,
        thread-safe). A failure is recorded on ``_start_error``, not raised, and
        not retried; ``Database.attach`` surfaces it per session."""
        if self._pool_db is not None or self._start_error is not None or not self._specs:
            return
        with self._lock:
            if self._pool_db is not None or self._start_error is not None:
                return
            try:
                self._open()
            except Exception as exc:
                if self._pool_db is not None:
                    try:
                        self._pool_db.close()
                    except duckdb.Error:
                        pass
                    self._pool_db = None
                self._start_error = exc
                _log.exception("Connection pool could not open its upstream; sessions will surface this.")

    def _open(self) -> None:
        """Open the pool DB and ATTACH every registered upstream; raises on failure."""
        self._pool_db = duckdb.connect(config={"allow_unsigned_extensions": "true"})

        for spec in self._specs.values():
            self._attach_connection(spec)

    def _attach_connection(self, spec: ConnectionSpec) -> None:
        assert self._pool_db is not None
        driver = get_driver(spec.db_type)
        driver.attach(self._pool_db, spec, pool_catalog=f"_pool_{spec.name}")
        native = driver.open_native_pool(spec)
        if native is not None:
            self._native_pools[spec.name] = native

    def sole_spec(self, db_type: str) -> ConnectionSpec:
        """The sole upstream of *db_type* on this pool — the one an `@rpc`'s native
        Connection parameter binds to. Raises if there is none (e.g. a bare Database,
        or a pool without that upstream) or if it's ambiguous."""
        matches = [s for s in self._specs.values() if s.db_type == db_type]
        if not matches:
            raise RuntimeError(
                f"This RPC declares a {db_type} Connection parameter, but its database has no "
                f"{db_type} upstream. Use the matching upstream Database so a real connection exists."
            )
        if len(matches) > 1:
            raise RuntimeError(f"Ambiguous {db_type} Connection: this database has multiple {db_type} upstreams.")
        return matches[0]

    @contextmanager
    def native_checkout(self, db_type: str) -> Iterator[Any]:
        """Lend a driver-native connection for one rpc call. The rpc owns its
        transactions; the pool resets the connection on return, so uncommitted
        changes and per-request state don't leak into the next checkout."""
        self.ensure_started()
        spec = self.sole_spec(db_type)
        pool = self._native_pools.get(spec.name)
        if pool is None:
            raise RuntimeError(
                f"This RPC needs a real {db_type} connection, but that driver's client library "
                f"isn't installed. Install the matching extra (e.g. n6k[postgres])."
            )
        with pool.checkout() as conn:
            yield conn

    def setup_bridge(self, session_conn: duckdb.DuckDBPyConnection, target_catalog: str = "memory") -> None:
        """Create ``target_catalog`` on ``session_conn`` as this pool's bridge.

        One upstream is one bridge is one catalog, so the sole spec's grants land
        under their own source schemas: ``public.users`` reaches the page as
        ``<target_catalog>.public.users``.
        """
        if not self._specs or self._pool_db is None:
            return

        if len(self._specs) > 1:
            raise RuntimeError(
                f"catalog {target_catalog!r} has {len(self._specs)} upstreams; one bridge catalog serves one upstream"
            )
        spec = next(iter(self._specs.values()))
        bridge(
            self._pool_db,
            session_conn,
            target_catalog,
            source_catalog=f"_pool_{spec.name}",
            policies=spec.policies,
            lock=self._lock,
        )
        _log.debug("setup_bridge: conn %s done", spec.name)

    def close(self) -> None:
        if self._pool_db is not None:
            try:
                self._pool_db.close()
            except duckdb.Error:
                pass
            self._pool_db = None
        for pool in self._native_pools.values():
            pool.close()
        self._native_pools.clear()

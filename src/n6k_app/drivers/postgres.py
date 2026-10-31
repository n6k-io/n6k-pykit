"""Postgres upstream driver.

ATTACH needs no Python client. psycopg is an optional dep for real ``@rpc``
``Connection`` support: imported lazily, its absence means no native connection.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

from n6k_app.drivers.base import Driver, NativePool

if TYPE_CHECKING:
    from n6k_app.connections import ConnectionSpec

_log = logging.getLogger(__name__)


class _PsycopgNativePool:
    """Adapts a ``psycopg_pool.ConnectionPool`` to the ``NativePool`` protocol.

    Lends the rpc a raw connection via ``getconn``/``putconn`` — deliberately NOT
    ``pool.connection()``, which would commit/rollback on the author's behalf. The
    rpc owns its transactions (``commit``/``rollback``/autocommit as it likes). On
    return ``putconn`` resets the connection (rolling back anything left open), so
    an uncommitted change simply doesn't persist and never leaks to the next call."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @contextmanager
    def checkout(self) -> Iterator[Any]:
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.close()


class PostgresDriver(Driver):
    db_type = "postgres"
    extension = "postgres"
    default_port = 5432

    def dsn(self, spec: ConnectionSpec) -> str:
        dsn = f"host={spec.host} user={spec.user} port={spec.port} dbname={spec.database}"
        if spec.password:
            dsn += f" password={spec.password}"
        return dsn

    def open_native_pool(self, spec: ConnectionSpec) -> NativePool | None:
        """``settings`` are DuckDB SETs, not libpq params, so they're not forwarded."""
        try:
            from psycopg.conninfo import make_conninfo
            from psycopg_pool import ConnectionPool as PgPool
        except ModuleNotFoundError:
            _log.debug("psycopg not installed; real Connection rpcs unavailable for %r", spec.name)
            return None

        conninfo = make_conninfo(
            host=spec.host,
            port=spec.port,
            user=spec.user,
            password=spec.password or None,
            dbname=spec.database,
        )
        pool = PgPool(conninfo, min_size=0, max_size=4, open=False, name=f"pg_{spec.name}")
        pool.open()
        return _PsycopgNativePool(pool)

    def claims_annotation(self, annotation: object) -> bool:
        try:
            import psycopg
        except ModuleNotFoundError:
            return False
        return isinstance(annotation, type) and issubclass(annotation, psycopg.Connection)

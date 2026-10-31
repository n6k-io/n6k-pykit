"""MySQL upstream driver — DuckDB ATTACH only; no native ``@rpc`` ``Connection``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from n6k_app.drivers.base import Driver

if TYPE_CHECKING:
    from n6k_app.connections import ConnectionSpec


class MysqlDriver(Driver):
    db_type = "mysql"
    extension = "mysql"
    default_port = 3306

    def dsn(self, spec: ConnectionSpec) -> str:
        dsn = f"host={spec.host} user={spec.user} port={spec.port} database={spec.database}"
        if spec.password:
            dsn += f" password={spec.password}"
        return dsn

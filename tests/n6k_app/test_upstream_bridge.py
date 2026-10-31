"""Upstream databases served through a bridge, against a live Postgres and MySQL.

Each test runs once per upstream and is skipped where that upstream is unreachable.
``exclude_<kind>`` skips a test that does not apply to that upstream;
``xfail_<kind>`` marks one that should work there and does not yet.
"""

import os
import uuid
from dataclasses import dataclass
from typing import Any, Iterator

import duckdb
import pytest

from n6k_app.database import Database, MysqlDatabase, PostgresDatabase, _UpstreamDatabase, policy, primary_key
from n6k_app.database import open_serving_conn, serving_authenticator, serving_sessions
from n6k_app.connections import ConnectionSpec
from n6k_app.drivers.base import check_reachable, get_driver
from n6k_server.pump import WsReject

ALIAS = "up"
MYSQL_JOIN_WRITE = "the mysql extension refuses an UPDATE/DELETE that joins, as the bridge's filtered writes do"


@dataclass
class Upstream:
    kind: str
    schema: str
    db_cls: type[_UpstreamDatabase]
    conn: dict[str, Any]
    admin: duckdb.DuckDBPyConnection
    table: str
    keyless: str

    def execute(self, sql: str) -> None:
        self.admin.execute(f"CALL {self.kind}_execute('admin', ?)", [sql])

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        return self.admin.execute(f"SELECT * FROM {self.kind}_query('admin', ?)", [sql]).fetchall()

    def q(self, name: str) -> str:
        return f"{self.schema}.{name}"

    def served(self, name: str) -> str:
        return f"{ALIAS}.{self.schema}.{name}"

    def database(self, policies: list[Any], **kw: Any) -> _UpstreamDatabase:
        return self.db_cls(policies=policies, **{**self.conn, **kw})


_PG: dict[str, Any] = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", os.getenv("USER", "postgres")),
    "password": os.getenv("PGPASSWORD", ""),
    "database": os.getenv("PGDATABASE", "n6k_test"),
}
_MY: dict[str, Any] = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "n6k_test"),
}


def _admin(kind: str, conn: dict[str, Any]) -> duckdb.DuckDBPyConnection:
    driver = get_driver(kind)
    admin = duckdb.connect()
    admin.execute(f"INSTALL {driver.extension}; LOAD {driver.extension}")
    dsn = driver.dsn(ConnectionSpec(name="admin", db_type=kind, **conn))
    admin.execute(f"ATTACH '{dsn}' AS admin (TYPE {driver.extension})")
    return admin


@pytest.fixture(params=["postgres", "mysql"])
def upstream(request: pytest.FixtureRequest) -> Iterator[Upstream]:
    kind = request.param
    if marker := request.node.get_closest_marker(f"exclude_{kind}"):
        pytest.skip(marker.kwargs.get("reason", f"excluded on {kind}"))
    if marker := request.node.get_closest_marker(f"xfail_{kind}"):
        request.applymarker(pytest.mark.xfail(strict=True, **marker.kwargs))

    conn, schema, db_cls = (
        (_PG, "public", PostgresDatabase) if kind == "postgres" else (_MY, _MY["database"], MysqlDatabase)
    )
    try:
        check_reachable(conn["host"], conn["port"])
    except Exception as exc:
        pytest.skip(f"{kind} unreachable: {exc}")

    suffix = uuid.uuid4().hex[:8]
    up = Upstream(kind, schema, db_cls, conn, _admin(kind, conn), f"n6k_t_{suffix}", f"n6k_nk_{suffix}")
    up.execute(f"CREATE TABLE {up.table} (id INT PRIMARY KEY, tenant INT, name VARCHAR(16))")
    up.execute(f"CREATE TABLE {up.keyless} (id INT, tenant INT, name VARCHAR(16))")
    for name in (up.table, up.keyless):
        up.execute(f"INSERT INTO {name} VALUES (1, 1, 'a'), (2, 2, 'b')")
    try:
        yield up
    finally:
        for name in (up.table, up.keyless):
            up.execute(f"DROP TABLE IF EXISTS {name}")
        up.admin.close()


@pytest.fixture
async def serve() -> Any:
    opened: list[tuple[duckdb.DuckDBPyConnection, _UpstreamDatabase]] = []

    async def _serve(db: _UpstreamDatabase) -> duckdb.DuckDBPyConnection:
        dbs: dict[str, Database] = {ALIAS: db}
        con = await open_serving_conn(dbs)
        opened.append((con, db))
        await serving_authenticator(dbs, serving_sessions(dbs, con))(None, con, "")
        return con

    yield _serve
    for con, db in opened:
        con.close()
        db.connection_pool.close()


async def test_select_predicate_reaches_served_table(upstream: Upstream, serve: Any) -> None:
    con = await serve(upstream.database([policy(upstream.q(upstream.table), "select", "tenant = 1")]))
    assert con.sql(f"SELECT id FROM {upstream.served(upstream.table)}").fetchall() == [(1,)]


async def test_ungranted_table_not_served(upstream: Upstream, serve: Any) -> None:
    con = await serve(upstream.database([policy(upstream.q(upstream.table), "select", "true")]))
    with pytest.raises(duckdb.CatalogException):
        con.sql(f"SELECT * FROM {upstream.served(upstream.keyless)}").fetchall()


async def test_insert_check_reaches_upstream(upstream: Upstream, serve: Any) -> None:
    t = upstream.q(upstream.table)
    con = await serve(upstream.database([policy(t, "select", "true"), policy(t, "insert", "true", check="tenant = 1")]))
    con.execute(f"INSERT INTO {upstream.served(upstream.table)} VALUES (3, 1, 'c')")
    with pytest.raises(duckdb.InvalidInputException, match="check predicate"):
        con.execute(f"INSERT INTO {upstream.served(upstream.table)} VALUES (4, 2, 'd')")
    assert upstream.rows(f"SELECT id FROM {upstream.table} ORDER BY id") == [(1,), (2,), (3,)]


@pytest.mark.xfail_mysql(reason=MYSQL_JOIN_WRITE)
async def test_update_using_and_check_reach_upstream(upstream: Upstream, serve: Any) -> None:
    t = upstream.q(upstream.table)
    con = await serve(
        upstream.database([policy(t, "select", "true"), policy(t, "update", "tenant = 1", check="tenant = 1")])
    )
    con.execute(f"UPDATE {upstream.served(upstream.table)} SET name = 'A'")
    with pytest.raises(duckdb.InvalidInputException, match="check predicate"):
        con.execute(f"UPDATE {upstream.served(upstream.table)} SET tenant = 2 WHERE id = 1")
    assert upstream.rows(f"SELECT id, tenant, name FROM {upstream.table} ORDER BY id") == [(1, 1, "A"), (2, 2, "b")]


@pytest.mark.xfail_mysql(reason=MYSQL_JOIN_WRITE)
async def test_delete_reaches_upstream(upstream: Upstream, serve: Any) -> None:
    t = upstream.q(upstream.table)
    con = await serve(upstream.database([policy(t, "select", "true"), policy(t, "delete", "tenant = 1")]))
    con.execute(f"DELETE FROM {upstream.served(upstream.table)} WHERE id IN (1, 2)")
    assert upstream.rows(f"SELECT id FROM {upstream.table} ORDER BY id") == [(2,)]


@pytest.mark.xfail_mysql(reason=MYSQL_JOIN_WRITE)
async def test_primary_keys_address_keyless_table(upstream: Upstream, serve: Any) -> None:
    nk = upstream.q(upstream.keyless)
    con = await serve(
        upstream.database(
            [policy(nk, "select", "true"), policy(nk, "update", "true")],
            primary_keys=[primary_key(nk, "id")],
        )
    )
    con.execute(f"UPDATE {upstream.served(upstream.keyless)} SET name = 'A' WHERE id = 1")
    assert upstream.rows(f"SELECT name FROM {upstream.keyless} ORDER BY id") == [("A",), ("b",)]


async def test_keyless_table_without_primary_keys_refuses_update(upstream: Upstream, serve: Any) -> None:
    nk = upstream.q(upstream.keyless)
    con = await serve(upstream.database([policy(nk, "select", "true"), policy(nk, "update", "true")]))
    with pytest.raises(duckdb.BinderException, match="has no key"):
        con.execute(f"UPDATE {upstream.served(upstream.keyless)} SET name = 'A' WHERE id = 1")


async def test_view_over_granted_table(upstream: Upstream, serve: Any) -> None:
    db = upstream.database([policy(upstream.q(upstream.table), "select", "true")])

    @db.view()
    def tenants() -> str:
        return f"SELECT DISTINCT tenant FROM {upstream.q(upstream.table)} ORDER BY tenant"

    con = await serve(db)
    assert con.sql(f"SELECT * FROM {ALIAS}.main.tenants").fetchall() == [(1,), (2,)]


async def test_bridged_conn_is_locked_down(upstream: Upstream, serve: Any) -> None:
    con = await serve(upstream.database([policy(upstream.q(upstream.table), "select", "true")]))
    assert con.sql(
        "SELECT current_setting('enable_external_access'), current_setting('lock_configuration')"
    ).fetchone() == (False, True)
    with pytest.raises(duckdb.PermissionException):
        con.sql("SELECT * FROM read_csv('/etc/hosts')").fetchall()
    with pytest.raises(duckdb.InvalidInputException, match="locked"):
        con.execute("SET threads = 1")


async def test_missing_upstream_database_rejects_session(upstream: Upstream, serve: Any) -> None:
    db = upstream.database([policy(upstream.q(upstream.table), "select", "true")], database="n6k_no_such_db")
    with pytest.raises(WsReject) as rejected:
        await serve(db)
    assert rejected.value.code == 1011

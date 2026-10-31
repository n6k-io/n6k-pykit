"""Server-side shaping of upstream grants into ``bridge_set_policy`` calls, and the
connection-wide switch ``finalize_serving_conn`` throws. No upstream is dialed."""

import duckdb
import pytest

from n6k_app.connections import ConnectionPool, ConnectionSpec
from n6k_app.database import MysqlDatabase, PostgresDatabase, policy, primary_key
from n6k_app.session import finalize_serving_conn
from n6k_server.bridge import Policy


def test_policy_without_check_passes_predicate() -> None:
    assert policy("public.t", "select", "tenant = 1") == Policy("public.t", "select", "tenant = 1")


def test_policy_insert_check_is_check_only() -> None:
    assert policy("public.t", "insert", "true", check="tenant = 1") == Policy(
        "public.t", "insert", {"check": "tenant = 1"}
    )


def test_policy_update_check_carries_using_and_check() -> None:
    assert policy("public.t", "update", "tenant = 1", check="tenant = 2") == Policy(
        "public.t", "update", {"using": "tenant = 1", "check": "tenant = 2"}
    )


def test_policy_rejects_unqualified_table() -> None:
    with pytest.raises(ValueError, match="fully qualified"):
        policy("t", "select", "true")


@pytest.mark.parametrize("cls", [PostgresDatabase, MysqlDatabase])
def test_primary_keys_become_primary_key_policies(cls: type) -> None:
    grant = policy("s.v", "update", "true")
    db = cls(database="d", policies=[grant], primary_keys=[primary_key("s.v", "id", "tenant")])
    (spec,) = db.connection_pool._specs.values()
    assert spec.policies == [grant, Policy("s.v", "primary_key", ["id", "tenant"])]


def test_empty_policies_rejected() -> None:
    with pytest.raises(ValueError, match="policies cannot be empty"):
        PostgresDatabase(database="d", policies=[])


def test_primary_key_needs_columns() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        primary_key("s.v")


def test_two_upstreams_on_one_catalog_rejected() -> None:
    pool = ConnectionPool()
    for name in ("a", "b"):
        pool.register(ConnectionSpec(name, "postgres", "localhost", 5432, "u", "", name))
    pool._pool_db = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="one bridge catalog serves one upstream"):
            pool.setup_bridge(duckdb.connect(), target_catalog="app")
    finally:
        pool.close()


def test_unbridged_conn_disables_external_access_without_locking() -> None:
    con = duckdb.connect()
    finalize_serving_conn(con, bridged=False)
    assert con.sql(
        "SELECT current_setting('enable_external_access'), current_setting('lock_configuration')"
    ).fetchone() == (False, False)

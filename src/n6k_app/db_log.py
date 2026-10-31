import logging

import duckdb

_log = logging.getLogger("n6k_app.db")


def execute_and_log(
    label: str, conn: duckdb.DuckDBPyConnection, sql: str, params: object = None
) -> duckdb.DuckDBPyConnection:
    _log.debug("[%s] %s", label, sql)
    if params is not None:
        return conn.execute(sql, params)
    return conn.execute(sql)

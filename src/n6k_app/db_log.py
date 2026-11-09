import logging

import duckdb

_log = logging.getLogger("n6k_app.db")


def execute_and_log(label: str, conn: duckdb.DuckDBPyConnection, sql: str, params: object = None) -> None:
    _log.debug("[%s] %s", label, sql)
    if params is not None:
        conn.execute(sql, params)
    else:
        conn.execute(sql)
    conn.complete()

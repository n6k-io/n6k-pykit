"""Provisioning of the DuckDB connection a served n6k session runs on.

One connection carries every catalog a socket serves. A plain database's catalog
is a ``virtual_catalog_provider`` ATTACHment with one ``RegistryProvider`` per
schema; an upstream database's is a ``virtual_catalog_bridge`` catalog created by
``bridge()``. Both carry their ``view_sql`` views as real DuckDB views. The connection is then
handed to ``n6k_server.pump.serve_and_close_connection``, which owns it for the rest of
its life — the C++ reactor serves the wire from it and closes it at teardown.
"""

import asyncio
import logging

import duckdb

from n6k_server.extension import load_virtual_catalog_provider, load_n6k_server
from n6k_server.provider import register_provider

from n6k_app.connections import ConnectionPool
from n6k_app.db_log import execute_and_log
from n6k_app.registry_provider import RegistryProvider
from n6k_app.table_registry import TableRegistry

log = logging.getLogger(__name__)

_LABEL = "session"


def create_serving_conn() -> duckdb.DuckDBPyConnection:
    """A fresh connection with both extensions loaded and nothing attached yet.

    ``virtual_catalog_provider`` backs the provider catalogs; ``n6k_server`` is the
    reactor that will serve this connection. Both must load before
    ``finalize_serving_conn`` disables external access. An upstream database adds
    ``virtual_catalog_bridge`` on top, from its own catalog's ``bridge()``.

    Blocking (extension install/load), so call it inside ``asyncio.to_thread``.
    """
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_virtual_catalog_provider(con)
    load_n6k_server(con)
    return con


def attach_catalog(
    con: duckdb.DuckDBPyConnection,
    registry: TableRegistry,
    connection_pool: ConnectionPool,
    *,
    catalog: str,
    main_loop: asyncio.AbstractEventLoop,
) -> None:
    """Attach one database to ``con`` as ``catalog``, ready to be served.

    The catalog is made default via ``USE`` so view bodies resolve their
    unqualified table names against it.

    Does blocking DuckDB/extension/external-DB work, so run it inside
    ``asyncio.to_thread``; since that worker thread has no running loop, the
    caller must pass ``main_loop`` so the registered providers bind their
    coroutines to it.
    """
    # Bridging happens before external access is disabled — extension
    # install/load needs network/file access.
    if connection_pool.has_connections:
        connection_pool.setup_bridge(con, target_catalog=catalog)
        execute_and_log(_LABEL, con, f'USE "{catalog}"')
    else:
        execute_and_log(_LABEL, con, f"ATTACH ':memory:' AS \"{catalog}\" (TYPE virtual_catalog_provider)")
        execute_and_log(_LABEL, con, f'USE "{catalog}"')
        for schema in sorted(registry.schemas or {"main"}):
            execute_and_log(_LABEL, con, f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{schema}"')
            register_provider(
                con,
                catalog=catalog,
                schema_name=schema,
                provider=RegistryProvider(registry, schema),
                main_loop=main_loop,
            )

    for name, view_spec in registry.view_specs().items():
        execute_and_log(
            _LABEL,
            con,
            f'CREATE VIEW "{catalog}"."{view_spec.schema}"."{name}" AS {view_spec.sql}',
        )


def finalize_serving_conn(con: duckdb.DuckDBPyConnection, *, bridged: bool) -> None:
    """Close the door once every catalog is attached.

    Separate from ``attach_catalog`` because it is a connection-wide switch and
    the next catalog's bridge would need the access it takes away. A bridge target
    has ``virtual_catalog_bridge`` loaded, so it gets ``virtual_catalog_lockdown``.
    """
    if bridged:
        execute_and_log(
            _LABEL,
            con,
            "SELECT virtual_catalog_lockdown(current_setting('memory_limit'), current_setting('threads'))",
        )
    else:
        execute_and_log(_LABEL, con, "SET enable_external_access = false")

"""Serve a set of databases and actions on one connection, whatever carries it."""

import logging
from typing import TYPE_CHECKING

from n6k_server.pump import WebSocketLike, serve_and_close_connection

from n6k_app.actions import Action
from n6k_app.database import open_serving_conn, serving_authenticator, serving_sessions
from n6k_app.text_app import Render, Show, build_text_app

if TYPE_CHECKING:
    from n6k_app.database import Database

_log = logging.getLogger(__name__)


async def serve_databases(
    transport: WebSocketLike,
    name: str,
    databases: dict[str, "Database"],
    actions: list[Action],
    render: Render | None = None,
    show: Show | None = None,
) -> None:
    """Serve until the peer leaves: binary frames reach ``databases`` by the alias a
    client names in its ``FT_OPEN``; text frames are JSON-RPC/MCP for ``actions``
    (and ``render``, ``show_page``). A setup failure closes ``transport`` with 1011."""
    try:
        con = await open_serving_conn(databases)
    except Exception as exc:  # a failed upstream must reach the client, not hang it
        _log.exception("could not open the serving connection for %s", name)
        await transport.close(code=1011, reason=str(exc)[:120])
        return

    sessions = serving_sessions(databases, con)
    try:
        text_app = build_text_app(name, databases, actions, sessions, render, show)
    except Exception as exc:
        _log.exception("could not build the text app for %s", name)
        con.close()
        await transport.close(code=1011, reason=str(exc)[:120])
        return

    await serve_and_close_connection(transport, con, serving_authenticator(databases, sessions), {}, text_app=text_app)

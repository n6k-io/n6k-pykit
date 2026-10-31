"""A page socket's text frames: JSON-RPC ``render`` plus every ``@action``, the
actions also served as MCP tools, plus the MCP-only ``show_page``. Built per
connection, bound to its Sessions."""

import asyncio
from typing import TYPE_CHECKING, Any, Callable

from mcp.server.mcpserver import MCPServer
from n6k_server.jsonrpc import JsonRpc, JsonRpcApp

from n6k_app.actions import Action, action_handler
from n6k_app.session_state import Session

if TYPE_CHECKING:
    from n6k_app.database import Database

Render = Callable[[dict[str, Any]], dict[str, Any]]
Show = Callable[[dict[str, Any]], dict[str, Any]]


def build_text_app(
    name: str,
    databases: dict[str, "Database"],
    actions: list[Action],
    sessions: dict[str, Session],
    render: Render | None,
    show: Show | None = None,
) -> JsonRpcApp:
    owners: dict[str, list[str]] = {}
    for act in actions:
        owners.setdefault(act.name, []).append(act.fn.__qualname__)
    if clashes := {action: fns for action, fns in owners.items() if len(fns) > 1}:
        detail = "; ".join(f"'{action}' by {fns}" for action, fns in sorted(clashes.items()))
        raise ValueError(f"These actions are served over one socket and share a name ({detail}). Rename one.")
    if render is not None and "render" in owners:
        raise ValueError("an action named 'render' clashes with this page's render method; rename it.")
    if show is not None and "show_page" in owners:
        raise ValueError("an action named 'show_page' clashes with this page's show_page tool; rename it.")
    handlers = {act.name: action_handler(act, databases, sessions) for act in actions}

    rpc = JsonRpc()
    mcp = MCPServer(name)
    for action, fn in handlers.items():
        rpc.method(action)(fn)
        mcp.add_tool(fn, name=action)
    if render is not None:
        page_render = render

        async def render_method(state: dict[str, Any]) -> dict[str, Any]:
            return await asyncio.to_thread(page_render, state)

        rpc.method("render")(render_method)
    if show is not None:
        page_show = show

        async def show_page(state: dict[str, Any] | None = None) -> dict[str, Any]:
            """What this page shows. For a page.py: {"type": "py", "tree": ...}, the component
            tree rendered against `state` (default {}). For page.mdx / page.tsx, which render in
            the browser: {"type": "mdx"|"tsx", "source": ...}, the page's source; `state` is ignored."""
            return await asyncio.to_thread(page_show, state or {})

        mcp.add_tool(show_page, name="show_page")
    return JsonRpcApp(rpc, mcp)

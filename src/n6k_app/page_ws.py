"""Page websocket at ``/<slug>/ws``; its JSON-RPC ``render(state)`` returns the tree:

    {"component", "props", "children"}

``render_tree`` imports ``<slug>/page.py`` and runs its ``page()`` against the
state; the client reconciles the returned tree into React. The MCP tool
``show_page`` returns that tree too, or the source of a ``page.mdx``/``page.tsx``.
"""

import functools
import importlib
import logging
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from fastapi import FastAPI, WebSocket
from nextpy_server.discover import PAGE_EXTENSIONS

from n6k_app.actions import page_actions
from n6k_app.database import Database
from n6k_app.frontend import Page
from n6k_app.serve import serve_databases

_log = logging.getLogger(__name__)


def _text(value: str) -> dict[str, Any]:
    return {"component": "p", "props": {}, "children": [value]}


def _page_file(app_dir: str, slug: str) -> str:
    return os.path.join(app_dir, "page.py") if slug == "index" else os.path.join(app_dir, slug, "page.py")


def _page_source(app_dir: str, slug: str) -> Path | None:
    base = Path(app_dir) if slug == "index" else Path(app_dir, slug)
    for ext in PAGE_EXTENSIONS:
        if (file := base / f"page{ext}").exists():
            return file
    return None


def _load_page(slug: str) -> Page:
    """Import the page as a namespace-package member (``app.<slug>.page``).

    Loading by dotted name sets ``__package__`` so ``from .data import simulate``
    resolves to the same cached data module (keeping its open connection pools).
    The page module itself is reloaded fresh so dev edits take effect.
    """
    app_dir = os.environ.get("N6K_APP_DIR") or os.getcwd()
    src_dir = os.environ.get("N6K_SRC_DIR") or os.path.dirname(app_dir)
    app_pkg = os.path.basename(app_dir)  # e.g. "app"

    rel = "page" if slug == "index" else f"{slug.replace('/', '.')}.page"
    mod_name = f"{app_pkg}.{rel}"

    file = _page_file(app_dir, slug)
    if not os.path.exists(file):
        raise FileNotFoundError(file)

    sys.modules.pop(mod_name, None)

    added = src_dir not in sys.path
    if added:
        sys.path.insert(0, src_dir)
    try:
        module = importlib.import_module(mod_name)
    finally:
        if added:
            sys.path.remove(src_dir)

    for value in vars(module).values():
        if isinstance(value, Page):
            return value
    raise LookupError(f"no @page-decorated function found in {file}")


def _page_data_module(slug: str) -> ModuleType | None:
    """This page's ``data.py`` module, or None if it has none.

    Lazy + cached — the data module is imported once and shared, so all
    connections and cross-page imports resolve to one provider (one upstream pool).
    """
    app_dir = os.environ.get("N6K_APP_DIR") or os.getcwd()
    src_dir = os.environ.get("N6K_SRC_DIR") or os.path.dirname(app_dir)
    app_pkg = os.path.basename(app_dir)

    rel = "data" if slug == "index" else f"{slug.replace('/', '.')}.data"
    mod_name = f"{app_pkg}.{rel}"

    added = src_dir not in sys.path
    if added:
        sys.path.insert(0, src_dir)
    try:
        return importlib.import_module(mod_name)
    except ModuleNotFoundError as exc:
        if exc.name is None or not f"{mod_name}.".startswith(f"{exc.name}."):
            raise
        return None
    finally:
        if added:
            sys.path.remove(src_dir)


def _page_databases(slug: str) -> dict[str, Database]:
    """Every ``Database`` in this page's ``data.py`` namespace (defined or
    imported), keyed by variable name = catalog alias. Also the map a render binds
    members against, so a member from a database this page doesn't name is a loud error.
    """
    module = _page_data_module(slug)
    return {} if module is None else _databases_in(module)


def _databases_in(module: ModuleType) -> dict[str, Database]:
    return {name: obj for name, obj in vars(module).items() if isinstance(obj, Database)}


def render_tree(slug: str, state: dict[str, Any], catalogs: dict[str, Database] | None = None) -> dict[str, Any]:
    """Run ``<slug>/page.py``'s page() against state and return its tree.

    ``catalogs`` is this page's ``{alias: Database}``; read fresh from
    ``_page_databases`` when omitted, so a member always resolves its alias against
    the data module as it stands now rather than a map captured at connect time.
    """
    try:
        if catalogs is None:
            catalogs = _page_databases(slug)
        return _load_page(slug).render(state, catalogs)
    except Exception as exc:  # surface author/runtime errors into the tree
        _log.exception("render_tree failed for slug=%s", slug)
        return {
            "component": "fragment",
            "props": {},
            "children": [_text(f"{type(exc).__name__}: {exc}")],
        }


def show_page(slug: str, source: Path, state: dict[str, Any]) -> dict[str, Any]:
    if source.suffix == ".py":
        return {"type": "py", "tree": render_tree(slug, state)}
    return {"type": source.suffix[1:], "source": source.read_text()}


def register_page_mux_ws(app: FastAPI) -> None:
    """One websocket per page at ``/<slug>/ws`` (bare ``/ws`` for the index page),
    the path the duckdb extension builds from ``ATTACH 'n6k://host/<slug>'``:

    - binary frames → the page's DuckDB catalog(s), by the alias each client names in
      its ``FT_OPEN``.
    - text frames → JSON-RPC: ``render(state)`` when the page has a ``page.py``, and
      every ``@action`` its ``data.py`` binds, those also as MCP tools; plus the MCP
      tool ``show_page`` when the page has any page file.
    """

    async def serve_page(ws: WebSocket, slug: str) -> None:
        await ws.accept()

        # Dev: import this page's data.py on first connect. Absent in serve, where
        # discovery already imported everything.
        ensure = getattr(app.state, "_n6k_ensure_loaded", None)
        if ensure is not None:
            await ensure(app, slug)

        module = _page_data_module(slug)
        page_dbs: dict[str, Database] = {} if module is None else _databases_in(module)
        actions = [] if module is None else page_actions(module)
        app_dir = os.environ.get("N6K_APP_DIR") or os.getcwd()
        source = _page_source(app_dir, slug)
        has_page = source is not None and source.suffix == ".py"
        _log.debug("serve_page: slug=%s page=%s catalogs=%s", slug, source, list(page_dbs))

        if not page_dbs and not actions and source is None:
            await ws.close(code=4404, reason=f"page {slug!r} serves nothing")
            return

        render = functools.partial(render_tree, slug) if has_page else None
        show = functools.partial(show_page, slug, source) if source is not None else None
        await serve_databases(ws, slug, page_dbs, actions, render, show)

    @app.websocket("/{slug:path}/ws")
    async def page_ws(ws: WebSocket, slug: str) -> None:
        await serve_page(ws, slug)

    # `n6k://host` with no path trims to a bare `/ws`, which `{slug:path}/ws`
    # cannot match (it needs a leading segment). Root page = the "index" slug,
    # matching what the browser mounts for `/`.
    @app.websocket("/ws")
    async def root_ws(ws: WebSocket) -> None:
        await serve_page(ws, "index")

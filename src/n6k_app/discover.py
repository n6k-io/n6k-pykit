"""Directory-based app discovery for n6k_app.

Wraps ``nextpy_server.discover`` to add data-layer loading. A ``data.py`` is
imported under its dotted namespace name (``<app_pkg>.<slug>.data``) so it is the
SAME module the page reaches via ``from .data import simulate`` — one ``Database``
instance backs both the served catalog and the members the page imports. Importing
is the whole registration step; instances have no URL, so callers re-read the
module to resolve a catalog by variable name.
"""

import asyncio
import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from n6k_app.actions import Action, page_actions
from n6k_app.app import create_app
from n6k_app.database import Database, page_databases
from nextpy_server.discover import _find_data_files, _import_module, _slug_for_data_file
from nextpy_server.discover import discover_app as _base_discover_app

logger = logging.getLogger(__name__)


def _src_dir_and_pkg(app_dir: Path) -> tuple[str, str]:
    """Importable source root and app package name for dotted imports.

    ``src/app`` → (``src``, ``app``), resolving ``app.<slug>.data`` as PEP 420
    namespace-package members (no ``__init__.py`` needed)."""
    src_dir = os.environ.get("N6K_SRC_DIR") or str(app_dir.parent)
    return src_dir, app_dir.name


def _dotted_name(app_pkg: str, slug: str, leaf: str) -> str:
    if slug == "index":
        return f"{app_pkg}.{leaf}"
    return f"{app_pkg}.{slug.replace('/', '.')}.{leaf}"


def _import_dotted(mod_name: str, src_dir: str) -> Any:
    """Fresh import (so dev reloads take effect) with ``src_dir`` on ``sys.path``."""
    sys.modules.pop(mod_name, None)
    added = src_dir not in sys.path
    if added:
        sys.path.insert(0, src_dir)
    try:
        return importlib.import_module(mod_name)
    finally:
        if added:
            sys.path.remove(src_dir)


def load_database_file(py_file: str | Path) -> tuple[dict[str, Database], list[Action]]:
    """Import ``py_file`` alone and return the databases and actions it binds.

    No directory walk, no pages. A Database has no URL, so the module's variable
    names are its aliases. Raises ``ValueError`` if the file binds no database.
    """
    py_file = Path(py_file).resolve()
    if not py_file.is_file():
        raise ValueError(f"Python file not found: {py_file}")

    # Clear stale modules from a previous uvicorn --reload pass.
    for key in [k for k in sys.modules if k.startswith("_n6k_")]:
        del sys.modules[key]

    module = _import_module(py_file, py_file.parent)
    databases = page_databases(module)
    if not databases:
        raise ValueError(f"{py_file.name} defines no Database")
    logger.info(f"Loaded {py_file.name}")
    return databases, page_actions(module)


def discover_app(app_dir: str | Path, *, eager: bool | None = None) -> FastAPI:
    """``eager=None`` means eager unless ``N6K_DEV=1``. Eager imports every user
    module up front (fail-fast, complete manifest). Lazy defers them to
    ``ensure_slug_loaded`` and ``prepare_manifest``, so one slow or broken module
    can't delay or fail boot.
    """
    if eager is None:
        eager = os.environ.get("N6K_DEV") != "1"

    if eager:
        app = _base_discover_app(
            app_dir,
            app_factory=create_app,
            on_data_file=_load_data_file,
        )
        app_path = Path(app_dir).resolve()

        app.state._n6k_page_meta = _extract_page_meta(app, app_path)
        return app

    # The walk registers routes and sets what the build needs, but imports no user module.
    app = _base_discover_app(
        app_dir,
        app_factory=create_app,
        on_data_file=lambda *_: None,
    )
    app.state._n6k_loaded_slugs = set()
    app.state._n6k_ensure_loaded = ensure_slug_loaded
    app.state._n6k_prepare_manifest = prepare_manifest
    return app


# Serializes first-visit imports: each runs in a thread that adds then removes
# src_dir on sys.path, so a concurrent one could lose it mid-import.
_import_lock = asyncio.Lock()


def _import_once(mod_name: str, src_dir: str) -> Any:
    """Import a module by dotted name if not already imported, with ``src_dir`` on
    ``sys.path``. Unlike ``_import_dotted`` this does NOT pop a cached copy — the data
    module is imported once per process so its ``Database`` instances (and their open
    connection pools) persist across page websockets."""
    added = src_dir not in sys.path
    if added:
        sys.path.insert(0, src_dir)
    try:
        return importlib.import_module(mod_name)
    finally:
        if added:
            sys.path.remove(src_dir)


async def ensure_slug_loaded(app: FastAPI, slug: str) -> None:
    """Import a page's ``data.py`` on first visit (dev).

    Idempotent and cached via ``app.state._n6k_loaded_slugs``; a page with no
    ``data.py`` caches as loaded (nothing to import). A user import that raises leaves
    the slug uncached and propagates, so one broken page surfaces its error on visit
    without affecting other pages or the server, and the next visit retries.
    """
    loaded: set[str] = app.state._n6k_loaded_slugs
    if slug in loaded:
        return
    async with _import_lock:
        if slug in loaded:
            return
        app_dir = Path(os.environ["N6K_APP_DIR"])
        data_file = app_dir / "data.py" if slug == "index" else app_dir / slug / "data.py"
        if not data_file.is_file():
            loaded.add(slug)
            return
        src_dir, app_pkg = _src_dir_and_pkg(app_dir)
        dotted = _dotted_name(app_pkg, slug, "data")
        await asyncio.to_thread(_import_once, dotted, src_dir)
        loaded.add(slug)


async def prepare_manifest(app: FastAPI) -> None:
    """Populate what the build reads: ``_n6k_page_meta`` and the ``sys.modules``
    entries ``catalogs_by_slug`` introspects. Runs inside the backgrounded dev build
    so user code stays off the synchronous startup path.
    """
    app_dir = Path(os.environ["N6K_APP_DIR"])
    for data_file in _find_data_files(app_dir):
        slug = _slug_for_data_file(data_file, app_dir)
        try:
            await ensure_slug_loaded(app, slug)
        except Exception as e:
            logger.warning("manifest load skipped for %s: %s", slug, e)
    app.state._n6k_page_meta = _extract_page_meta(app, app_dir)


def _extract_page_meta(app: FastAPI, app_dir: Path) -> dict[str, dict[str, Any]]:
    entry_points = app.state._n6k_entry_points
    src_dir, app_pkg = _src_dir_and_pkg(app_dir)
    meta: dict[str, dict[str, Any]] = {}

    for key, rel_path in entry_points.items():
        if not rel_path.endswith(".py"):
            continue  # .mdx/.tsx keep their JS-side frontmatter
        slug = key.split("/", 1)[1] if "/" in key else key
        try:
            module = _import_dotted(_dotted_name(app_pkg, slug, "page"), src_dir)
        except Exception as e:
            logger.warning("page meta skipped for %s: %s", slug, e)
            continue

        page_meta: dict[str, Any] = {}
        if (title := getattr(module, "title", None)) is not None:
            page_meta["title"] = str(title)
        if (section := getattr(module, "section", None)) is not None:
            page_meta["section"] = str(section)
        if (nav_order := getattr(module, "nav_order", None)) is not None:
            page_meta["navOrder"] = int(nav_order)
        if (nav := getattr(module, "nav", None)) is not None:
            page_meta["nav"] = bool(nav)
        # The .py analogue of extra .mdx/.tsx frontmatter; the fields above win.
        if isinstance((extra := getattr(module, "meta", None)), dict):
            for extra_key, extra_val in extra.items():
                page_meta.setdefault(str(extra_key), extra_val)
        meta[key] = page_meta

    return meta


def _load_data_file(app: FastAPI, data_file: Path, app_dir: Path) -> None:
    """Not a throwaway ``_n6k_*`` name, so the page's ``from .data import …`` reuses
    this module and a member resolves its catalog alias by identity at render time.
    """
    slug = _slug_for_data_file(data_file, app_dir)
    src_dir, app_pkg = _src_dir_and_pkg(app_dir)
    _import_dotted(_dotted_name(app_pkg, slug, "data"), src_dir)
    logger.info(f"Loaded {data_file.relative_to(app_dir)}")

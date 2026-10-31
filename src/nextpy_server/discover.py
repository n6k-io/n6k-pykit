"""Directory-based app discovery: walk a dir for page.{tsx,mdx,py} routes and
data.py files, optionally delegating data loading via callbacks."""

import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable

from fastapi import FastAPI

from nextpy_server.app import create_app
from nextpy_server.pages import page as register_page
from nextpy_server.pages import register_public_passthrough

logger = logging.getLogger(__name__)

# .py pages are server-driven: rendered via <ServerPage> over /<slug>/ws, not
# compiled to JS (the build emits a client shell for them).
PAGE_EXTENSIONS = (".tsx", ".mdx", ".py")


def discover_app(
    app_dir: str | Path,
    *,
    app_factory: Callable[[], FastAPI] | None = None,
    on_data_file: Callable[[FastAPI, Path, Path], None] | None = None,
) -> FastAPI:
    """Build a FastAPI app from a directory structure.

    Directory layout:
        app_dir/
            n6k.py          # optional — must export `app`
            data.py          # root data pipeline
            page.tsx           # root page → route /
            summary/
                data.py      # child data (can derive from parent)
                page.tsx       # route /summary
    """
    app_dir = Path(app_dir).resolve()

    if not app_dir.is_dir():
        raise ValueError(f"App directory not found: {app_dir}")

    # Clean stale _n6k_ modules from previous loads (uvicorn --reload)
    for key in [k for k in sys.modules if k.startswith("_n6k_")]:
        del sys.modules[key]

    factory = app_factory or create_app

    n6k_py = app_dir / "n6k.py"
    if n6k_py.exists():
        app = _load_app_from_file(n6k_py)
        logger.info(f"Loaded app from {n6k_py}")
    else:
        app = factory()
        logger.info("Created default app (no n6k.py found)")

    data_files = _find_data_files(app_dir)
    for data_file in data_files:
        if on_data_file is not None:
            on_data_file(app, data_file, app_dir)
        else:
            _load_data_file(app, data_file, app_dir)

    pages = _find_pages(app_dir)
    if not pages:
        logger.warning(f"No page files found in {app_dir}")
    _register_pages(app, pages, app_dir)

    # Root static passthrough — registered after all page routes so it only
    # serves public/ files for paths no page claims.
    register_public_passthrough(app)

    cwd = Path.cwd()
    app.state._n6k_source_dir = str(app_dir.relative_to(cwd))
    app.state._n6k_entry_points = {f"pages/{slug}": str(page_file.relative_to(cwd)) for slug, page_file in pages}

    return app


def _load_app_from_file(n6k_py: Path) -> FastAPI:
    spec = importlib.util.spec_from_file_location("_n6k_app_config", str(n6k_py))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app: FastAPI | None = getattr(module, "app", None)
    if app is None:
        raise ValueError(f"{n6k_py} must export an 'app' object")
    return app


def _find_data_files(app_dir: Path) -> list[Path]:
    """Find all data.py files, sorted parent-first (shortest path first)."""
    return sorted(app_dir.rglob("data.py"), key=lambda p: len(p.parts))


def _slug_for_data_file(data_file: Path, app_dir: Path) -> str:
    rel = data_file.parent.relative_to(app_dir)
    slug = str(rel).replace("\\", "/")
    return "index" if slug == "." else slug


def _load_data_file(app: FastAPI, data_file: Path, app_dir: Path) -> None:
    """Import a data.py so its decorators register."""
    _import_module(data_file, app_dir)
    logger.info(f"Loaded {data_file.relative_to(app_dir)}")


def _import_module(data_file: Path, app_dir: Path) -> ModuleType:
    safe_name = str(data_file.relative_to(app_dir)).replace("/", "_").replace(".", "_")
    module_name = f"_n6k_{safe_name}"
    spec = importlib.util.spec_from_file_location(module_name, str(data_file))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _find_pages(app_dir: Path) -> list[tuple[str, Path]]:
    """Find all ``page`` files with a ``PAGE_EXTENSIONS`` suffix and return (slug, path) pairs."""
    pages = []
    seen_slugs = set()

    page_files: list[Path] = []
    for ext in PAGE_EXTENSIONS:
        page_files.extend(app_dir.rglob(f"page{ext}"))
    page_files.sort(key=lambda p: len(p.parts))

    for page_file in page_files:
        rel = page_file.parent.relative_to(app_dir)
        slug = str(rel).replace("\\", "/")
        if slug == ".":
            slug = "index"

        if slug in seen_slugs:
            raise ValueError(
                f"Duplicate page slug '{slug}': found multiple page files " f"that resolve to the same route."
            )
        seen_slugs.add(slug)
        pages.append((slug, page_file))

    return pages


def _register_pages(app: FastAPI, pages: list[tuple[str, Path]], app_dir: Path) -> None:
    for slug, page_file in pages:
        route_path = "/" if slug == "index" else f"/{slug}"

        register_page(app, route_path, "")

        logger.info(f"Registered page: {route_path} ← {page_file.relative_to(app_dir)}")

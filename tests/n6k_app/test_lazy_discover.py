"""Lazy dev discovery: `n6k dev` must register routes without importing any user
``data.py``/``page.py``, then import each page on first visit and in the background
build — while ``serve``/``build``/``inspect`` keep importing everything up
front. These tests pin that split and its fault isolation.
"""

import sys
from pathlib import Path
from typing import Iterator

import pytest

from n6k_app.discover import discover_app, ensure_slug_loaded, prepare_manifest


def _write_app(app_dir: Path, *, broken: bool) -> None:
    app_dir.mkdir(parents=True)
    (app_dir / "data.py").write_text(
        "from n6k_app import Database, action\n\ndb = Database()\n\n\n@action\ndef ping():\n    return {'ok': True}\n"
    )
    (app_dir / "page.py").write_text(
        "from n6k_app.frontend import Components, page\n\ntitle = 'Home'\n\n\n"
        "@page\ndef home(c: Components):\n    pass\n"
    )
    if broken:
        broken_dir = app_dir / "broken"
        broken_dir.mkdir()
        (broken_dir / "data.py").write_text("raise RuntimeError('boom at import')\n")
        (broken_dir / "page.py").write_text(
            "from n6k_app.frontend import Components, page\n\n\n@page\ndef broken(c: Components):\n    pass\n"
        )


@pytest.fixture
def app_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> Iterator[Path]:
    """A unique-per-test app package (dotted imports persist in ``sys.modules``),
    cwd'd into tmp_path: the base discover_app resolves the app dir relative to cwd."""
    pkg = "lazyapp" + "".join(ch for ch in request.node.name if ch.isalnum())
    path = tmp_path / pkg
    _write_app(path, broken=request.node.get_closest_marker("no_broken") is None)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("N6K_SRC_DIR", str(path.parent))
    monkeypatch.setenv("N6K_APP_DIR", str(path))
    yield path
    for mod in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
        del sys.modules[mod]


def _index_data_mod(app_dir: Path) -> str:
    return f"{app_dir.name}.data"


async def test_lazy_discovery_imports_no_user_code(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=False)

    assert not any(m == app_dir.name or m.startswith(app_dir.name + ".") for m in sys.modules)
    assert hasattr(app.state, "_n6k_ensure_loaded")
    assert hasattr(app.state, "_n6k_prepare_manifest")
    assert any(getattr(r, "path", None) == "/" for r in app.routes)


async def test_ensure_slug_loaded_imports_data(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=False)

    await ensure_slug_loaded(app, "index")
    assert _index_data_mod(app_dir) in sys.modules

    await ensure_slug_loaded(app, "index")  # idempotent / cached
    assert "index" in app.state._n6k_loaded_slugs


async def test_slug_without_data_caches_cleanly(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=False)

    # Cached as loaded, so a page.py-only page never repeats a failing import.
    await ensure_slug_loaded(app, "nodata")
    assert "nodata" in app.state._n6k_loaded_slugs
    assert f"{app_dir.name}.nodata.data" not in sys.modules


async def test_broken_page_is_isolated(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=False)
    await ensure_slug_loaded(app, "index")

    with pytest.raises(RuntimeError, match="boom at import"):
        await ensure_slug_loaded(app, "broken")

    assert "broken" not in app.state._n6k_loaded_slugs  # uncached → next visit retries
    assert _index_data_mod(app_dir) in sys.modules  # unaffected


async def test_prepare_manifest_populates_meta_despite_broken(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=False)

    await prepare_manifest(app)  # broken page is skipped, not fatal

    assert app.state._n6k_page_meta["pages/index"]["title"] == "Home"
    assert _index_data_mod(app_dir) in sys.modules


@pytest.mark.no_broken
async def test_eager_discovery_imports_up_front(app_dir: Path) -> None:
    app = discover_app(app_dir, eager=True)

    assert _index_data_mod(app_dir) in sys.modules
    assert app.state._n6k_page_meta["pages/index"]["title"] == "Home"

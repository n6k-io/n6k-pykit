"""End-to-end ``n6k selftest``: spawn a server, ATTACH over a websocket, sweep members.

This is the only test that drives the real client path — `n6k_client` loaded into a
DuckDB connection that dials a live server — so it is what catches a renamed extension
function or a changed ATTACH type, neither of which mypy or the in-process tests see.

Needs both the client and server extensions, so it self-skips where there is no local
build.
"""

import socket
import sys
from pathlib import Path
from typing import Iterator

import duckdb
import pytest

from n6k_app.selftest import attach_url, run_selftest

_DATA_PY = """
import pandas as pd
import pyarrow as pa
from n6k_app.database import Database

db = Database()


@db.data(primary_keys=("id",))
def items() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "title": ["milk", "bread"]})


@db.view()
def open_items() -> str:
    return "SELECT * FROM items WHERE id > 1"


@db.rpc(returns=pa.schema([("id", pa.int64()), ("title", pa.string())]))
def search(needle: str) -> pd.DataFrame:
    frame = items()
    return frame[frame["title"].str.contains(needle)]
"""

_PAGE_PY = """
from n6k_app.frontend import Components, page

title = "Home"


@page
def home(c: Components):
    pass
"""


def _extensions_loadable() -> bool:
    from n6k_server.extension import load_n6k, load_n6k_server, load_virtual_catalog_provider

    try:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.load_extension("httpfs")
        load_n6k(con)
        load_virtual_catalog_provider(con)
        load_n6k_server(con)
        con.close()
    except Exception:
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _extensions_loadable(),
    reason="n6k_client/virtual_catalog/n6k_server could not be loaded; set N6K_EXTENSION_DIR to a local build",
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def app_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> Iterator[Path]:
    """A unique-per-test app package (dotted imports persist in ``sys.modules``) under
    tmp_path, cwd'd into its parent: the base ``discover_app`` resolves the app dir
    relative to cwd."""
    pkg = "sfapp" + "".join(ch for ch in request.node.name if ch.isalnum())
    path = tmp_path / pkg
    (path / "reports").mkdir(parents=True)
    (path / "data.py").write_text(_DATA_PY)
    (path / "page.py").write_text(_PAGE_PY)
    (path / "reports" / "data.py").write_text(_DATA_PY)
    (path / "reports" / "page.py").write_text(_PAGE_PY)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("N6K_SRC_DIR", str(tmp_path))
    monkeypatch.setenv("N6K_APP_DIR", str(path))
    yield path
    for mod in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
        del sys.modules[mod]


def _run(app_dir: Path, **kwargs: object) -> int:
    return run_selftest(
        app_dir,
        app_dir.parent,
        "127.0.0.1",
        _free_port(),
        kwargs.pop("only", ()),  # type: ignore[arg-type]
        bool(kwargs.pop("verbose", False)),
    )


def test_attach_url_puts_the_slug_in_the_path_and_not_the_alias() -> None:
    # The alias rides in the ATTACH `AS` name, which the driver sends as FT_OPEN `catalog`.
    assert attach_url("127.0.0.1", 7823, "index") == "n6k://127.0.0.1:7823"
    assert attach_url("127.0.0.1", 7823, "reports") == "n6k://127.0.0.1:7823/reports"


def test_sweeps_every_page_and_passes(app_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(app_dir) == 0
    out = capsys.readouterr().out
    assert "📄 index" in out and "📄 reports" in out
    assert "1 table, 1 view" in out
    assert "❌" not in out


def test_rpcs_warn_when_no_test_file_covers_them(app_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(app_dir) == 0  # a warning does not fail the run
    assert "db.search  rpc untested" in capsys.readouterr().out


def test_page_test_runs_against_the_attached_catalog(app_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_dir / "reports" / "page.test").write_text(
        "query II\nSELECT id, title FROM db.items ORDER BY id;\n----\n1\tmilk\n2\tbread\n\n"
        "query II\nSELECT * FROM db.search('milk');\n----\n1\tmilk\n"
    )
    assert _run(app_dir, only=("reports",)) == 0
    out = capsys.readouterr().out
    assert "page.test  2 checks" in out
    assert "rpc untested" not in out  # a test file covers the page's rpcs


def test_a_failing_page_test_fails_the_run(app_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_dir / "reports" / "page.test").write_text(
        "query II\nSELECT id, title FROM db.items ORDER BY id;\n----\n1\tcheese\n2\tbread\n"
    )
    assert _run(app_dir, only=("reports",)) == 1
    out = capsys.readouterr().out
    assert "expected" in out and "cheese" in out and "milk" in out


def test_a_malformed_page_test_fails_the_run(app_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_dir / "reports" / "page.test").write_text("require n6k_client\n")
    assert _run(app_dir, only=("reports",)) == 1
    assert "unknown directive 'require'" in capsys.readouterr().out


def test_unknown_page_is_rejected(app_dir: Path) -> None:
    import click

    with pytest.raises(click.ClickException, match="no database on page"):
        _run(app_dir, only=("nope",))

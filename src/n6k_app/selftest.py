"""``n6k selftest`` — attach every page's catalogs the way a browser does, and scan them.

Nothing else in the suite exercises the client path: ``ATTACH 'n6k://…'`` over a real
websocket into a real running server. A renamed extension function or a changed ATTACH
type is a plain string, so it passes mypy and every in-process test and only surfaces when
someone opens the page. Attaching alone catches that class of regression.

Discovery and execution are both local: pages come from importing the app's own source,
never from asking the server what it has.
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Iterator

import click
import duckdb
from n6k_server.extension import load_n6k

from n6k_app.database import Database
from n6k_app.sqltest import CheckResult, SqlTestError, parse, run

TEST_FILENAME = "page.test"
_SPAWN_TIMEOUT = 30.0
_ROOT_SLUG = "index"

OK = "ok"
FAIL = "fail"


@dataclass
class Check:
    label: str
    status: str
    kind: str = ""
    detail: str = ""
    note: str = ""


@dataclass
class CatalogResult:
    alias: str
    url: str
    attach_error: str = ""
    members: list[Check] = field(default_factory=list)
    # Members never reached because the attach failed — counted as skipped, not passed.
    unreached: int = 0
    untested_rpcs: list[str] = field(default_factory=list)

    @property
    def attached(self) -> bool:
        return not self.attach_error

    @property
    def failures(self) -> list[Check]:
        return [m for m in self.members if m.status == FAIL]


@dataclass
class PageResult:
    slug: str
    catalogs: list[CatalogResult] = field(default_factory=list)
    test_path: Path | None = None
    test_checks: list[Check] = field(default_factory=list)
    test_error: str = ""
    # Directives not run because no catalog attached — reporting them would just restate
    # the attach failure once per line.
    test_skipped: int = 0

    @property
    def test_failures(self) -> list[Check]:
        return [c for c in self.test_checks if c.status == FAIL]


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _listening(host: str, port: int, timeout: float = 0.25) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def _spawn_server(app_dir: Path, src_dir: Path, host: str, port: int, verbose: bool) -> _Server:
    """A server on host:port with eager discovery, so a broken page fails at boot.

    ``N6K_DEV`` is cleared rather than merely left unset: inherited from the caller's
    shell it would silently switch discovery to the lazy path.

    ``create_app`` calls ``setup_logging()`` at INFO, so a quiet run has to capture the
    child's output rather than set a level. It goes to a temp file, not a pipe, so a
    server that logs heavily cannot block on a buffer nobody is draining; the file is
    replayed if the server dies before it listens.
    """
    env = os.environ.copy()
    env.pop("N6K_DEV", None)
    env["N6K_SRC_DIR"] = str(src_dir)
    env["N6K_APP_DIR"] = str(app_dir)
    env["N6K_HOST"] = host
    env["N6K_PORT"] = str(port)

    log = None if verbose else tempfile.TemporaryFile()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "n6k_app._dir_app:app",
            "--host",
            host,
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT if log else None,
    )
    server = _Server(proc, log)

    deadline = time.monotonic() + _SPAWN_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise click.ClickException(
                f"server exited with code {proc.returncode} before it began listening{server.captured()}"
            )
        if _listening(host, port):
            return server
        time.sleep(0.1)

    server.stop()
    raise click.ClickException(
        f"server did not listen on {host}:{port} within {_SPAWN_TIMEOUT:.0f}s{server.captured()}"
    )


class _Server:
    """A spawned uvicorn plus the temp file holding whatever it printed."""

    def __init__(self, proc: subprocess.Popen[bytes], log: IO[bytes] | None) -> None:
        self._proc = proc
        self._log = log

    def captured(self) -> str:
        if self._log is None:
            return ""
        self._log.seek(0)
        text = self._log.read().decode(errors="replace").strip()
        return f":\n{text}" if text else ""

    def stop(self) -> None:
        self._proc.terminate()
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
        if self._log is not None:
            self._log.close()


def client_connection() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection able to ``ATTACH 'n6k://…'``, exactly as a client would."""
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.install_extension("httpfs")
    except duckdb.Error:
        pass  # already installed, or offline with a cached copy — the load below is the real check
    con.load_extension("httpfs")  # the n6k client's HTTP transport depends on it
    load_n6k(con)
    return con


def attach_url(host: str, port: int, slug: str) -> str:
    """The URL a browser on that page dials. The catalog alias is not in the path — it
    rides in the ATTACH alias, which the driver puts in the HELLO's ``catalog`` field."""
    base = f"n6k://{host}:{port}"
    return base if slug == _ROOT_SLUG else f"{base}/{slug}"


def _members(db: Database) -> Iterator[tuple[str, str, str]]:
    """``(name, schema, kind)`` for every queryable member, in report order.

    Derives carry a parent but live in the same registry as sources, so both come out of
    ``table_specs()``.
    """
    registry = db.table_registry
    for name, spec in sorted(registry.table_specs().items()):
        yield name, spec.schema, "derive" if spec.parent is not None else "table"
    for name, view in sorted(registry.view_specs().items()):
        yield name, view.schema, "view"


def _sweep_member(con: duckdb.DuckDBPyConnection, alias: str, schema: str, name: str, kind: str) -> Check:
    label = f"{alias}.{name}"
    target = f"{_quote(alias)}.{_quote(schema)}.{_quote(name)}"
    try:
        cur = con.execute(f"SELECT * FROM {target} LIMIT 1")
        rows = cur.fetchall()
        columns = len(cur.description or ())
    except Exception as exc:
        return Check(label, FAIL, kind, f"{type(exc).__name__}: {exc}")

    describe = (
        f"SELECT * FROM n6k_table_describe({_literal(alias)}, "
        f'schema := {_literal(schema)}, "table" := {_literal(name)})'
    )
    try:
        con.execute(describe).fetchall()
    except Exception as exc:
        return Check(label, FAIL, kind, f"n6k_table_describe: {type(exc).__name__}: {exc}")

    return Check(label, OK, kind, note=f"{len(rows)} row, {columns} cols")


def _sweep_catalog(con: duckdb.DuckDBPyConnection, alias: str, db: Database, url: str) -> CatalogResult:
    result = CatalogResult(alias=alias, url=url)
    members = list(_members(db))
    try:
        con.execute(f"ATTACH {_literal(url)} AS {_quote(alias)} (TYPE n6k)")
    except Exception as exc:
        result.attach_error = f"{type(exc).__name__}: {exc}"
        result.unreached = len(members)
        return result

    for name, schema, kind in members:
        result.members.append(_sweep_member(con, alias, schema, name, kind))
    result.untested_rpcs = sorted(db.table_registry._rpcs)
    return result


def _test_path(app_dir: Path, slug: str) -> Path:
    return app_dir / TEST_FILENAME if slug == _ROOT_SLUG else app_dir / slug / TEST_FILENAME


def _as_check(result: CheckResult) -> Check:
    return Check(
        label=f"{TEST_FILENAME}:{result.directive.line}",
        status=OK if result.ok else FAIL,
        detail=result.detail,
        note=result.directive.sql.splitlines()[0],
    )


def _run_page(app_dir: Path, host: str, port: int, slug: str, catalogs: dict[str, Database]) -> PageResult:
    page = PageResult(slug=slug)
    url = attach_url(host, port, slug)
    con = client_connection()
    try:
        for alias, db in catalogs.items():
            page.catalogs.append(_sweep_catalog(con, alias, db, url))

        path = _test_path(app_dir, slug)
        if not path.is_file():
            return page

        page.test_path = path
        try:
            directives = parse(path.read_text(), str(path))
        except SqlTestError as exc:
            page.test_error = str(exc)  # malformed regardless of whether the server is up
            return page

        if not any(c.attached for c in page.catalogs):
            page.test_skipped = len(directives)
            return page

        page.test_checks = [_as_check(r) for r in run(con, directives)]
        for catalog in page.catalogs:
            catalog.untested_rpcs = []
    finally:
        con.close()
    return page


def _summarize(catalog: CatalogResult) -> str:
    counts: dict[str, int] = {}
    for member in catalog.members:
        counts[member.kind] = counts.get(member.kind, 0) + 1
    if not counts:
        return "no members"
    return ", ".join(f"{n} {kind}{'s' if n != 1 else ''}" for kind, n in sorted(counts.items()))


def _emit_catalog(catalog: CatalogResult, verbose: bool) -> None:
    if not catalog.attached:
        click.echo(f"  ❌ {catalog.alias}  attach failed")
        click.echo(f"       {catalog.url}")
        for line in catalog.attach_error.splitlines():
            click.echo(f"       {line}")
        return

    if verbose:
        click.echo(f"  🔌 {catalog.url} → {catalog.alias}")
        for member in catalog.members:
            icon = "✅" if member.status == OK else "❌"
            click.echo(f"     {icon} {member.label}  {member.note or member.detail}")
    else:
        failures = catalog.failures
        if failures:
            click.echo(f"  ❌ {catalog.alias}  {len(failures)} of {len(catalog.members)} failed")
            for member in failures:
                click.echo(f"       {member.label}  {member.detail}")
        else:
            click.echo(f"  ✅ {catalog.alias}  {_summarize(catalog)}")

    for name in catalog.untested_rpcs:
        click.echo(f"  ⚠️  {catalog.alias}.{name}  rpc untested (no {TEST_FILENAME})")


def _emit_page(page: PageResult, verbose: bool) -> None:
    click.echo(f"\n📄 {page.slug}")
    for catalog in page.catalogs:
        _emit_catalog(catalog, verbose)

    if page.test_error:
        click.echo(f"  ❌ {TEST_FILENAME}  {page.test_error}")
        return

    if page.test_skipped:
        click.echo(f"  ⏭️  {TEST_FILENAME}  {page.test_skipped} checks skipped — no catalog attached")
        return

    failures = page.test_failures
    if page.test_checks and not failures:
        click.echo(f"  ✅ {TEST_FILENAME}  {len(page.test_checks)} checks")
    for check in failures:
        click.echo(f"  ❌ {check.label}")
        click.echo(f"       {check.note}")
        for line in check.detail.splitlines():
            click.echo(f"       {line}")


def _tally(pages: list[PageResult]) -> tuple[int, int, int, int]:
    """``(passed, failed, warned, skipped)`` across every page."""
    passed = failed = warned = skipped = 0
    for page in pages:
        for catalog in page.catalogs:
            if not catalog.attached:
                failed += 1
                skipped += catalog.unreached
                continue
            passed += len(catalog.members) - len(catalog.failures)
            failed += len(catalog.failures)
            warned += len(catalog.untested_rpcs)
        failed += 1 if page.test_error else len(page.test_failures)
        passed += len(page.test_checks) - len(page.test_failures)
        skipped += page.test_skipped
    return passed, failed, warned, skipped


def run_selftest(app_dir: Path, src_dir: Path, host: str, port: int, only: tuple[str, ...], verbose: bool) -> int:
    """Sweep every page that serves a database. Returns the process exit code."""
    from n6k_app.database import page_databases_by_slug
    from n6k_app.discover import discover_app

    app = discover_app(app_dir, eager=True)
    pages = page_databases_by_slug(app, app_dir.name)
    if only:
        unknown = sorted(set(only) - set(pages))
        if unknown:
            known = ", ".join(sorted(pages)) or "none"
            raise click.ClickException(f"no database on page(s) {unknown}. Pages with databases: {known}")
        pages = {slug: dbs for slug, dbs in pages.items() if slug in only}

    if not pages:
        click.echo("no page serves a database — nothing to sweep")
        return 0

    server: _Server | None = None
    if _listening(host, port):
        click.echo(f"🔗 using server on {host}:{port}")
    else:
        server = _spawn_server(app_dir, src_dir, host, port, verbose)
        click.echo(f"🚀 server on {host}:{port}")

    results: list[PageResult] = []
    try:
        for slug, catalogs in sorted(pages.items()):
            page = _run_page(app_dir, host, port, slug, catalogs)
            results.append(page)
            _emit_page(page, verbose)
    finally:
        if server is not None:
            server.stop()

    passed, failed, warned, skipped = _tally(results)
    parts = [f"{'❌' if failed else '✅'} {passed} passed"]
    if failed:
        parts.append(f"{failed} failed")
    if warned:
        parts.append(f"⚠️ {warned} warning{'s' if warned != 1 else ''}")
    if skipped:
        parts.append(f"{skipped} skipped")
    click.echo("\n" + "  ".join(parts))
    return 1 if failed else 0

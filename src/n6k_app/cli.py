import click

from n6k_app.build.run import _config_fn
from nextpy_server.cli import (
    APP_DIR,
    PORT,
    PROJECT_DIR,
    SRC_DIR,
    _run_build,
)
from nextpy_server.logging import setup_logging


def _load_dotenv() -> None:
    """Real env vars win over ./.env. Runs once in the parent; reload/serve
    subprocesses inherit os.environ."""
    from pathlib import Path

    from dotenv import load_dotenv

    env_path = Path.cwd() / ".env"
    if env_path.is_file():
        load_dotenv(env_path)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def cli(debug: bool) -> None:
    _load_dotenv()
    if debug:
        import os

        os.environ["N6K_DEBUG"] = "1"


@cli.command()
@click.argument("path", default=".")
@click.option(
    "--ref",
    default=None,
    help="Template branch, tag, or sha to scaffold from. Defaults to the latest published tag.",
)
@click.option(
    "--template-dir",
    default=None,
    help="Scaffold from a local template checkout instead of GitHub (for testing an "
    "unpublished template). Also settable via N6K_TEMPLATE_DIR.",
)
def init(path: str, ref: str | None, template_dir: str | None) -> None:
    """Initialize a new n6k project."""
    from pathlib import Path
    from nextpy_server.build import ensure_deps
    from nextpy_server.config import CONFIG_FILENAME, init_config
    from nextpy_server.scaffold import copy_local_template, fetch_template, local_template_dir, resolve_ref

    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)

    # Generated locally, not part of the template repo.
    if (target / CONFIG_FILENAME).is_file():
        click.echo(f"  {CONFIG_FILENAME} already exists, skipping")
    else:
        init_config(target)
        click.echo(f"  {CONFIG_FILENAME}")

    src = local_template_dir(template_dir)
    try:
        if src:
            click.echo(f"  Copying template from {src}...")
            written = copy_local_template(target, src)
        else:
            ref = resolve_ref(ref)
            click.echo(f"  Fetching template ({ref})...")
            written = fetch_template(target, ref)
    except RuntimeError as e:
        raise click.ClickException(str(e))
    for rel in written:
        click.echo(f"  {rel}")
    if not written:
        click.echo("  Template files already present, skipping")

    if (target / "node_modules").exists():
        click.echo("  node_modules exists, skipping install")
    else:
        click.echo("  Installing dependencies...")
        try:
            ensure_deps(target)
        except RuntimeError as e:
            raise click.ClickException(str(e))
        click.echo("  Dependencies installed")

    click.echo("")
    click.echo(f"Project initialized in {target}")
    click.echo("")
    click.echo("Next steps:")
    if path != ".":
        click.echo(f"  cd {path}")
    click.echo("  uv run n6k dev     Start the dev server")
    click.echo("  uv run n6k build   Build the project")


@cli.group()
def inspect() -> None:
    """Inspect discovered pages/databases for debugging (stat, render, db)."""


@inspect.command("stat")
def inspect_stat() -> None:
    """Show the app's pages, databases (catalogs), and websocket routes."""
    import logging

    from n6k_app.discover import discover_app

    logging.disable(logging.INFO)
    app = discover_app(APP_DIR.resolve(), eager=True)

    entry_points = app.state._n6k_entry_points
    click.echo(f"\nPages ({len(entry_points)}):")
    for key, path in sorted(entry_points.items()):
        slug = key.split("/", 1)[1] if "/" in key else key
        ext = path.rsplit(".", 1)[-1]
        click.echo(f"  /{slug:<24} {ext:<4} ← {path}")

    databases = getattr(app.state, "databases", {})
    click.echo(f"\nDatabases ({len(databases)}):")
    for prefix, db in sorted(databases.items()):
        click.echo(f"  {prefix:<24} catalog={getattr(db, 'catalog', None)!r}")

    ws_routes = sorted(
        getattr(r, "path") for r in app.routes if "websocket" in type(r).__name__.lower() and getattr(r, "path", None)
    )
    click.echo(f"\nWebsocket routes ({len(ws_routes)}):")
    for path in ws_routes:
        click.echo(f"  {path}")


@inspect.command("render")
@click.argument("slug")
@click.option("--state", default="{}", help="JSON UI state for .py pages, e.g. '{\"n\": 60}'.")
def inspect_render(slug: str, state: str) -> None:
    """Render a page without a browser or dev server.

    A `.py` page runs its page() and prints the component tree it produces for
    a given --state. An `.mdx` page compiles through the build's own MDX
    pipeline and prints the compiled module — so a syntax error surfaces here
    with the compiler's message instead of at the next full build.
    """
    import json
    import logging
    import os

    os.environ.setdefault("N6K_APP_DIR", str(APP_DIR.resolve()))
    from n6k_app.discover import discover_app

    # Discover first so databases are imported/finalized, then hand the page its
    # catalogs so a page() parameter resolves to its served alias, as the server does.
    logging.disable(logging.INFO)
    app = discover_app(APP_DIR.resolve(), eager=True)

    entry_points: dict[str, str] = app.state._n6k_entry_points
    source = entry_points.get(f"pages/{slug}")
    if source is None:
        raise click.ClickException(f"No page found for slug {slug!r}. Try `n6k inspect stat` to list pages.")

    if source.endswith(".mdx"):
        import subprocess

        from nextpy_server.build.run import write_config

        # Same first step as `n6k build`, so the JS side sees pages added since the last build.
        write_config(
            PROJECT_DIR,
            src_dir=str(SRC_DIR),
            app_dir=str(APP_DIR),
            entry_points=entry_points,
            asset_prefix=app.state.asset_prefix,
            config_fn=_config_fn,
            app=app,
        )
        result = subprocess.run(
            ["bun", "run", "n6k-build", "--render", f"pages/{slug}"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_DIR),
        )
        if result.stdout:
            click.echo(result.stdout, nl=False)
        if result.stderr:
            click.echo(result.stderr, nl=False, err=True)
        if result.returncode != 0:
            raise SystemExit(result.returncode)
        return

    from n6k_app.page_ws import _page_databases, render_tree

    tree = render_tree(slug, json.loads(state), _page_databases(slug))
    click.echo(json.dumps(tree, indent=2, ensure_ascii=False))


@inspect.command("db")
@click.argument("slug")
def inspect_db(slug: str) -> None:
    """Show a page's database members and how a page() reaches each one.

    For every source/view/rpc, prints its kind and both data paths: the client-side
    `.ref()` query and the server-side call that returns rows.
    """
    import logging

    from n6k_app.discover import discover_app

    logging.disable(logging.INFO)
    app = discover_app(APP_DIR.resolve(), eager=True)
    from n6k_app.database import page_databases_by_slug

    dbs = page_databases_by_slug(app, APP_DIR.name).get(slug, {})
    if not dbs:
        raise click.ClickException(f"No database found for page {slug!r}. Try `n6k inspect stat` to list pages.")

    for var, db in dbs.items():
        click.echo(f"\n{var}  (catalog={var!r}, url=/{slug}/{var})")
        reg = db.table_registry
        for name in sorted(reg.table_specs()):
            click.echo(f"  source  {name:<20}  {var}.{name}.ref()   |  {var}.{name}() -> DataFrame")
        for name in sorted(reg.view_specs()):
            click.echo(f"  view    {name:<20}  {var}.{name}.ref()   |  (SQL view — query only)")
        for name, spec in sorted(reg._rpcs.items()):
            args = ", ".join(spec.scalar_params)
            click.echo(f"  rpc     {name:<20}  {var}.{name}.ref({args})   |  {var}.{name}({args}) -> rows")


@cli.command()
@click.option("--host", default="127.0.0.1", help="Bind host.")
@click.option("--port", default=PORT, type=int, help="Bind port.")
@click.option("--page", "pages", multiple=True, help="Only sweep this page (repeatable).")
@click.option("-v", "--verbose", is_flag=True, help="List every member, not just failures.")
def selftest(host: str, port: int, pages: tuple[str, ...], verbose: bool) -> None:
    """Attach every page's catalogs over the wire and scan their members.

    Reuses a server already listening on HOST:PORT, otherwise starts one and stops it
    again. Pages are discovered from local source, never from the server. RPCs need
    arguments, so they are only exercised by a page's `page.test` file.
    """
    import logging

    from n6k_app.selftest import run_selftest

    # create_app() calls setup_logging() at INFO, so setting a level beforehand does not hold.
    logging.disable(logging.INFO)
    raise SystemExit(run_selftest(APP_DIR.resolve(), SRC_DIR.resolve(), host, port, pages, verbose))


@cli.command()
def build() -> None:
    """Discover app, write config, and build CSS/JS. Requires init to have been run first."""
    from n6k_app.discover import discover_app

    # The build needs every module imported for a complete manifest.
    _run_build(discover_fn=lambda d: discover_app(d, eager=True), config_fn=_config_fn)


@cli.command()
@click.option("--host", default="127.0.0.1", help="Bind host.")
@click.option("--port", default=PORT, type=int, help="Bind port.")
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    help="Log level.",
)
def dev(host: str, port: int, log_level: str) -> None:
    """Start uvicorn with --reload and live browser refresh."""
    import os
    import uvicorn

    setup_logging(log_level)
    os.environ["N6K_DEV"] = "1"
    click.echo("Starting dev server (live reload)...")

    src_dir = str(SRC_DIR.resolve())
    app_dir = str(APP_DIR.resolve())
    os.environ["N6K_SRC_DIR"] = src_dir
    os.environ["N6K_APP_DIR"] = app_dir
    os.environ["N6K_HOST"] = host
    os.environ["N6K_PORT"] = str(port)
    uvicorn.run(
        "n6k_app._dir_app:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=True,
        reload_dirs=[src_dir, app_dir],
        reload_includes=["*.py"],
        timeout_graceful_shutdown=1,
    )


@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind host.")
@click.option("--port", default=PORT, type=int, help="Bind port.")
@click.option("--workers", default=1, type=int, help="Number of worker processes.")
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    help="Log level.",
)
def serve(host: str, port: int, workers: int, log_level: str) -> None:
    """Start uvicorn for production (no build, no reload)."""
    import os
    import uvicorn

    setup_logging(log_level)
    click.echo("Starting server...")

    src_dir = str(SRC_DIR.resolve())
    app_dir = str(APP_DIR.resolve())
    os.environ["N6K_SRC_DIR"] = src_dir
    os.environ["N6K_APP_DIR"] = app_dir
    os.environ["N6K_HOST"] = host
    os.environ["N6K_PORT"] = str(port)
    uvicorn.run(
        "n6k_app._dir_app:app",
        host=host,
        port=port,
        workers=workers,
        log_level=log_level,
    )


@cli.command()
@click.argument("pyfile")
@click.option("--host", default="127.0.0.1", help="Bind host.")
@click.option("--port", default=PORT, type=int, help="Bind port.")
@click.option("--reload", is_flag=True, default=False, help="Reload on file changes.")
@click.option(
    "--listen",
    "listen_path",
    default=None,
    envvar="N6K_LISTEN",
    show_envvar=True,
    help="Listen on this Unix socket path instead of HTTP.",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    help="Log level.",
)
def database(pyfile: str, host: str, port: int, reload: bool, listen_path: str | None, log_level: str) -> None:
    """Run a single Python file as a database server (no frontend).

    Every database the file defines is served; a client selects one by the
    variable name it is bound to (its ATTACH catalog alias). Its ``@action``s
    answer JSON-RPC and MCP on the same connection.

    Over HTTP the only endpoint is ``/ws``. With --listen, listen on that Unix
    socket and serve every client in n6k stream framing.
    """
    import os
    from pathlib import Path

    setup_logging(log_level)

    py_path = Path(pyfile).resolve()
    if not py_path.is_file():
        raise click.ClickException(f"File not found: {pyfile}")

    if listen_path:
        from n6k_app.socket_app import run_listen

        click.echo(f"Serving {py_path.name} on socket {listen_path}...")
        try:
            run_listen(str(py_path), listen_path)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        return

    import uvicorn

    os.environ["N6K_DB_FILE"] = str(py_path)
    os.environ["N6K_HOST"] = host
    os.environ["N6K_PORT"] = str(port)
    click.echo(f"Starting database server for {py_path.name}...")

    if reload:
        uvicorn.run(
            "n6k_app._file_app:app",
            host=host,
            port=port,
            log_level=log_level,
            reload=True,
            reload_dirs=[str(py_path.parent)],
            reload_includes=["*.py"],
        )
        return

    from n6k_app.database_app import database_app

    try:
        app = database_app(py_path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    uvicorn.run(app, host=host, port=port, log_level=log_level)


@cli.command()
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    help="Log level.",
)
@click.option(
    "--listen",
    "listen_path",
    required=True,
    envvar="N6K_LISTEN",
    show_envvar=True,
    help="Listen on this Unix socket path.",
)
@click.option(
    "--catalog",
    default="kernel",
    envvar="N6K_CATALOG",
    show_envvar=True,
    show_default=True,
    help="Name of the served n6k catalog.",
)
def kernel(listen_path: str, catalog: str, log_level: str) -> None:
    """Run a Python kernel listening on a Unix socket (--listen).

    One connection carries the exec API (JSON-RPC) and the ``--catalog`` catalog (n6k).
    Every client shares the namespace and receives cell output while connected. The
    namespace lives as long as the process.
    """
    if not catalog.isidentifier():
        raise click.ClickException(f"--catalog {catalog!r} is not a valid identifier")

    from n6k_app.kernel.socket_kernel import run_kernel_listen

    setup_logging(log_level)
    try:
        run_kernel_listen(listen_path, catalog)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command()
@click.argument("socket")
@click.option("--host", default="127.0.0.1", help="Bind host for the WebSocket.")
@click.option("--port", default=PORT, type=int, help="Bind port for the WebSocket.")
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error", "critical"]),
    help="Log level.",
)
def ws(socket: str, host: str, port: int, log_level: str) -> None:
    """Relay a WebSocket to a `n6k database --listen` Unix socket so the duckdb CLI can ATTACH.

    Start `n6k database --listen <socket> <pyfile>`, run this, then from duckdb:
    `ATTACH 'n6k://<host>:<port>' AS x (TYPE n6k);`. Each WebSocket gets its own
    connection to the socket.
    """
    from n6k_app.ws_relay import run_relay

    setup_logging(log_level)
    click.echo(f"Relaying ws://{host}:{port}/ws ⇄ unix:{socket}")
    click.echo(f"  worker:  n6k database --listen {socket} <pyfile>")
    click.echo(f"  duckdb:  ATTACH 'n6k://{host}:{port}' AS x (TYPE n6k);")
    run_relay(socket, host, port, log_level)

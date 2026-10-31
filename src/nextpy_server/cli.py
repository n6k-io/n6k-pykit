from pathlib import Path
from typing import Any, Callable

import click
from fastapi import FastAPI

from nextpy_server.build import (
    build_css,
    copy_wasm,
    run_build,
    write_config,
)
from nextpy_server.config import get_app_dir, get_port, get_src_dir

PROJECT_DIR = Path(".")
SRC_DIR = get_src_dir(PROJECT_DIR)
APP_DIR = get_app_dir(PROJECT_DIR)
PORT = get_port(PROJECT_DIR)


def _build_js() -> None:
    click.echo("Building JS...")
    build_result = run_build(PROJECT_DIR)

    if build_result.returncode != 0:
        click.echo(f"Build failed:\n{build_result.stderr}", err=True)
        raise SystemExit(1)

    click.echo("Build succeeded.")


def _run_build_directory(
    *,
    discover_fn: Callable[[Path], FastAPI] | None = None,
    config_fn: Callable[[Path, dict[str, Any], FastAPI | None], None] | None = None,
) -> None:
    """Build pipeline (assumes init has been run)."""
    if discover_fn is None:
        from nextpy_server.discover import discover_app

        discover_fn = discover_app

    app_dir_path = APP_DIR.resolve()
    click.echo(f"Discovering app from {app_dir_path}...")
    app = discover_fn(app_dir_path)

    page_count = len(app.state.pages)
    click.echo(f"Discovered {page_count} page(s).")

    write_config(
        PROJECT_DIR,
        src_dir=str(SRC_DIR),
        app_dir=str(APP_DIR),
        entry_points=app.state._n6k_entry_points,
        asset_prefix=app.state.asset_prefix,
        config_fn=config_fn,
        app=app,
    )
    click.echo("Wrote build/n6k.build.json.")

    click.echo("Building CSS...")
    build_css(PROJECT_DIR, src_dir=SRC_DIR)
    click.echo("CSS built → build/styles.css")

    _build_js()

    click.echo("Copying WASM extensions...")
    copy_wasm(PROJECT_DIR)
    click.echo("Done.")


def _run_build(
    *,
    discover_fn: Callable[[Path], FastAPI] | None = None,
    config_fn: Callable[[Path, dict[str, Any], FastAPI | None], None] | None = None,
) -> None:
    _run_build_directory(discover_fn=discover_fn, config_fn=config_fn)

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI


def run_build(project_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bun", "run", "n6k-build"],
        capture_output=True,
        text=True,
        cwd=str(project_dir),
    )


def ensure_deps(project_dir: Path) -> None:
    if not (project_dir / "node_modules").exists():
        result = subprocess.run(
            ["bun", "install", "--cwd", str(project_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to install dependencies:\n{result.stderr}")


def write_config(
    project_dir: Path,
    *,
    src_dir: str | None = None,
    app_dir: str | None = None,
    entry_points: dict[str, str] | None = None,
    asset_prefix: str | None = None,
    scripts: list[str] | None = None,
    head_tags: list[str] | None = None,
    config_fn: Callable[[Path, dict[str, Any], FastAPI | None], None] | None = None,
    app: FastAPI | None = None,
) -> None:
    config: dict[str, Any] = {}
    if src_dir:
        config["srcDir"] = src_dir
    if app_dir:
        config["appDir"] = app_dir
    if entry_points:
        config["entryPoints"] = entry_points
    if asset_prefix:
        config["assetPrefix"] = asset_prefix
    if scripts:
        config["scripts"] = scripts
    if head_tags:
        config["headTags"] = head_tags
    # `dedup`: packages the JS bundler must collapse to one instance (a linked/
    # non-hoisted dep would otherwise split a React context).
    from nextpy_server.config import load_config

    dedup = load_config(project_dir).get("dedup")
    if dedup:
        config["dedup"] = dedup
    if config_fn:
        config_fn(project_dir, config, app)
    build_dir = project_dir / "build"
    build_dir.mkdir(parents=True, exist_ok=True)
    (build_dir / "n6k.build.json").write_text(json.dumps(config, indent=2) + "\n")


def build_css(project_dir: Path, src_dir: Path | str | None = None) -> None:
    if src_dir is None:
        from nextpy_server.config import get_src_dir
        src_dir = get_src_dir(project_dir)

    public_dir = project_dir / "build" / "public"
    public_dir.mkdir(parents=True, exist_ok=True)

    css_input = str(Path(src_dir) / "app" / "globals.css")
    result = subprocess.run(
        [
            "bunx",
            "@tailwindcss/cli",
            "-i", css_input,
            "-o", str((public_dir / "styles.css").relative_to(project_dir)),
        ],
        capture_output=True,
        text=True,
        cwd=str(project_dir),
    )

    if result.returncode != 0:
        raise RuntimeError(f"Tailwind CSS build failed:\n{result.stderr}")


def copy_wasm(project_dir: Path) -> None:
    src = project_dir / "node_modules/@n6k.io/db/wasm"
    dst = project_dir / "build" / "public" / "wasm"
    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

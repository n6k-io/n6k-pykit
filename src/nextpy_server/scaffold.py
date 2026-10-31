"""Scaffold new projects from the n6k template repo.

The template lives in its own repo (github.com/n6k-io/template), fetched over HTTP
at ``init`` time rather than bundled — init already needs the network for
``bun install``, so this lets the template evolve independently at no offline cost.
"""

import io
import json
import os
import re
import shutil
import tarfile
import urllib.error
import urllib.request
from pathlib import Path

TEMPLATE_REPO = "n6k-io/template"

# Repo-meta files at the template root that must not land in a scaffolded project.
_SKIP_ROOT_FILES = {"README.md", "LICENSE"}

# VCS/build/dependency dirs never copied from a local template checkout.
_SKIP_DIRS = {".git", "node_modules", "build", ".venv", "__pycache__", "dist"}

_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def _latest_tag() -> str:
    """Return the newest ``vX.Y.Z`` tag in the template repo (GitHub API)."""
    url = f"https://api.github.com/repos/{TEMPLATE_REPO}/tags?per_page=100"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/vnd.github+json", "User-Agent": "n6k-init"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            tags = json.load(resp)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Failed to list template tags ({e.code}): {url}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to reach {url}: {e.reason}") from e

    versioned: list[tuple[tuple[int, ...], str]] = []
    for tag in tags:
        m = _TAG_RE.match(tag.get("name", ""))
        if m:
            versioned.append((tuple(int(p) for p in m.groups()), str(tag["name"])))
    if not versioned:
        raise RuntimeError(
            f"No vX.Y.Z tags found in {TEMPLATE_REPO}. " "Pass --ref to scaffold from a specific branch or tag."
        )
    versioned.sort()
    return versioned[-1][1]


def resolve_ref(ref: str | None = None) -> str:
    return ref or os.environ.get("N6K_TEMPLATE_REF") or _latest_tag()


def _archive_url(ref: str) -> str:
    # This form resolves branches, tags, and full shas alike.
    return f"https://github.com/{TEMPLATE_REPO}/archive/{ref}.tar.gz"


def fetch_template(target: Path, ref: str | None = None) -> list[Path]:
    """Download the template repo at *ref* and extract into *target*.

    Existing files are left untouched (``init`` is idempotent). Returns the
    project-relative paths written, in archive order.
    """
    target = Path(target)
    ref = resolve_ref(ref)
    url = _archive_url(ref)
    try:
        with urllib.request.urlopen(url) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise RuntimeError(
                f"Template ref {ref!r} not found ({url}). " "Pass --ref with an existing branch or tag."
            ) from e
        raise RuntimeError(f"Failed to download template ({e.code}): {url}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to reach {url}: {e.reason}") from e

    written: list[Path] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            # GitHub prefixes every entry with '<repo>-<ref>/'; strip it.
            parts = member.name.split("/", 1)
            if len(parts) != 2 or not parts[1]:
                continue
            rel = parts[1]
            if rel in _SKIP_ROOT_FILES:
                continue
            dest = target / rel
            if dest.exists():
                continue
            extracted = tar.extractfile(member)
            if extracted is None:
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(extracted.read())
            written.append(Path(rel))
    return written


def copy_local_template(target: Path, source: Path | str) -> list[Path]:
    """``fetch_template`` from a local checkout; also skips VCS/build/dep dirs."""
    source = Path(source)
    if not source.is_dir():
        raise RuntimeError(f"Local template dir not found: {source}")
    target = Path(target)
    written: list[Path] = []
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source)
        if set(rel.parts) & _SKIP_DIRS:
            continue
        if str(rel) in _SKIP_ROOT_FILES:
            continue
        dest = target / rel
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        written.append(rel)
    return written


def local_template_dir(template_dir: str | None = None) -> str | None:
    """``None`` means fetch from GitHub."""
    return template_dir or os.environ.get("N6K_TEMPLATE_DIR")


def scaffold(target: Path, ref: str | None = None, template_dir: str | None = None) -> list[Path]:
    src = local_template_dir(template_dir)
    if src:
        return copy_local_template(target, src)
    return fetch_template(target, ref)

"""``GET /_info`` — runtime version + capability diagnostics.

Reports *what actually loaded* — the real extension file, its version, and whether
the handler's required functions are present — so a stale/wrong ``vcatprovider`` (which
otherwise surfaces only as a cryptic ``... does not exist`` deep in a query) is one
``curl`` away.
"""

import json
import logging
import os
import platform
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, Request
from virtual_catalog.extension import (
    VIRTUAL_CATALOG_EXT_DIR_ENV,
    VCATPROVIDER,
    load_vcatprovider,
    local_extension_path,
)
from starlette.responses import JSONResponse

from n6k_app.database import page_databases_by_slug

_log = logging.getLogger(__name__)

# Functions the session path requires from the vcatprovider extension.
# If any are missing, the loaded extension is stale or wrong.
_REQUIRED_EXT_FUNCTIONS = ("vcatprovider_register", "vcatprovider_unregister", "vcatprovider_table_permissions")


def _server_versions() -> dict[str, Any]:
    try:
        n6k_version = pkg_version("n6k")
    except PackageNotFoundError:
        n6k_version = "unknown"  # source tree, never installed (no dist metadata)
    return {
        "n6k": n6k_version,
        "duckdb_python": duckdb.__version__,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }


def _extension_info() -> dict[str, Any]:
    """Load the n6k extension exactly as a session would and report what landed."""
    local_path = local_extension_path(VCATPROVIDER)
    info: dict[str, Any] = {
        VIRTUAL_CATALOG_EXT_DIR_ENV: os.environ.get(VIRTUAL_CATALOG_EXT_DIR_ENV),
        "local_extension_path": local_path,
        "source": "local-build" if local_path else "installed-by-name (GCS/cache)",
    }
    try:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_vcatprovider(con)

        cur = con.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = ?", [VCATPROVIDER])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        if rows:
            row = rows[0]
            info["loaded"] = {
                k: row[k]
                for k in ("extension_version", "install_path", "install_mode", "installed_from", "loaded", "installed")
                if k in row
            }

        fns = [
            r[0]
            for r in con.execute(
                "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'provider%' ORDER BY 1"
            ).fetchall()
        ]
        info["provider_functions"] = fns
        missing = [f for f in _REQUIRED_EXT_FUNCTIONS if f not in fns]
        info["missing_required_functions"] = missing
        info["ok"] = not missing
        if missing:
            hint = (
                f"Set and EXPORT {VIRTUAL_CATALOG_EXT_DIR_ENV} to a fresh local build directory "
                "(duckdb-virtual-catalog/build/release/extension), or clear the "
                "stale cache at ~/.duckdb/extensions."
                if not local_path
                else f"The extension at {local_path} is stale — rebuild it (make in duckdb-virtual-catalog)."
            )
            info["warning"] = f"Loaded {VCATPROVIDER} is missing {missing}. {hint}"
        con.close()
    except Exception as exc:  # report, don't 500 — this is the diagnostics route
        info["ok"] = False
        info["error"] = f"{type(exc).__name__}: {exc}"
    return info


def _database_info(app: FastAPI) -> dict[str, Any]:
    """Per-page ``{slug: {catalog: …}}`` from the already-imported data modules."""
    out: dict[str, Any] = {}
    try:
        app_dir = os.environ.get("N6K_APP_DIR") or os.getcwd()
        for slug, catalogs in page_databases_by_slug(app, os.path.basename(app_dir)).items():
            out[slug] = {
                name: {
                    "schemas": sorted(db.table_registry.schemas or {"main"}),
                    "external_connections": bool(db.connection_pool and db.connection_pool.has_connections),
                }
                for name, db in catalogs.items()
            }
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _worker_info() -> Any:
    cfg = Path("build/n6k.build.json")
    if not cfg.is_file():
        return None
    try:
        return json.loads(cfg.read_text()).get("workers")
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def register_info(app: FastAPI) -> None:
    @app.get("/_info")
    async def info(request: Request) -> JSONResponse:
        payload = {
            "server": _server_versions(),
            "extension": _extension_info(),
            "databases": _database_info(request.app),
            "workers": _worker_info(),
        }
        return JSONResponse(payload)

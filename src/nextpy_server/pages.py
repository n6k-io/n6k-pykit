import mimetypes
import os
from pathlib import Path

from fastapi import APIRouter, FastAPI
from starlette.responses import HTMLResponse, Response

DEV_MODE = os.environ.get("N6K_DEV") == "1"

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="{asset_prefix}/styles.css">
</head>
<body>
<div id="root"></div>
<script type="module" src="{script_src}"></script>
{dev_script}
</body>
</html>"""

BUILD_DIR = Path("build")
PUBLIC_DIR = BUILD_DIR / "public"

asset_router = APIRouter()


MIME_TYPES = {
    ".css": "text/css",
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".map": "application/json",
}


@asset_router.get("/{path:path}")
async def serve_asset(path: str) -> Response:
    asset = PUBLIC_DIR / path
    if not asset.exists() or not asset.is_file():
        return Response("not found", status_code=404)
    media_type = MIME_TYPES.get(asset.suffix, "application/octet-stream")
    return Response(content=asset.read_bytes(), media_type=media_type)


# Served at the site root; build artifacts are served at the asset prefix instead.
PROJECT_PUBLIC_DIR = Path("public")


def register_public_passthrough(app: FastAPI) -> None:
    """Register this LAST, after every page route, so exact page paths win; it 404s
    unless a real file exists under `public/`, so it can't shadow routing."""
    public_root = PROJECT_PUBLIC_DIR.resolve()

    @app.get("/{public_path:path}")
    async def serve_public(public_path: str) -> Response:
        asset = (public_root / public_path).resolve()
        # Reject empty paths and traversal that escapes public/.
        if public_root not in asset.parents or not asset.is_file():
            return Response("not found", status_code=404)
        media_type = mimetypes.guess_type(asset.name)[0] or "application/octet-stream"
        return Response(content=asset.read_bytes(), media_type=media_type)


def wrap_html(asset_prefix: str, script_src: str) -> str:
    dev_script = ""
    if DEV_MODE:
        from nextpy_server.dev import dev_script as make_dev_script

        dev_script = make_dev_script(asset_prefix)
    return HTML_TEMPLATE.format(asset_prefix=asset_prefix, script_src=script_src, dev_script=dev_script)


def page(app: FastAPI, path: str, snippet: str) -> None:
    slug = path.strip("/") or "index"
    app.state.pages[slug] = snippet
    _register_shared_routes(app)

    prefix = app.state.asset_prefix
    script_src = f"{prefix}/pages/{slug}.js"

    html_file = PUBLIC_DIR / "pages" / f"{slug}.html"

    @app.get(path, response_class=HTMLResponse)
    async def serve_page() -> HTMLResponse:
        if html_file.exists():
            return HTMLResponse(html_file.read_text())
        return HTMLResponse(wrap_html(prefix, script_src))


def _register_shared_routes(app: FastAPI) -> None:
    if getattr(app.state, "_asset_routes_registered", False):
        return
    app.state._asset_routes_registered = True

    if DEV_MODE:
        from nextpy_server.dev import register_dev

        register_dev(app)

    app.include_router(asset_router, prefix=app.state.asset_prefix)

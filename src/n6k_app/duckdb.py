from pathlib import Path

from fastapi import APIRouter, FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

MIME_TYPES = {
    ".wasm": "application/wasm",
    ".json": "application/json",
    ".js": "text/javascript",
}


class COOPCOEPMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Cross-Origin-Embedder-Policy"] = "require-corp"
        return response


worker_router = APIRouter()
_build_dir = Path("build/public").resolve()


@worker_router.get("/workers/{filename}")
async def serve_worker(filename: str) -> Response:
    file = _build_dir / filename
    if not file.is_file():
        return Response("not found", status_code=404)
    return Response(content=file.read_text(), media_type="text/javascript")


@worker_router.get("/duckdb/{filename}")
async def serve_duckdb_asset(filename: str) -> Response:
    # Served here, not via the general asset route, so the engine .wasm gets
    # `application/wasm` — WebAssembly.instantiateStreaming throws on octet-stream.
    file = _build_dir / "duckdb" / filename
    if not file.is_file():
        return Response("not found", status_code=404)
    media_type = MIME_TYPES.get(Path(filename).suffix, "application/octet-stream")
    return Response(content=file.read_bytes(), media_type=media_type)


def register_duckdb(app: FastAPI) -> None:
    app.add_middleware(COOPCOEPMiddleware)

    wasm_dir = _build_dir / "wasm"

    @app.get("/v{version}/{platform}/{filename}")
    async def serve_wasm_extension(version: str, platform: str, filename: str) -> Response:
        # Serve the exact platform requested. duckdb-wasm picks the platform from
        # its runtime (coi → wasm_threads); collapsing eh/threads to mvp hands the
        # threaded runtime a non-shared extension → "mismatch in shared state of
        # memory" LinkError on LOAD.
        file = wasm_dir / f"v{version}" / platform / filename
        if not file.is_file():
            available = next(wasm_dir.iterdir(), None)
            if available:
                file = available / platform / filename
        if not file.is_file():
            return Response("not found", status_code=404)
        suffix = file.suffix
        media_type = MIME_TYPES.get(suffix, "application/octet-stream")
        return Response(content=file.read_bytes(), media_type=media_type)

    app.include_router(worker_router, prefix=app.state.asset_prefix)

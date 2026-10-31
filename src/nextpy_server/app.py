import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import PlainTextResponse, Response

_log = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    _log.debug("nextpy lifespan: starting")
    boot_task = None
    if os.environ.get("N6K_DEV") == "1":
        from nextpy_server.dev import start_watcher, startup_rebuild

        # Build in the background so the port binds immediately; the watcher starts
        # only after the initial build so the two never race over build/public.
        async def _dev_boot() -> None:
            await startup_rebuild(app)
            await start_watcher()

        boot_task = asyncio.create_task(_dev_boot())

    _log.debug("nextpy lifespan: yielding")
    yield

    if boot_task:
        boot_task.cancel()


DEFAULT_ASSET_PREFIX = "/_nextpy"


def create_app(*, asset_prefix: str = DEFAULT_ASSET_PREFIX) -> FastAPI:
    from nextpy_server.logging import setup_logging

    setup_logging()

    application = FastAPI(lifespan=_lifespan)
    application.state.asset_prefix = asset_prefix
    application.state.pages = {}

    application.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r".*",
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )

    @application.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> PlainTextResponse:
        return PlainTextResponse(exc.detail, status_code=exc.status_code)

    @application.exception_handler(Exception)
    async def plain_text_exception_handler(request: Request, exc: Exception) -> PlainTextResponse:
        status = getattr(exc, "status_code", 500)
        return PlainTextResponse(str(exc), status_code=status)

    @application.middleware("http")
    async def request_log(request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        _log.info("%s %s %d", request.method, request.url.path, response.status_code)
        return response

    return application

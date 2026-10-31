import asyncio
import logging
import os
from pathlib import Path
from typing import AsyncIterator

from fastapi import APIRouter, FastAPI
from starlette.responses import StreamingResponse

from nextpy_server.build.run import build_css, copy_wasm, run_build, write_config

logger = logging.getLogger(__name__)

PROJECT_DIR = Path(".")

_DEV_SCRIPT_TEMPLATE = """\
<script>
(function() {{
  var es = new EventSource("{asset_prefix}/events");
  var wasConnected = false;
  es.addEventListener("reload", function() {{ location.reload(); }});
  es.onopen = function() {{
    if (wasConnected) {{ location.reload(); }}
    wasConnected = true;
  }};
}})();
</script>"""


def dev_script(asset_prefix: str) -> str:
    return _DEV_SCRIPT_TEMPLATE.format(asset_prefix=asset_prefix)


_sse_queues: set[asyncio.Queue[str]] = set()


def broadcast_reload() -> None:
    for queue in set(_sse_queues):
        queue.put_nowait("reload")


async def _sse_generator(queue: asyncio.Queue[str]) -> AsyncIterator[str]:
    _sse_queues.add(queue)
    try:
        while True:
            data = await queue.get()
            yield f"event: reload\ndata: {data}\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        _sse_queues.discard(queue)


dev_router = APIRouter()


@dev_router.get("/events")
async def sse_events() -> StreamingResponse:
    queue: asyncio.Queue[str] = asyncio.Queue()
    return StreamingResponse(
        _sse_generator(queue),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def startup_rebuild(app: FastAPI) -> None:
    from nextpy_server.config import get_src_dir

    # n6k lazy discovery: complete the manifest. Duck-typed so this package keeps no
    # n6k dependency; absent after eager discovery.
    prep = getattr(app.state, "_n6k_prepare_manifest", None)
    if prep is not None:
        await prep(app)

    src_dir = os.environ.get("N6K_SRC_DIR", str(get_src_dir(PROJECT_DIR)))
    app_dir = getattr(app.state, "_n6k_source_dir", None)
    entry_points = getattr(app.state, "_n6k_entry_points", None)

    config_fn = getattr(app.state, "_config_fn", None)

    prefix = app.state.asset_prefix
    logger.debug("Writing n6k.build.json...")
    await asyncio.to_thread(
        write_config,
        PROJECT_DIR,
        src_dir=src_dir,
        app_dir=app_dir,
        entry_points=entry_points,
        asset_prefix=prefix,
        scripts=[dev_script(prefix)],
        config_fn=config_fn,
        app=app,
    )

    logger.debug("Building CSS...")
    await asyncio.to_thread(build_css, PROJECT_DIR, src_dir=src_dir)

    logger.debug("Copying WASM extensions...")
    await asyncio.to_thread(copy_wasm, PROJECT_DIR)

    logger.debug("Building JS...")
    result = await asyncio.to_thread(run_build, PROJECT_DIR)
    if result.returncode != 0:
        logger.error(f"Startup build failed (rc={result.returncode}):\n{result.stderr}")
    else:
        logger.info("Startup build complete")
        # The port bound before this build ran, so nudge any browser that connected
        # during it to reload now that styles.css / pages/<slug>.js exist.
        broadcast_reload()


async def start_watcher() -> None:
    from watchfiles import awatch

    from nextpy_server.config import get_src_dir

    source_dir = os.environ.get("N6K_APP_DIR")
    source_path = Path(source_dir) if source_dir else None

    src_dir = Path(os.environ.get("N6K_SRC_DIR", str(get_src_dir(PROJECT_DIR))))
    watch_dirs = [src_dir / d for d in ("components", "lib")]
    watch_files = [src_dir / "app" / "globals.css"]

    if source_path:
        watch_dirs.append(source_path)

    watch_paths = [p for p in watch_dirs + watch_files if p.exists()]

    # Resolve symlinked children (e.g. demo/src/components/ui -> real/path) so edits
    # through them are detected.
    for d in watch_dirs:
        if not d.exists():
            continue
        for child in d.iterdir():
            if child.is_symlink():
                target = child.resolve()
                if target.is_dir() and target not in watch_paths:
                    watch_paths.append(target)

    extra = os.environ.get("N6K_EXTRA_WATCH_DIRS", "")
    if extra:
        for p in extra.split(":"):
            ep = Path(p).resolve()
            if ep.exists() and ep.is_dir():
                watch_paths.append(ep)

    if not watch_paths:
        logger.warning("No watch paths found, file watcher not started")
        return

    logger.info(f"Watching: {[str(p) for p in watch_paths]}")

    try:
        async for changes in awatch(*watch_paths):
            # .py is uvicorn's; .pyi stubs are written into the watched app dir by
            # every rebuild, so watching them would loop.
            non_py_changes = [
                (change_type, path) for change_type, path in changes if not path.endswith((".py", ".pyi"))
            ]
            if not non_py_changes:
                continue

            changed_files = [str(path) for _, path in non_py_changes]
            logger.info(f"File change detected: {changed_files}")

            try:
                logger.debug("Rebuilding CSS...")
                await asyncio.to_thread(build_css, PROJECT_DIR)
                logger.debug("Rebuilding JS...")
                result = await asyncio.to_thread(run_build, PROJECT_DIR)
                if result.returncode != 0:
                    logger.error(f"Build failed (rc={result.returncode}):\n{result.stderr}")
                    continue
                n_clients = len(_sse_queues)
                logger.info(f"Build succeeded, sending reload to {n_clients} client(s)")
                broadcast_reload()
            except Exception as e:
                logger.error(f"Rebuild error: {e}")
    except asyncio.CancelledError:
        pass


def register_dev(app: FastAPI) -> None:
    app.include_router(dev_router, prefix=app.state.asset_prefix)

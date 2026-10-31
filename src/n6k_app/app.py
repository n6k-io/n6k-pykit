from typing import Any

from fastapi import FastAPI

from nextpy_server.app import create_app as _base_create_app

from n6k_app.duckdb import register_duckdb
from n6k_app.info import register_info
from n6k_app.page_ws import register_page_mux_ws


def create_app(**kwargs: Any) -> FastAPI:
    app = _base_create_app(**kwargs)

    register_duckdb(app)
    register_info(app)
    register_page_mux_ws(app)

    from n6k_app.build.run import _config_fn

    app.state._config_fn = _config_fn

    return app

from pathlib import Path
from typing import AsyncIterator
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from nextpy_server.app import create_app
from nextpy_server.pages import page, wrap_html


class TestWrapHtml:
    def test_produces_valid_html_document(self) -> None:
        html = wrap_html("/_nextpy", "/_nextpy/pages/index.js")
        assert "<!DOCTYPE html>" in html
        assert '<div id="root"></div>' in html
        assert '<link rel="stylesheet" href="/_nextpy/styles.css">' in html
        assert '<script type="module" src="/_nextpy/pages/index.js"></script>' in html


class TestPageRoute:
    @pytest.fixture
    def app_with_page(self) -> FastAPI:
        application = create_app()
        page(application, "/test-page", "<div>Test</div>")
        return application

    @pytest.fixture
    def transport(self, app_with_page: FastAPI) -> ASGITransport:
        return ASGITransport(app=app_with_page)

    @pytest.fixture
    async def client(self, transport: ASGITransport) -> AsyncIterator[AsyncClient]:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

    async def test_returns_html(self, client: AsyncClient) -> None:
        resp = await client.get("/test-page")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    async def test_html_references_script(self, client: AsyncClient, app_with_page: FastAPI) -> None:
        prefix = app_with_page.state.asset_prefix
        resp = await client.get("/test-page")
        assert f'src="{prefix}/pages/test-page.js"' in resp.text

    async def test_css_route_registered(self, tmp_path: Path) -> None:
        # PUBLIC_DIR is derived from BUILD_DIR at import time, so patch both
        public = tmp_path / "public"
        public.mkdir()
        css_file = public / "styles.css"
        css_file.write_text("body{}")
        with patch("nextpy_server.pages.BUILD_DIR", tmp_path), patch("nextpy_server.pages.PUBLIC_DIR", public):
            application = create_app()
            page(application, "/css-test", "<div>CSS</div>")
            prefix = application.state.asset_prefix
            transport = ASGITransport(app=application)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(f"{prefix}/styles.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers["content-type"]


class TestPageRegistration:
    def test_stores_snippet_in_state(self) -> None:
        application = create_app()
        page(application, "/reg-test", "<div>Hello</div>")
        assert application.state.pages["reg-test"] == "<div>Hello</div>"

    def test_root_uses_index_slug(self) -> None:
        application = create_app()
        page(application, "/", "<div>Root</div>")
        assert "index" in application.state.pages

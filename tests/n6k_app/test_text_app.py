"""A page socket's text frames over a real handshake: JSON-RPC ``render`` and
``@action``, the actions also as MCP tools, the MCP-only ``show_page``, with binary
n6k on the same socket."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any, Iterator

import pytest
from fastapi import FastAPI
from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_REQ,
    FT_RESP_END,
    N6K_PROTOCOL_VERSION,
    OP_QUERY,
)
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from n6k_app import Database, Session, action
from n6k_app.discover import discover_app
from n6k_app.text_app import build_text_app

_DATA_PY = """
import pyarrow as pa
from n6k_app import Database, Session, action
from n6k_server.jsonrpc import JsonRpcError

db = Database()
seen = []


@action
def add(a: int, b: int, session: Session) -> int:
    \"\"\"Add two numbers.\"\"\"
    seen.append(session)
    return a + b


@action("double")
async def twice(n: int) -> int:
    return n * 2


@action()
def refuse() -> None:
    raise JsonRpcError(4001, "nope")


@db.rpc(returns=pa.schema([("x", pa.int64())]))
def probe(session: Session) -> pa.Table:
    seen.append(session)
    return pa.table({"x": [1]})
"""

_PAGE_PY = """
from n6k_app.frontend import Components, page


@page
def home(c: Components):
    c.h1("hello")
"""

_BAD_PAGE_PY = """
from n6k_app.frontend import Components, page


@page
def bad(c: Components):
    raise RuntimeError("kaboom")
"""

_PAGE_MDX = "# Sales\n\nThe quarter so far.\n"

_PAGE_TSX = "export default function Page() {\n  return <h1>hi</h1>;\n}\n"


def _extensions_loadable() -> bool:
    import duckdb
    from n6k_server.extension import load_n6k_server, load_virtual_catalog_provider

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        load_n6k_server(con)
        load_virtual_catalog_provider(con)
    except Exception:
        return False
    finally:
        con.close()
    return True


pytestmark = pytest.mark.skipif(not _extensions_loadable(), reason="needs the n6k_server extension")


@pytest.fixture
def app_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> Iterator[Path]:
    pkg = "textapp" + "".join(ch for ch in request.node.name if ch.isalnum())
    path = tmp_path / pkg
    path.mkdir()
    (path / "data.py").write_text(_DATA_PY)
    (path / "page.py").write_text(_PAGE_PY)
    (path / "bad").mkdir()
    (path / "bad" / "page.py").write_text(_BAD_PAGE_PY)
    (path / "dataonly").mkdir()
    (path / "dataonly" / "data.py").write_text("from n6k_app import Database\n\ndb = Database()\n")
    (path / "mdx").mkdir()
    (path / "mdx" / "page.mdx").write_text(_PAGE_MDX)
    (path / "tsxonly").mkdir()
    (path / "tsxonly" / "page.tsx").write_text(_PAGE_TSX)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("N6K_SRC_DIR", str(tmp_path))
    monkeypatch.setenv("N6K_APP_DIR", str(path))
    yield path
    for mod in [m for m in sys.modules if m == pkg or m.startswith(pkg + ".")]:
        del sys.modules[mod]


@pytest.fixture
def app(app_dir: Path) -> FastAPI:
    return discover_app(app_dir, eager=True)


def _hello_frame() -> bytes:
    return pack_frame(
        {
            "t": FT_HELLO,
            "token": "x",
            "protocol_version": N6K_PROTOCOL_VERSION,
            "capabilities": ["crossing", "jsonrpc", "mcp"],
        }
    )


def _hello(ws: Any) -> list[str]:
    ws.send_bytes(_hello_frame())
    header, _ = unpack_frame(ws.receive_bytes())
    assert header["t"] == FT_HELLO_ACK
    return list(header["capabilities"])


@contextmanager
def _socket(app: FastAPI, path: str, *, hello: bool = True) -> Iterator[Any]:
    """A logged-in socket, ended by a second HELLO (a protocol error) so the serve
    unwinds before the test does."""
    with TestClient(app).websocket_connect(path) as ws:
        if hello:
            _hello(ws)
        yield ws
        ws.send_bytes(_hello_frame())
        with pytest.raises(WebSocketDisconnect):
            while True:
                ws.receive_bytes()


def _recv_text(ws: Any) -> dict[str, Any]:
    while True:
        message = ws.receive()
        if message.get("text") is not None:
            return dict(json.loads(message["text"]))


def _call(ws: Any, req_id: int, method: str, params: Any = None) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        message["params"] = params
    ws.send_text(json.dumps(message))
    reply = _recv_text(ws)
    assert reply["id"] == req_id, reply
    return reply


def _query(ws: Any, req_id: int, sql: str) -> dict[str, Any]:
    ws.send_bytes(pack_frame({"t": FT_REQ, "id": req_id, "ns": 1, "op": OP_QUERY, "sql": sql}))
    while True:
        header, _ = unpack_frame(ws.receive_bytes())
        if header["t"] in (FT_RESP_END, FT_ERR) and header.get("id") == req_id:
            return dict(header)


def test_hello_grants_jsonrpc_and_mcp(app: FastAPI) -> None:
    with _socket(app, "/ws", hello=False) as ws:
        assert {"jsonrpc", "mcp"} <= set(_hello(ws))


def test_render_returns_the_tree_only_when_asked(app: FastAPI) -> None:
    with _socket(app, "/ws") as ws:
        tree = _call(ws, 1, "render", {"state": {}})["result"]
        assert tree["children"][0]["component"] == "h1"


def test_render_error_is_a_fragment(app: FastAPI) -> None:
    with _socket(app, "/bad/ws") as ws:
        tree = _call(ws, 1, "render", {"state": {}})["result"]
        assert tree["component"] == "fragment"
        assert "kaboom" in tree["children"][0]["children"][0]


def test_no_render_without_page_py(app: FastAPI) -> None:
    with _socket(app, "/dataonly/ws") as ws:
        assert _call(ws, 1, "render", {"state": {}})["error"]["code"] == -32601


def test_actions_answer_over_jsonrpc(app: FastAPI) -> None:
    with _socket(app, "/ws") as ws:
        assert _call(ws, 1, "add", {"a": 2, "b": 3})["result"] == 5
        assert _call(ws, 2, "double", {"n": 4})["result"] == 8
        assert _call(ws, 3, "refuse")["error"] == {"code": 4001, "message": "nope"}
        assert _call(ws, 4, "add", {"a": 1, "b": 2, "session": None})["error"]["code"] == -32602


def test_an_action_and_an_rpc_share_the_connection_session(app: FastAPI, app_dir: Path) -> None:
    seen = sys.modules[f"{app_dir.name}.data"].seen
    with _socket(app, "/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
        _call(ws, 1, "add", {"a": 1, "b": 1})
        assert _query(ws, 7, "SELECT * FROM db.probe()")["t"] == FT_RESP_END
    assert len(seen) == 2 and seen[0] is seen[1]
    assert seen[0].catalog == "db"


def _mcp_init(ws: Any) -> None:
    _call(
        ws,
        1,
        "initialize",
        {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
    )
    ws.send_text(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}))


def _tool_names(ws: Any) -> set[str]:
    return {t["name"] for t in _call(ws, 2, "tools/list", {})["result"]["tools"]}


def _show_page(ws: Any, arguments: dict[str, Any]) -> dict[str, Any]:
    result = _call(ws, 3, "tools/call", {"name": "show_page", "arguments": arguments})["result"]
    assert not result.get("isError"), result
    return dict(json.loads(result["content"][0]["text"]))


def test_actions_are_mcp_tools_without_injected_params(app: FastAPI) -> None:
    with _socket(app, "/ws") as ws:
        _mcp_init(ws)
        tools = {t["name"]: t for t in _call(ws, 2, "tools/list", {})["result"]["tools"]}
        assert set(tools) == {"add", "double", "refuse", "show_page"}
        assert set(tools["add"]["inputSchema"]["properties"]) == {"a", "b"}
        assert tools["add"]["description"] == "Add two numbers."
        result = _call(ws, 3, "tools/call", {"name": "add", "arguments": {"a": 2, "b": 5}})["result"]
        assert result["content"][0]["text"] == "7"


def test_show_page_returns_a_py_pages_tree(app: FastAPI) -> None:
    with _socket(app, "/ws") as ws:
        _mcp_init(ws)
        shown = _show_page(ws, {"state": {}})
        assert shown["type"] == "py"
        assert shown["tree"]["children"][0]["component"] == "h1"
        assert _show_page(ws, {})["tree"] == shown["tree"]


def test_show_page_error_is_a_fragment(app: FastAPI) -> None:
    with _socket(app, "/bad/ws") as ws:
        _mcp_init(ws)
        tree = _show_page(ws, {})["tree"]
        assert tree["component"] == "fragment"
        assert "kaboom" in tree["children"][0]["children"][0]


def test_show_page_returns_an_mdx_pages_source(app: FastAPI) -> None:
    with _socket(app, "/mdx/ws") as ws:
        _mcp_init(ws)
        assert _show_page(ws, {"state": {"x": 1}}) == {"type": "mdx", "source": _PAGE_MDX}


def test_a_tsx_only_page_serves_show_page(app: FastAPI) -> None:
    with _socket(app, "/tsxonly/ws") as ws:
        _mcp_init(ws)
        assert _tool_names(ws) == {"show_page"}
        assert _show_page(ws, {}) == {"type": "tsx", "source": _PAGE_TSX}
        assert _call(ws, 4, "render", {"state": {}})["error"]["code"] == -32601


def test_no_show_page_without_a_page_file(app: FastAPI) -> None:
    with _socket(app, "/dataonly/ws") as ws:
        _mcp_init(ws)
        assert "show_page" not in _tool_names(ws)


def test_show_page_is_not_a_jsonrpc_method(app: FastAPI) -> None:
    with _socket(app, "/ws") as ws:
        assert _call(ws, 1, "show_page", {"state": {}})["error"]["code"] == -32601


def test_text_before_login_is_dropped(app: FastAPI) -> None:
    with _socket(app, "/ws", hello=False) as ws:
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "add", "params": {"a": 1, "b": 1}}))
        _hello(ws)
        assert _call(ws, 2, "add", {"a": 1, "b": 2})["result"] == 3


def test_a_page_with_neither_file_closes_4404(app: FastAPI) -> None:
    with TestClient(app).websocket_connect("/nothing/ws") as ws:
        with pytest.raises(WebSocketDisconnect) as excinfo:
            ws.receive_bytes()
    assert excinfo.value.code == 4404


def _go() -> None:
    pass


def _other_go() -> None:
    pass


def test_an_action_name_shared_by_two_actions_is_refused() -> None:
    with pytest.raises(ValueError, match=r"'go' by \['_go', '_other_go'\]"):
        build_text_app("p", {}, [action("go")(_go), action("go")(_other_go)], {}, None)


def test_an_action_named_render_is_refused_on_a_page() -> None:
    actions = [action("render")(_go)]
    with pytest.raises(ValueError, match="render"):
        build_text_app("p", {}, actions, {}, lambda state: {})
    build_text_app("p", {}, actions, {}, None)


def test_an_action_named_show_page_is_refused_on_a_page() -> None:
    actions = [action("show_page")(_go)]
    with pytest.raises(ValueError, match="show_page"):
        build_text_app("p", {}, actions, {}, None, lambda state: {})
    build_text_app("p", {}, actions, {}, None)


def _session_only(session: Session) -> None:
    pass


def test_a_bare_session_needs_exactly_one_database() -> None:
    dbs = {"a": Database(), "b": Database()}
    with pytest.raises(TypeError, match="Annotated"):
        build_text_app("p", dbs, [action(_session_only)], {}, None)
    build_text_app("p", {"a": dbs["a"]}, [action(_session_only)], {}, None)


_ELSEWHERE = Database()


def _other_db(session: Annotated[Session, _ELSEWHERE]) -> None:
    pass


def test_an_action_for_a_database_the_page_does_not_serve_is_refused() -> None:
    with pytest.raises(TypeError, match="does not serve"):
        build_text_app("p", {"a": Database()}, [action(_other_db)], {}, None)

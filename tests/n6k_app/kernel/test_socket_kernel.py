"""``n6k kernel`` over a real Unix socket: exec as JSON-RPC and the ``kernel`` catalog
as n6k frames on one connection, and the namespace surviving a reconnect."""

import asyncio
import io
import json
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from jsonschema import Draft202012Validator

from n6k_protocol.protocol import (
    FT_OPEN,
    FT_OPEN_ACK,
    FT_PUSH,
    FT_RESP_END,
    OP_CATALOG_INVALIDATED,
)
from n6k_app.kernel.capture import Capture
from n6k_app.kernel.kernel import Kernel
from n6k_app.kernel.socket_kernel import listen
from tests.n6k_app.test_socket_app import _ClientConn, _dial, _extensions_loadable, _scan_req

pytestmark = pytest.mark.skipif(
    not _extensions_loadable(),
    reason="virtual_catalog/n6k_server could not be loaded; set N6K_EXTENSION_DIR to a local build",
)


def _kind(msg: dict[str, Any]) -> str | None:
    return msg["params"]["state"] if msg.get("method") == "process.status" else msg.get("method")


async def _notifications(client: _ClientConn, until: str, cell_id: str | None = None) -> list[dict[str, Any]]:
    out = []
    while True:
        msg = json.loads(await client.await_text())
        out.append(msg)
        if _kind(msg) == until and (cell_id is None or msg["params"].get("cell_id") == cell_id):
            return out


async def _execute(client: _ClientConn, cell_id: str, code: str) -> list[dict[str, Any]]:
    await client.send_text(
        json.dumps({"jsonrpc": "2.0", "method": "process.execute", "params": {"cell_id": cell_id, "code": code}})
    )
    return await _notifications(client, "completed", cell_id)


async def test_exec_and_catalog_share_one_connection_and_survive_reconnect() -> None:
    kernel = Kernel()
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    task = asyncio.create_task(listen(kernel, path))
    try:
        client = await _dial(path)
        started = json.loads(await client.await_text())
        assert started["method"] == "process.ready" and started["params"]["language"] == "python"

        msgs = await _execute(client, "c1", "import pandas as pd\ndf = pd.DataFrame({'a': [1, 2, 3]})\nlen(df)")
        assert [_kind(m) for m in msgs] == ["queued", "running", "process.display", "completed"]
        assert msgs[2]["params"]["data"] == "3"
        assert msgs[-1]["params"] == {
            "cell_id": "c1",
            "state": "completed",
            "success": True,
            "interrupted": False,
            "added_vars": ["df", "pd"],
            "seq": 4,
        }

        await client.send({"t": FT_OPEN, "ns": 1, "catalog": "kernel"})
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await client.send(_scan_req(1, 1, "kernel", "df"))
        await client.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 1)
        assert client.arrow_rows(req_id=1, ns=1) == 3

        await _execute(client, "c1b", "df2 = df")
        await client.await_frame(lambda h: h["t"] == FT_PUSH and h.get("op") == OP_CATALOG_INVALIDATED)

        await client.close()
        client = await _dial(path)
        assert json.loads(await client.await_text())["method"] == "process.ready"
        msgs = await _execute(client, "c2", "len(df)")
        assert msgs[2]["params"]["data"] == "3"
        await client.close()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_serves_a_custom_catalog_name() -> None:
    kernel = Kernel("nb1")
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    task = asyncio.create_task(listen(kernel, path))
    try:
        client = await _dial(path)
        await client.await_text()
        await _execute(client, "c1", "import pandas as pd\ndf = pd.DataFrame({'a': [1, 2]})")

        await client.send({"t": FT_OPEN, "ns": 1, "catalog": "nb1"})
        await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)
        await client.send(_scan_req(1, 1, "nb1", "df"))
        await client.await_frame(lambda h: h["t"] == FT_RESP_END and h.get("ns") == 1)
        assert client.arrow_rows(req_id=1, ns=1) == 2

        await _execute(client, "c2", "df2 = df")
        await client.await_frame(lambda h: h["t"] == FT_PUSH and h.get("op") == OP_CATALOG_INVALIDATED)
        await client.close()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_interrupt_stops_a_running_cell() -> None:
    kernel = Kernel()
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    task = asyncio.create_task(listen(kernel, path))
    try:
        client = await _dial(path)
        await client.await_text()
        await client.send_text(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "process.execute",
                    "params": {"cell_id": "loop", "code": "print('go')\nwhile True:\n    pass"},
                }
            )
        )
        await _notifications(client, "running", "loop")
        await asyncio.sleep(0.2)
        await client.send_text(
            json.dumps({"jsonrpc": "2.0", "method": "process.interrupt", "params": {"cell_id": "loop"}})
        )
        msgs = await _notifications(client, "completed", "loop")
        assert msgs[-1]["params"]["success"] is False and msgs[-1]["params"]["interrupted"] is True
        assert any("KeyboardInterrupt" in m["params"].get("data", "") for m in msgs)
        await client.close()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


SCHEMA = json.loads((Path(__file__).parents[3] / "schemas" / "kernel-rpc.json").read_text())


def _validator(name: str) -> Draft202012Validator:
    return Draft202012Validator({**SCHEMA, "$ref": f"#/$defs/{name}"})


def _has(received: list[dict[str, Any]], kind: str, cell_id: str | None = None) -> bool:
    return any(_kind(m) == kind and cell_id in (None, m["params"]["cell_id"]) for m in received)


class TestRpcSchemaLive:
    """Every text frame of a real session validates against the kernel RPC schema."""

    async def test_session(self) -> None:
        s2c, c2s = _validator("serverToClient"), _validator("clientToServer")
        kernel = Kernel()
        path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
        task = asyncio.create_task(listen(kernel, path))
        received: list[dict[str, Any]] = []
        try:
            client = await _dial(path)

            async def send(msg: dict[str, Any]) -> None:
                c2s.validate(msg)
                await client.send_text(json.dumps(msg))

            async def until(done: Any) -> None:
                while not done():
                    msg = json.loads(await client.await_text())
                    s2c.validate(msg)
                    received.append(msg)

            def replied(req_id: Any) -> bool:
                return any("method" not in m and m["id"] == req_id for m in received)

            def completed(cell_id: str) -> bool:
                return _has(received, "completed", cell_id)

            await until(lambda: received)

            code = (
                "import sys\n"
                "print('out')\n"
                "print('err', file=sys.stderr)\n"
                "display({'a': [1, None]}, mime='application/json', title='t', icon='i')\n"
                "d = display('a')\n"
                "display('b', displayId=d, method='append')\n"
                "x = 1\n"
                "x"
            )
            await send(
                {"jsonrpc": "2.0", "id": 1, "method": "process.execute", "params": {"cell_id": "c1", "code": code}}
            )
            await until(lambda: replied(1) and completed("c1"))

            await send({"jsonrpc": "2.0", "id": "two", "method": "process.execute", "params": ["c2", "1/0", "main"]})
            await until(lambda: replied("two") and completed("c2"))

            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "process.execute",
                    "params": ["c3", "while True:\n    pass", "t2"],
                }
            )
            await until(lambda: _has(received, "running", "c3"))
            await asyncio.sleep(0.2)
            await send({"jsonrpc": "2.0", "id": 17, "method": "process.execute", "params": ["c3", "1", "main"]})
            await until(lambda: replied(17))
            await send({"jsonrpc": "2.0", "id": 4, "method": "process.interrupt", "params": {"cell_id": "c3"}})
            await until(lambda: replied(3) and replied(4) and completed("c3"))

            await send({"jsonrpc": "2.0", "id": 5, "method": "process.interrupt", "params": {"cell_id": "c1"}})
            await send({"jsonrpc": "2.0", "id": 6, "method": "process.interrupt", "params": ["nope"]})
            await client.send_text(json.dumps({"jsonrpc": "2.0", "id": 7, "method": "process.interrupt"}))
            await until(lambda: replied(5) and replied(6) and replied(7))

            await send({"jsonrpc": "2.0", "id": 11, "method": "process.cells"})
            await send({"jsonrpc": "2.0", "id": 12, "method": "process.cell_state", "params": {"cell_id": "c1"}})
            await send({"jsonrpc": "2.0", "id": 13, "method": "process.cell_state", "params": ["c2"]})
            await send({"jsonrpc": "2.0", "id": 14, "method": "process.forget", "params": {"cell_id": "c3"}})
            await send({"jsonrpc": "2.0", "id": 15, "method": "process.cell_state", "params": ["c3"]})
            await send({"jsonrpc": "2.0", "id": 16, "method": "process.cells", "params": {}})
            await until(lambda: all(replied(i) for i in range(11, 17)) and _has(received, "process.forgotten"))

            await client.send_text(json.dumps({"jsonrpc": "2.0", "id": 8, "method": "nope"}))
            await client.send_text(
                json.dumps({"jsonrpc": "2.0", "id": 9, "method": "process.execute", "params": {"cell_id": "c"}})
            )
            await client.send_text(json.dumps({"jsonrpc": "1.0", "id": 10, "method": "process.execute"}))
            await client.send_text("not json")
            await client.send_text("[]")
            await until(
                lambda: replied(8) and replied(9) and replied(10) and sum(m.get("id", 0) is None for m in received) == 2
            )
            await client.close()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        kinds = {_kind(m) or ("error" if "error" in m else "result") for m in received}
        assert kinds == {
            "process.ready",
            "queued",
            "running",
            "process.stream",
            "process.display",
            "completed",
            "process.forgotten",
            "result",
            "error",
        }
        codes = {m["error"]["code"] for m in received if "error" in m}
        assert codes == {-32601, -32602, -32600, -32700, -32001}
        assert [(m["params"]["success"], m["params"]["interrupted"]) for m in received if _kind(m) == "completed"] == [
            (True, False),
            (False, False),
            (False, True),
        ]
        replies = {m["id"]: m for m in received if "method" not in m}
        for req_id, cell_id in ((1, "c1"), ("two", "c2"), (3, "c3")):
            done = next(
                i for i, m in enumerate(received) if _kind(m) == "completed" and m["params"]["cell_id"] == cell_id
            )
            assert received.index(replies[req_id]) > done
        assert replies[1]["result"] == {"success": True, "interrupted": False}
        assert replies["two"]["result"] == {"success": False, "interrupted": False}
        assert replies[3]["result"] == {"success": False, "interrupted": True}
        assert replies[17]["error"]["code"] == -32001
        assert replies[4]["result"] is None and replies[5]["result"] is None
        assert replies[6]["error"]["code"] == replies[7]["error"]["code"] == -32602
        assert [c["cell_id"] for c in replies[11]["result"]["cells"]] == ["c1", "c2", "c3"]
        c1 = replies[12]["result"]["cell"]
        assert c1["state"] == "completed" and c1["success"] is True and c1["code"] == code
        assert [o["type"] for o in c1["outputs"]] == ["display", "display", "display"]
        assert c1["outputs"][0]["parts"] == [{"mime": "application/json", "data": {"a": [1, None]}}]
        assert c1["outputs"][1]["parts"] == [
            {"mime": "text/plain", "data": "'a'"},
            {"mime": "text/plain", "data": "'b'"},
        ]
        assert "ZeroDivisionError" in replies[13]["result"]["cell"]["outputs"][0]["data"]
        assert replies[14]["result"] is None
        assert replies[15]["error"]["code"] == -32602
        assert [c["cell_id"] for c in replies[16]["result"]["cells"]] == ["c1", "c2"]
        seqs = [m["params"]["seq"] for m in received if "seq" in m.get("params", {})]
        assert seqs == list(range(1, len(seqs) + 1))


async def test_listen_broadcasts_output_and_shares_the_namespace() -> None:
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    kernel = Kernel()
    task = asyncio.create_task(listen(kernel, path))
    try:
        a = await _dial(path)
        b = await _dial(path)
        assert json.loads(await a.await_text())["method"] == "process.ready"
        assert json.loads(await b.await_text())["method"] == "process.ready"

        for client in (a, b):
            await client.send({"t": FT_OPEN, "ns": 1, "catalog": "kernel"})
            await client.await_frame(lambda h: h["t"] == FT_OPEN_ACK and h.get("ns") == 1)

        msgs = await _execute(a, "c1", "x = 41\nx + 1")
        assert msgs[2]["params"]["data"] == "42"
        seen_by_b = await _notifications(b, "completed", "c1")
        assert seen_by_b == msgs
        for client in (a, b):
            await client.await_frame(lambda h: h["t"] == FT_PUSH and h.get("op") == OP_CATALOG_INVALIDATED)

        await a.close()
        msgs = await _execute(b, "c2", "x")
        assert msgs[2]["params"]["data"] == "41"
        await b.close()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@asynccontextmanager
async def _listening(kernel: Kernel) -> AsyncIterator[Callable[[], Awaitable[_ClientConn]]]:
    """Yields a connect function; its clients are closed on exit, which ``listen`` waits for."""
    path = f"/tmp/n6k-{uuid.uuid4().hex[:8]}.sock"
    task = asyncio.create_task(listen(kernel, path))
    clients: list[_ClientConn] = []

    async def connect() -> _ClientConn:
        client = await _dial(path)
        clients.append(client)
        assert json.loads(await client.await_text())["method"] == "process.ready"
        return client

    try:
        yield connect
    finally:
        for client in clients:
            await client.close()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def _call(
    client: _ClientConn, req_id: int, method: str, params: Any = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The reply to one request, and the notifications received before it."""
    msg: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        msg["params"] = params
    await client.send_text(json.dumps(msg))
    seen = []
    while True:
        reply = json.loads(await client.await_text())
        if "method" not in reply and reply["id"] == req_id:
            return reply, seen
        seen.append(reply)


def _capturing_kernel() -> Kernel:
    kernel = Kernel()
    kernel.global_vars["out"] = Capture("stdout", io.StringIO())
    kernel.global_vars["stop"] = threading.Event()
    return kernel


async def test_late_client_catches_up_and_forget_reaches_everyone() -> None:
    kernel = Kernel()
    async with _listening(kernel) as connect:
        a = await connect()
        msgs = await _execute(
            a, "c1", "display('x', displayId='d')\ndisplay('y', displayId='d', method='append')\n{1, 2}"
        )
        live_set = msgs[-2]["params"]
        assert live_set["mime"] == "text/plain" and live_set["data"] == "{1, 2}"

        b = await connect()
        listed, _ = await _call(b, 1, "process.cells")
        assert listed["result"]["cells"] == [
            {"cell_id": "c1", "thread": "main", "state": "completed", "success": True, "truncated": False}
        ]
        state, _ = await _call(b, 2, "process.cell_state", {"cell_id": "c1"})
        snap = state["result"]["seq"]
        assert state["result"]["cell"]["outputs"] == [
            {
                "type": "display",
                "displayId": "d",
                "parts": [{"mime": "text/plain", "data": "'x'"}, {"mime": "text/plain", "data": "'y'"}],
            },
            {
                "type": "display",
                "displayId": live_set["displayId"],
                "parts": [{"mime": "text/plain", "data": "{1, 2}"}],
                "title": "{1, 2}",
            },
        ]

        await _execute(a, "c2", "1;")
        seen = await _notifications(b, "completed", "c2")
        assert _kind(seen[0]) == "queued" and seen[0]["params"]["code"] == "1;"
        assert all(m["params"]["seq"] > snap for m in seen)

        reply, _ = await _call(b, 3, "process.forget", ["c1"])
        assert reply["result"] is None
        forgotten = (await _notifications(a, "process.forgotten"))[-1]
        assert forgotten["params"]["cell_id"] == "c1"
        listed, _ = await _call(a, 4, "process.cells")
        assert [c["cell_id"] for c in listed["result"]["cells"]] == ["c2"]


def _stream_text(cell: dict[str, Any], live: list[dict[str, Any]], cell_id: str, after: int) -> str:
    stored = "".join(o["data"] for o in cell["outputs"] if o["type"] == "stream")
    return stored + "".join(
        m["params"]["data"]
        for m in live
        if m.get("method") == "process.stream" and m["params"]["cell_id"] == cell_id and m["params"]["seq"] > after
    )


async def test_cell_state_plus_later_notifications_is_the_whole_stream() -> None:
    kernel = _capturing_kernel()
    code = (
        "import time\n{0} = 0\n"
        "while not stop.is_set():\n    print({0}, file=out)\n    {0} += 1\n    time.sleep(0.001)\n"
    )
    async with _listening(kernel) as connect:
        a = await connect()
        for cell_id, thread in (("p1", "t1"), ("p2", "t2")):
            await a.send_text(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "process.execute",
                        "params": {"cell_id": cell_id, "code": code.format(f"i_{cell_id}"), "thread": thread},
                    }
                )
            )
        received = await _notifications(a, "running", "p1")
        if not _has(received, "running", "p2"):
            received += await _notifications(a, "running", "p2")
        await asyncio.sleep(0.05)

        r1, seen = await _call(a, 1, "process.cell_state", ["p1"])
        received += seen
        r2, seen = await _call(a, 2, "process.cell_state", ["p2"])
        received += seen
        refused, seen = await _call(a, 3, "process.forget", ["p1"])
        received += seen
        assert refused["error"]["code"] == -32602
        assert r1["result"]["cell"]["state"] == r2["result"]["cell"]["state"] == "running"

        kernel.global_vars["stop"].set()
        while not (_has(received, "completed", "p1") and _has(received, "completed", "p2")):
            received.append(json.loads(await a.await_text()))

        for cell_id, reply in (("p1", r1), ("p2", r2)):
            text = _stream_text(reply["result"]["cell"], received, cell_id, reply["result"]["seq"])
            count = text.count("\n")
            assert count > 0 and text == "".join(f"{i}\n" for i in range(count))
            state, _ = await _call(a, 4, "process.cell_state", [cell_id])
            assert _stream_text(state["result"]["cell"], [], cell_id, 0) == text


async def test_reexecuting_a_running_cell_is_refused_and_leaves_it_running() -> None:
    kernel = _capturing_kernel()
    old = "import time\nwhile not stop.is_set():\n    print('old', file=out)\n    time.sleep(0.001)\n"
    async with _listening(kernel) as connect:
        a = await connect()
        await a.send_text(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "process.execute",
                    "params": {"cell_id": "r", "code": old, "thread": "t1"},
                }
            )
        )
        await _notifications(a, "process.stream", "r")
        refused, _ = await _call(a, 2, "process.execute", {"cell_id": "r", "code": "1", "thread": "t2"})
        assert refused["error"]["code"] == -32001

        kernel.global_vars["stop"].set()
        while True:
            reply = json.loads(await a.await_text())
            if "method" not in reply:
                break
        assert reply == {"jsonrpc": "2.0", "id": 1, "result": {"success": True, "interrupted": False}}
        state, _ = await _call(a, 3, "process.cell_state", ["r"])
        cell = state["result"]["cell"]
        assert cell["state"] == "completed" and cell["success"] is True and cell["code"] == old


async def test_interrupting_a_queued_cell_skips_it() -> None:
    kernel = _capturing_kernel()
    async with _listening(kernel) as connect:
        a = await connect()
        await a.send_text(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "process.execute",
                    "params": {"cell_id": "first", "code": "stop.wait(10);"},
                }
            )
        )
        await _notifications(a, "running", "first")
        await a.send_text(
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "process.execute", "params": ["queued", "x = 1"]})
        )
        await _notifications(a, "queued", "queued")
        interrupted, _ = await _call(a, 2, "process.interrupt", ["queued"])
        assert interrupted["result"] is None
        kernel.global_vars["stop"].set()
        seen = []
        while True:
            msg = json.loads(await a.await_text())
            if "method" not in msg:
                break
            seen.append(msg)
        assert msg == {"jsonrpc": "2.0", "id": 1, "result": {"success": False, "interrupted": True}}
        assert not _has(seen, "running", "queued")
        assert "x" not in kernel.local_vars

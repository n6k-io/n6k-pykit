import asyncio
import os
import sys
import threading

import pandas as pd
import pyarrow as pa
import pytest

from virtual_catalog.provider import TableNotFound

from tests.n6k_app.test_socket_app import _extensions_loadable
from n6k_app.kernel.catalog import (
    CatalogInvalidator,
    DataFrameProvider,
    MetadataProvider,
    Namespaces,
    _to_arrow,
    open_catalog,
    rpc_registry,
)


def _main(*namespaces):
    return DataFrameProvider(Namespaces(*namespaces))


def _meta(*namespaces):
    return MetadataProvider(Namespaces(*namespaces))


def _scan_rows(provider, table, columns=None, filters=None):
    out = asyncio.run(provider.scan(table, columns, filters))
    return out.schema, out.to_pylist()


def test_to_arrow_preserves_named_column():
    # pd.DataFrame(range(10), columns=['a']) must surface as column
    # 'a' over the data API, not the positional "0" pyarrow assigns when a
    # DataFrame carries integer column labels.
    df = pd.DataFrame(range(10), columns=["a"])
    assert _to_arrow(df).schema.names == ["a"]


def test_schema_reports_user_column_names():
    df = pd.DataFrame(range(10), columns=["a"])
    schema = asyncio.run(_main({"df": df}).schema("df"))
    assert schema.names == ["a"]


def test_scan_preserves_column_names_and_rows():
    df = pd.DataFrame(range(10), columns=["a"])
    schema, rows = _scan_rows(_main({"df": df}), "df")
    assert schema.names == ["a"]
    assert [r["a"] for r in rows] == list(range(10))


def test_scan_applies_filter_and_projection():
    df = pd.DataFrame({"a": range(10), "b": [str(i) for i in range(10)]})
    schema, rows = _scan_rows(_main({"df": df}), "df", ["b"], [("a", ">", 6)])
    assert schema.names == ["b"]
    assert rows == [{"b": "7"}, {"b": "8"}, {"b": "9"}]


def test_main_list_tables_only_dataframes():
    df = pd.DataFrame(range(3), columns=["a"])
    assert asyncio.run(_main({"df": df, "x": 1}).list_tables()) == ["df"]


def test_main_locals_shadow_globals():
    g = pd.DataFrame({"a": [1]})
    loc = pd.DataFrame({"a": [2]})
    _, rows = _scan_rows(_main({"df": g}, {"df": loc}), "df")
    assert rows == [{"a": 2}]


def test_main_unknown_table_raises():
    with pytest.raises(TableNotFound):
        asyncio.run(_main({}).schema("nope"))


def test_metadata_list_tables():
    assert set(asyncio.run(_meta({}).list_tables())) == {"kernel", "vars"}


def test_metadata_kernel_schema_and_row():
    provider = _meta({})
    schema = asyncio.run(provider.schema("kernel"))
    assert schema.names == [
        "language",
        "python_version",
        "python_version_short",
        "implementation",
        "executable",
        "platform",
        "system",
        "machine",
        "pid",
        "cwd",
    ]
    assert schema.field("pid").type == pa.int64()
    _, rows = _scan_rows(provider, "kernel")
    assert len(rows) == 1
    row = rows[0]
    assert row["language"] == "python"
    assert row["python_version"] == sys.version
    assert row["executable"] == sys.executable
    assert row["pid"] == os.getpid()


def test_metadata_vars_basic():
    g_df = pd.DataFrame(range(4), columns=["a"])
    _, rows = _scan_rows(_meta({"g_df": g_df}, {"x": 5}), "vars")
    by_name = {r["name"]: r for r in rows}

    assert by_name["g_df"]["is_dataframe"] is True
    assert by_name["g_df"]["scope"] == "global"
    assert by_name["g_df"]["summary"].startswith("DataFrame(")

    assert by_name["x"]["type"] == "int"
    assert by_name["x"]["is_dataframe"] is False
    assert by_name["x"]["scope"] == "local"


def test_metadata_vars_excludes_only_dunders():
    provider = _meta({"__name__": "__main__", "display": lambda *a, **k: None, "mod": os, "_tmp": 1})
    _, rows = _scan_rows(provider, "vars")
    names = {r["name"] for r in rows}
    assert "__name__" not in names
    assert {"display", "mod", "_tmp"} <= names


def test_metadata_vars_shadowing():
    _, rows = _scan_rows(_meta({"v": 1}, {"v": "local"}), "vars")
    v_rows = [r for r in rows if r["name"] == "v"]
    assert len(v_rows) == 1
    assert v_rows[0]["scope"] == "local"
    assert v_rows[0]["type"] == "str"


def test_metadata_scan_applies_filter():
    _, rows = _scan_rows(_meta({"g": 1}, {"loc": 2}), "vars", None, [("scope", "=", "local")])
    assert rows
    assert all(r["scope"] == "local" for r in rows)


def test_metadata_scan_projection():
    schema, rows = _scan_rows(_meta({}), "kernel", ["language", "pid"])
    assert schema.names == ["language", "pid"]
    assert set(rows[0]) == {"language", "pid"}


def test_metadata_unknown_table_raises():
    provider = _meta({})
    with pytest.raises(TableNotFound):
        asyncio.run(provider.schema("nope"))
    with pytest.raises(TableNotFound):
        _scan_rows(provider, "nope")


def test_metadata_vars_empty_namespace():
    schema, rows = _scan_rows(_meta({}), "vars")
    assert rows == []
    assert schema.names == ["name", "type", "scope", "is_dataframe", "summary"]


def test_metadata_vars_safe_summary_does_not_throw():
    class Hostile:
        def __repr__(self):
            raise RuntimeError("boom")

    _, rows = _scan_rows(_meta({"bad": Hostile()}), "vars")
    by_name = {r["name"]: r for r in rows}
    assert by_name["bad"]["summary"] == "<unrepresentable>"


def test_rpc_python_execs_into_globals():
    g: dict = {}
    fn, schema = rpc_registry(Namespaces(g, {}))["python"]
    out = fn(["x = 41 + 1"], None)
    assert out.schema == schema
    assert out.to_pylist() == [{"ok": True}]
    assert g["x"] == 42


def test_rpc_python_error_propagates():
    fn, _ = rpc_registry(Namespaces({}))["python"]
    with pytest.raises(ZeroDivisionError):
        fn(["1 / 0"], None)


def test_rpc_sh():
    fn, schema = rpc_registry(Namespaces({}))["sh"]
    out = fn(["echo hi; echo err >&2; exit 3"], None)
    assert out.schema == schema
    assert out.to_pylist() == [{"returncode": 3, "stdout": "hi\n", "stderr": "err\n"}]


class _CatalogLoop:
    """open_catalog on a background loop, as the kernel runs it."""

    def __init__(self, *namespaces):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.con = self.run(open_catalog(*namespaces))

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout=60)

    def sql(self, query):
        async def _q():
            return await asyncio.to_thread(lambda: self.con.execute(query).fetchall())

        return self.run(_q())

    def close(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)


needs_extensions = pytest.mark.skipif(
    not _extensions_loadable(),
    reason="virtual_catalog/n6k_server could not be loaded; set N6K_EXTENSION_DIR to a local build",
)


@pytest.fixture
def catalog():
    g: dict = {"df": pd.DataFrame({"a": [1, 2, 3]})}
    loop = _CatalogLoop(g, {})
    loop.globals = g
    yield loop
    loop.close()


def _tables(catalog):
    return sorted(
        catalog.sql("SELECT schema_name || '.' || table_name FROM duckdb_tables() " "WHERE database_name = 'kernel'")
    )


@needs_extensions
def test_catalog_serves_both_schemas(catalog):
    assert _tables(catalog) == [
        ("main.df",),
        ("metadata.kernel",),
        ("metadata.vars",),
    ]
    assert catalog.sql("SELECT a FROM kernel.main.df WHERE a > 1") == [(2,), (3,)]


@needs_extensions
def test_catalog_rpc_python_and_sh(catalog):
    assert catalog.sql("SELECT * FROM kernel.main.python('y = 7')") == [(True,)]
    assert catalog.globals["y"] == 7
    assert catalog.sql("SELECT returncode, stdout FROM kernel.main.sh('echo hi')") == [(0, "hi\n")]


def test_invalidator_noop_when_unbound():
    assert CatalogInvalidator().invalidate(["main"]) == []


@needs_extensions
def test_invalidator_refreshes_table_list(catalog):
    inv = CatalogInvalidator()
    inv.bind(catalog.loop, catalog.con)
    assert ("main.df2",) not in _tables(catalog)
    catalog.globals["df2"] = pd.DataFrame({"b": [1]})
    [fut] = inv.invalidate(["main"])
    fut.result(timeout=10)
    assert ("main.df2",) in _tables(catalog)

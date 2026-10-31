"""The ``page.test`` subset of sqllogictest, against a plain in-memory DuckDB (no
server, no n6k extension).
"""

import duckdb
import pytest

from n6k_app.sqltest import QUERY, STATEMENT_ERROR, STATEMENT_OK, SqlTestError, parse, run


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO t VALUES (1, 'alice'), (2, NULL)")
    return c


def _run(con: duckdb.DuckDBPyConnection, text: str) -> list[bool]:
    return [r.ok for r in run(con, parse(text, "page.test"))]


def _details(con: duckdb.DuckDBPyConnection, text: str) -> list[str]:
    return [r.detail for r in run(con, parse(text, "page.test"))]


def test_parses_each_directive_with_its_line() -> None:
    directives = parse(
        "# a comment\n"
        "\n"
        "statement ok\n"
        "SELECT 1;\n"
        "\n"
        "query II\n"
        "SELECT id, name FROM t ORDER BY id;\n"
        "----\n"
        "1\talice\n"
        "\n"
        "statement error\n"
        "SELECT nope FROM t;\n"
        "----\n"
        "Binder Error\n",
        "page.test",
    )
    assert [(d.kind, d.line) for d in directives] == [(STATEMENT_OK, 3), (QUERY, 6), (STATEMENT_ERROR, 11)]
    assert directives[1].arity == 2
    assert directives[2].expected == "Binder Error"
    assert directives[0].where == "page.test:3"


def test_statement_ok_passes_and_fails(con: duckdb.DuckDBPyConnection) -> None:
    assert _run(con, "statement ok\nINSERT INTO t VALUES (3, 'bob');\n") == [True]
    assert _run(con, "statement ok\nSELECT nope FROM t;\n") == [False]


def test_statement_error_matches_on_substring(con: duckdb.DuckDBPyConnection) -> None:
    assert _run(con, "statement error\nSELECT nope FROM t;\n----\nBinder Error\n") == [True]
    assert _run(con, "statement error\nSELECT nope FROM t;\n----\n") == [True]  # empty = any error
    assert _run(con, "statement error\nSELECT nope FROM t;\n----\nCatalog Error\n") == [False]
    assert _run(con, "statement error\nSELECT 1;\n----\nBinder Error\n") == [False]  # it succeeded


def test_query_compares_rows_in_order(con: duckdb.DuckDBPyConnection) -> None:
    passing = "query II\nSELECT id, name FROM t ORDER BY id;\n----\n1\talice\n2\tNULL\n"
    assert _run(con, passing) == [True]

    reversed_rows = "query II\nSELECT id, name FROM t ORDER BY id;\n----\n2\tNULL\n1\talice\n"
    assert _run(con, reversed_rows) == [False]


def test_null_renders_as_the_null_literal(con: duckdb.DuckDBPyConnection) -> None:
    assert _run(con, "query I\nSELECT name FROM t WHERE id = 2;\n----\nNULL\n") == [True]
    assert _run(con, "query I\nSELECT name FROM t WHERE id = 2;\n----\n\n") == [False]


def test_arity_mismatch_is_reported_not_ignored(con: duckdb.DuckDBPyConnection) -> None:
    (detail,) = _details(con, "query I\nSELECT id, name FROM t ORDER BY id;\n----\n1\talice\n2\tNULL\n")
    assert "declared 1 column(s), query returned 2" in detail


def test_row_mismatch_shows_both_sides(con: duckdb.DuckDBPyConnection) -> None:
    (detail,) = _details(con, "query I\nSELECT id FROM t ORDER BY id;\n----\n1\n99\n")
    assert "expected" in detail and "got" in detail
    assert "99" in detail and "2" in detail


def test_a_failure_does_not_stop_later_directives(con: duckdb.DuckDBPyConnection) -> None:
    text = "statement ok\nSELECT nope;\n\nstatement ok\nSELECT 1;\n"
    assert _run(con, text) == [False, True]


def test_unknown_directive_fails_loudly() -> None:
    with pytest.raises(SqlTestError, match="unknown directive 'require'"):
        parse("require n6k_client\n", "page.test")
    with pytest.raises(SqlTestError, match="unknown directive 'loop'"):
        parse("loop i 0 10\nSELECT 1;\n", "page.test")


def test_malformed_directives_are_parse_errors() -> None:
    with pytest.raises(SqlTestError, match="no SQL after it"):
        parse("statement ok\n\nSELECT 1;\n", "page.test")
    with pytest.raises(SqlTestError, match="needs a '----' block"):
        parse("query I\nSELECT 1;\n", "page.test")
    with pytest.raises(SqlTestError, match="takes no '----' block"):
        parse("statement ok\nSELECT 1;\n----\n1\n", "page.test")
    with pytest.raises(SqlTestError, match="one letter per column"):
        parse("query XY\nSELECT 1;\n----\n1\n", "page.test")
    with pytest.raises(SqlTestError, match="expected 'statement ok' or 'statement error'"):
        parse("statement maybe\nSELECT 1;\n", "page.test")

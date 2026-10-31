"""Runner for ``page.test`` files — a subset of duckdb's sqllogictest format.

Supported directives are ``statement ok``, ``statement error`` and ``query <letters>``.
Anything else raises: a ``require`` or ``loop`` pasted from a duckdb test must fail
loudly rather than silently drop the checks that follow it.

Deliberately independent of the rest of n6k so it runs against any DuckDB connection.
"""

from dataclasses import dataclass
from typing import Any, Sequence

import duckdb

STATEMENT_OK = "statement_ok"
STATEMENT_ERROR = "statement_error"
QUERY = "query"

_SEPARATOR = "----"
# sqllogictest type letters: integer, text, real. Only the count is checked (arity).
_TYPE_LETTERS = set("ITR")


class SqlTestError(Exception):
    """A malformed ``.test`` file. Raised at parse time, before anything executes."""


@dataclass(frozen=True)
class Directive:
    kind: str
    path: str
    line: int
    sql: str
    # statement_error: the substring the message must contain ("" = any error).
    # query: the expected rows, already tab-joined. statement_ok: None.
    expected: str | None = None
    arity: int | None = None

    @property
    def where(self) -> str:
        return f"{self.path}:{self.line}"


@dataclass(frozen=True)
class CheckResult:
    directive: Directive
    ok: bool
    detail: str = ""


def _parse_header(text: str, path: str, lineno: int) -> tuple[str, int | None]:
    """``statement ok`` / ``statement error`` / ``query III`` -> (kind, arity)."""
    parts = text.split()
    head = parts[0]

    if head == "statement":
        mode = parts[1] if len(parts) > 1 else ""
        if mode == "ok":
            return STATEMENT_OK, None
        if mode == "error":
            return STATEMENT_ERROR, None
        raise SqlTestError(f"{path}:{lineno}: expected 'statement ok' or 'statement error', got {text!r}")

    if head == "query":
        letters = parts[1] if len(parts) > 1 else ""
        if not letters or set(letters) - _TYPE_LETTERS:
            raise SqlTestError(
                f"{path}:{lineno}: 'query' needs a type string of {''.join(sorted(_TYPE_LETTERS))} "
                f"(one letter per column), got {letters!r}"
            )
        return QUERY, len(letters)

    raise SqlTestError(
        f"{path}:{lineno}: unknown directive {head!r}. This runner supports "
        f"'statement ok', 'statement error' and 'query <letters>' only."
    )


def parse(text: str, path: str) -> list[Directive]:
    """Directives in file order. Raises ``SqlTestError`` on anything malformed."""
    lines = text.splitlines()
    out: list[Directive] = []
    i = 0

    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            i += 1
            continue

        header_line = i + 1
        kind, arity = _parse_header(stripped, path, header_line)
        i += 1

        sql_lines: list[str] = []
        while i < len(lines) and lines[i].strip() and lines[i].strip() != _SEPARATOR:
            sql_lines.append(lines[i])
            i += 1
        sql = "\n".join(sql_lines).strip()
        if not sql:
            raise SqlTestError(f"{path}:{header_line}: {stripped!r} has no SQL after it")

        has_separator = i < len(lines) and lines[i].strip() == _SEPARATOR
        expected: str | None = None
        if has_separator:
            i += 1
            body: list[str] = []
            while i < len(lines) and lines[i].strip():
                body.append(lines[i].rstrip())
                i += 1
            expected = "\n".join(body)

        if kind == STATEMENT_OK and has_separator:
            raise SqlTestError(f"{path}:{header_line}: 'statement ok' takes no '{_SEPARATOR}' block")
        if kind in (STATEMENT_ERROR, QUERY) and not has_separator:
            raise SqlTestError(f"{path}:{header_line}: {stripped!r} needs a '{_SEPARATOR}' block after its SQL")

        out.append(Directive(kind=kind, path=path, line=header_line, sql=sql, expected=expected, arity=arity))

    return out


def render_value(value: Any) -> str:
    return "NULL" if value is None else str(value)


def render_row(row: Sequence[Any]) -> str:
    return "\t".join(render_value(v) for v in row)


def _diff(expected: list[str], got: list[str]) -> str:
    """Aligned expected/got columns, with a dash where one side ran out."""
    width = max((len(e) for e in expected), default=0)
    width = max(width, len("expected"))
    lines = [f"{'expected'.ljust(width)}    got"]
    for n in range(max(len(expected), len(got))):
        left = expected[n] if n < len(expected) else "—"
        right = got[n] if n < len(got) else "—"
        marker = "  " if left == right else "≠ "
        lines.append(f"{marker}{left.ljust(width)}    {right}")
    return "\n".join(lines)


def _check(con: duckdb.DuckDBPyConnection, d: Directive) -> CheckResult:
    if d.kind == STATEMENT_OK:
        try:
            con.execute(d.sql)
        except Exception as exc:
            return CheckResult(d, False, f"{type(exc).__name__}: {exc}")
        return CheckResult(d, True)

    if d.kind == STATEMENT_ERROR:
        wanted = (d.expected or "").strip()
        try:
            con.execute(d.sql)
        except Exception as exc:
            message = str(exc)
            if wanted and wanted not in message:
                return CheckResult(d, False, f"error did not contain {wanted!r}:\n{message}")
            return CheckResult(d, True)
        return CheckResult(
            d, False, f"expected an error{f' containing {wanted!r}' if wanted else ''}, but it succeeded"
        )

    try:
        cur = con.execute(d.sql)
        rows = cur.fetchall()
        columns = len(cur.description or ())
    except Exception as exc:
        return CheckResult(d, False, f"{type(exc).__name__}: {exc}")

    if d.arity is not None and columns != d.arity:
        return CheckResult(d, False, f"declared {d.arity} column(s), query returned {columns}")

    got = [render_row(r) for r in rows]
    expected = [line for line in (d.expected or "").splitlines() if line.strip()]
    if got != expected:
        return CheckResult(d, False, _diff(expected, got))
    return CheckResult(d, True)


def run(con: duckdb.DuckDBPyConnection, directives: Sequence[Directive]) -> list[CheckResult]:
    """Every directive in order. A failure does not stop the ones after it."""
    return [_check(con, d) for d in directives]

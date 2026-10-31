import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pytest
from n6k_app import Database, Rpc, Schema, Source, Table, View
from n6k_app.members import _sql_lit, bind_catalogs


class Metric(Schema):
    value = pa.float64()


@dataclass
class Fixture:
    db: Database
    items: Source
    weather_metric: Rpc[Table[Metric]]
    windowed: Rpc[Table[Metric]]
    spread: Rpc[Table[Metric]]
    top: Rpc[Table[Metric]]
    recent: View


@pytest.fixture
def data() -> Fixture:
    database = Database()

    @database.data()
    def items() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3]})

    @database.rpc()
    def weather_metric(agg: str, col: str, year: int) -> Table[Metric]:
        return pd.DataFrame([{"value": 1.0}])

    @database.rpc()
    def windowed(col: str, year: int = 2015) -> Table[Metric]:
        return pd.DataFrame([{"value": 1.0}])

    @database.rpc()
    def spread(lo: int = 0, hi: int = 10) -> Table[Metric]:
        return pd.DataFrame([{"value": 1.0}])

    # Parent-bound: n6k supplies the parent DataFrame, so it is not a SQL argument.
    @items.rpc()
    def top(df: pd.DataFrame, limit: int) -> Table[Metric]:
        return pd.DataFrame([{"value": float(len(df.head(limit)))}])

    @database.view()
    def recent() -> str:
        return "SELECT * FROM items"

    return Fixture(database, items, weather_metric, windowed, spread, top, recent)


@pytest.fixture(autouse=True)
def rendering(data: Fixture) -> Iterator[None]:
    """Every test below runs as if inside a page render serving `data.db` as `db`."""
    with bind_catalogs({"db": data.db}):
        yield


def test_alias_comes_from_the_rendering_page(data: Fixture) -> None:
    assert data.items.table == "db.items"
    assert data.items.name == "items"


def test_same_database_resolves_per_page_alias(data: Fixture) -> None:
    # A Database is slug-agnostic: the alias is whatever the page's data.py named it.
    with bind_catalogs({"sales": data.db}):
        assert data.items.table == "sales.items"
        assert data.items.ref().sql == "SELECT * FROM sales.items"


def test_first_alias_wins_when_one_database_has_two_names(data: Fixture) -> None:
    with bind_catalogs({"sales": data.db, "orders": data.db}):
        assert data.items.table == "sales.items"


def test_a_database_this_page_does_not_serve_is_an_error(data: Fixture) -> None:
    with bind_catalogs({"other": Database()}):
        with pytest.raises(RuntimeError, match="this page serves: other"):
            data.items.ref()


def test_ref_outside_a_render_is_an_error(data: Fixture) -> None:
    # The autouse fixture binds a map; escape it to reach the unbound case.
    from n6k_app.members import _catalogs

    token = _catalogs.set(None)
    try:
        with pytest.raises(RuntimeError, match="only exists while a page renders"):
            data.items.ref()
    finally:
        _catalogs.reset(token)


def test_non_main_schema_is_qualified() -> None:
    database = Database()

    @database.reporting.data()
    def monthly() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    with bind_catalogs({"db": database}):
        assert monthly.table == "db.reporting.monthly"


def test_declared_order_is_used_regardless_of_kwarg_order(data: Fixture) -> None:
    expected = "SELECT * FROM db.weather_metric('max', 'temp_max', 2015)"
    assert data.weather_metric.ref(agg="max", col="temp_max", year=2015).sql == expected
    # Kwargs in a different call order must not reorder the SQL.
    assert data.weather_metric.ref(year=2015, agg="max", col="temp_max").sql == expected


def test_zero_arg_source_ref_has_no_call_parens(data: Fixture) -> None:
    assert data.items.ref().sql == "SELECT * FROM db.items"


def test_defaulted_trailing_arg_may_be_omitted(data: Fixture) -> None:
    assert data.windowed.ref(col="wind").sql == "SELECT * FROM db.windowed('wind')"


def test_omitting_a_defaulted_arg_before_a_supplied_one_is_an_error(data: Fixture) -> None:
    # `spread(lo=0, hi=10)` — supplying only `hi` would slide it into `lo`.
    with pytest.raises(TypeError, match=r"cannot pass 'hi' while omitting \['lo'\]"):
        data.spread.ref(hi=5)
    assert data.spread.ref(lo=1).sql == "SELECT * FROM db.spread(1)"


def test_unknown_and_missing_args_are_errors(data: Fixture) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        data.weather_metric.ref(agg="max", col="temp_max", year=2015, nope=1)
    with pytest.raises(TypeError, match="missing required argument 'year'"):
        data.weather_metric.ref(agg="max", col="temp_max")
    with pytest.raises(TypeError, match="missing required argument 'col'"):
        data.windowed.ref(year=2016)


def test_args_on_a_non_rpc_are_an_error(data: Fixture) -> None:
    with pytest.raises(TypeError, match="not an rpc"):
        data.items.ref(year=2015)


def test_parent_bound_rpc_omits_the_parent_from_the_sql_args(data: Fixture) -> None:
    assert data.top.ref(limit=2).sql == "SELECT * FROM db.top(2)"


def test_source_materializes(data: Fixture) -> None:
    assert list(data.items()["id"]) == [1, 2, 3]


def test_source_takes_no_arguments(data: Fixture) -> None:
    with pytest.raises(TypeError, match="takes no arguments"):
        data.items(year=2015)


def test_rpc_runs_here_and_now(data: Fixture) -> None:
    result = data.weather_metric(agg="max", col="temp_max", year=2015)
    assert isinstance(result, pd.DataFrame)
    assert result["value"][0] == 1.0


def test_parent_bound_rpc_is_handed_its_parent(data: Fixture) -> None:
    result = data.top(limit=2)
    assert isinstance(result, pd.DataFrame)
    assert result["value"][0] == 2.0


def test_rpc_with_an_injected_param_cannot_be_called_directly() -> None:
    from n6k_app import Session

    database = Database()

    @database.rpc()
    def whoami(user_id: int, session: Session) -> Table[Metric]:
        return pd.DataFrame([{"value": 1.0}])

    with pytest.raises(RuntimeError, match="injected parameter"):
        whoami(user_id=1)


def test_a_view_has_nothing_to_materialize(data: Fixture) -> None:
    with pytest.raises(RuntimeError, match="nothing to"):
        data.recent()


def test_a_view_is_queryable(data: Fixture) -> None:
    assert data.recent.ref().sql == "SELECT * FROM db.recent"


@pytest.mark.parametrize(
    ("value", "sql"),
    [
        (None, "NULL"),
        (True, "true"),
        (False, "false"),
        (2015, "2015"),
        (1.5, "1.5"),
        ("max", "'max'"),
        ("O'Hare", "'O''Hare'"),  # quote escaping
        (date(2015, 1, 2), "'2015-01-02'"),
        (datetime(2015, 1, 2, 3, 4), "'2015-01-02T03:04:00'"),
        ([2012, 2013], "[2012, 2013]"),
        (["a", "b"], "['a', 'b']"),
    ],
)
def test_sql_literals(value: object, sql: str) -> None:
    assert _sql_lit(value) == sql


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_floats_have_no_literal(value: float) -> None:
    with pytest.raises(ValueError, match="no SQL literal form"):
        _sql_lit(value)


def test_unsupported_type_is_an_error() -> None:
    with pytest.raises(TypeError, match="cannot pass object as a SQL argument"):
        _sql_lit(object())

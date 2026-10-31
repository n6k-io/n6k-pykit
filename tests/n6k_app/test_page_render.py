import pandas as pd
import pytest
from n6k_app import Database
from n6k_app.frontend import Components, page


@pytest.fixture
def db() -> Database:
    database = Database()

    @database.data()
    def items() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    return database


def test_components_is_injected_and_builds_the_tree() -> None:
    @page
    def home(c: Components) -> None:
        c.h1("hello")

    tree = home.render({})
    assert tree["children"][0]["component"] == "h1"


def test_a_page_may_take_nothing() -> None:
    @page
    def bare() -> None:
        return None

    assert bare.render({})["children"] == []


def test_members_resolve_against_the_rendering_page_catalogs(db: Database) -> None:
    @db.data("rows")
    def rows() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    @page
    def home(c: Components) -> None:
        c.Table(data=rows.table)

    tree = home.render({}, {"sales": db})
    assert tree["children"][0]["props"]["data"] == "sales.rows"


def test_the_binding_does_not_outlive_the_render(db: Database) -> None:
    @db.data("rows")
    def rows() -> pd.DataFrame:
        return pd.DataFrame({"id": [1]})

    @page
    def home(c: Components) -> None:
        c.h1("hi")

    home.render({}, {"sales": db})
    with pytest.raises(RuntimeError, match="only exists while a page renders"):
        rows.table


def test_an_unknown_parameter_names_the_import_to_use() -> None:
    @page
    def home(c: Components, revenue: object) -> None:
        return None

    with pytest.raises(TypeError, match="from .data import revenue"):
        home.render({})

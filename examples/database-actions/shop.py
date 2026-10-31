from typing import Annotated

import pandas as pd

from n6k_app import Database, Session, action

sales = Database()


@sales.data()
def orders() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3, 4], "item": ["apple", "pear", "plum", "fig"]})


@action
def order_count(session: Annotated[Session, sales]) -> dict[str, object]:
    """Number of orders, and the catalog the call was served under."""
    return {"count": len(orders()), "catalog": session.catalog}


@action
def items_after(min_id: int) -> list[str]:
    """Items of the orders whose id is above min_id."""
    df = orders()
    return list(df[df["id"] > min_id]["item"])

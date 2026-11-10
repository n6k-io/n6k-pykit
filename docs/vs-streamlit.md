# n6k vs Streamlit

## Same idea, one change

You still write Python, top to bottom, and get a live data app. The one change:
your data lives in `data.py`, and the page reads it by name.

## Side by side

A small sales dashboard: a title, a category filter, two KPIs and a
revenue-by-month chart.

**Streamlit**, `app.py`:

```python
import pandas as pd
import streamlit as st

@st.cache_data
def load():
    return pd.read_parquet("orders.parquet")

orders = load()

st.title("Sales")
st.write("Revenue and order volume by month. Pick a category to drill in.")

category = st.selectbox("Category", ["All", "Hardware", "Software", "Services"])
if category != "All":
    orders = orders[orders.category == category]

st.metric("Revenue", f"${orders.amount.sum():,.0f}")
st.metric("Orders", len(orders))

by_month = (
    orders.assign(month=orders.order_date.dt.strftime("%Y-%m"))
    .groupby("month", as_index=False)["amount"].sum()
)
st.line_chart(by_month, x="month", y="amount")
```

Run it with `streamlit run app.py`.

**n6k**, `src/app/data.py`:

```python
import pandas as pd
from n6k_app import Database

db = Database()

@db.data()
def orders() -> pd.DataFrame:
    return pd.read_parquet("orders.parquet")

@orders.derive()
def revenue_by_month(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(month=df.order_date.dt.strftime("%Y-%m"))
        .groupby(["month", "category"], as_index=False)
        .agg(revenue=("amount", "sum"), orders=("amount", "count"))
    )
```

**n6k**, `src/app/page.py`:

```python
from n6k_app.frontend import Components, page

from .data import revenue_by_month

@page
def sales(c: Components) -> None:
    c.h1("Sales")
    c.p("Revenue and order volume by month. Pick a category to drill in.")

    category = c.Select(
        key="category",
        label="Category",
        options=["Hardware", "Software", "Services"],
        empty="All",
    )

    c.Metric(label="Revenue", data=revenue_by_month, value="sum(revenue)", valueFormat="$,.0f", filters={"category": category})
    c.Metric(label="Orders", data=revenue_by_month, value="sum(orders)", filters={"category": category})
    c.LineChart(data=revenue_by_month, x="month", y="sum(revenue)", filters={"category": category})
```

Run it with `uv run n6k dev`.

## What you get for free

- **Data loads once.** `orders` and `revenue_by_month` are computed once per
  server process. Moving the filter re-runs `page()`, which only lays out
  components. Your pandas code doesn't run again.
- **The charts do the work.** Each component names a table and a filter. The
  browser queries the table for exactly what that component shows.
- **Your data is a database.** While the app runs, any DuckDB can query the same
  tables the page shows. See [Query it as SQL](#query-it-as-sql).

## Most dashboards don't need Python in the page

Usually the hard part is the data, and the page is just controls and charts.
Keep your Python in `data.py` and write the same page as Markdown, `page.mdx`:

```mdx
# Sales

Revenue and order volume by month. Pick a category to drill in.

::Select[category]{label="Category" options="Hardware,Software,Services" empty="All"}

<Metric label="Revenue" data="db.revenue_by_month" value="sum(revenue)" valueFormat="$,.0f" filters={{ category }} />
<Metric label="Orders" data="db.revenue_by_month" value="sum(orders)" filters={{ category }} />
<LineChart data="db.revenue_by_month" x="month" y="sum(revenue)" filters={{ category }} />
```

No Python runs for this page. It's plain frontend, and Python only runs in
`data.py`.

## Try it

```sh
uvx n6k init myapp --ref streamlit-v2
cd myapp
uv run n6k dev
```

This scaffolds the sales dashboard above, with a sample `orders.parquet`.

### View it as a web dashboard

Open http://localhost:7823 for the `page.py` version, and
http://localhost:7823/mdx for the `page.mdx` version. Edit any file and the page
reloads.

### Query it as SQL

From the DuckDB CLI, started with `duckdb -unsigned`:

```sql
INSTALL n6k_client FROM 'https://storage.googleapis.com/n6k-duckdb-release';
LOAD n6k_client;
ATTACH 'n6k://localhost:7823' AS db (TYPE n6k);
SELECT * FROM db.revenue_by_month;
```

### Explore it with an agent

Install the MCP server:

```sh
curl -fsSL https://n6k-io.github.io/n6kd/get.sh | sh
```

If you are using Claude Code, run:

```sh
claude mcp add n6kd -- n6kd mcp
```

Then prompt it:

> Connect to my n6k app running at ws://localhost:7823/ws. Call it myapp, and
> save it so it's still there next time.

> what tables does my app have, and what's in them?

## Running Streamlit in production?

Sound familiar?

- The app slows down every time more people open it.
- Scaling out means sticky sessions.
- Which rows a user can see is an `if` statement in your script.
- Product wants it inside the real app, and that means a rewrite.

n6k fixes these at the root. See
[n6k vs Streamlit in production](vs-streamlit-in-production.md).

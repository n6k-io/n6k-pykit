# n6k vs Streamlit in production

Looking for an easy intro to n6k? See [n6k vs Streamlit](vs-streamlit.md).

## The wall

The same problems come up once a Streamlit app gets real users:

- **Reruns.** Every interaction reruns the script. `@st.fragment` narrows that,
  but you scope each fragment by hand, and anything outside it still reruns.
- **Caching.** Loads need `@st.cache_data`, connections and models need
  `@st.cache_resource`, or each rerun rebuilds them.
- **Concurrent users.** Each reader's reruns are Python on your server, so load
  grows with readers
  ([example](https://discuss.streamlit.io/t/we-have-400-500-users-using-streamlit-at-scale-but-its-getting-choked/120506)).
- **Scaling out.** Sessions live in one process, so scaling out needs sticky
  sessions
  ([example](https://discuss.streamlit.io/t/horizontal-scaling-issues-on-render-with-custom-components-anyone-experienced-this/116201)).
- **Async.** The rerun model has no native async.
- **Permissions.** `st.login` tells you who the user is. Which rows they may see
  is still `if` statements in your script, after the data is loaded.
- **Your own frontend.** The view only runs inside Streamlit. You can't put it in
  an existing React app, and nothing else can query the data behind it.
- **The rewrite.** The app grows as one `app.py`, and the usual exit is a rewrite
  to FastAPI and React
  ([example](https://dev.to/przemyslawkepka/from-streamlit-to-flask-when-and-why-to-make-the-jump-4fkp)).

## One cause

A Streamlit app is one Python process holding your session. The UI, the state and
the compute are the same object.

## How n6k solves this

n6k puts a data protocol between the data and the UI.

A **server** publishes tables as a **catalog**: a named set of tables. A
**client** attaches the catalog and queries it with SQL. The UI is one client
among several, and the only thing it shares with the server is the protocol.

Servers:

- **pykit `data.py`.** Tables from Python functions, plus Postgres and MySQL with
  per-table policies.
- **`n6kd`.** A SQL file served as a catalog, with a YAML permissions file. No
  application code.

Clients:

- **A pykit page**, written as `page.py`, `page.mdx` or `page.tsx`.
- **Your own React app**, using the same components.
- **Any DuckDB**: the CLI, a notebook, another service.
- **An agent**, through MCP.

Either side can change without the other. The same page works against a
`data.py` or `n6kd`.

## The fixes

### Speed

**No rerun of your data code.** A control change never reloads or re-aggregates
data in Python. Components query the served tables, and filters and column
selections are pushed down to the server, so only what a query needs travels.

**Data loads once per process.** A `@db.data()` table loads on first access and
stays loaded, so there is no cache to add around a rerun. Upstream connections
open once and are pooled.

### Production

**Concurrent users.** Readers send queries, not reruns. Python runs when a query
reaches a Python table or a page calls a function.

**Scaling out.** Each replica loads its own copy of the data, and there is no
render loop or shared app state to keep in sync between them. A client's
WebSocket stays on one replica for its lifetime, and per-user state lives in a
`Session` on that connection, holding only what your functions put there.

**Async.** An `@action` can be `async def`, and an `@db.rpc()` can stream rows
as an `AsyncIterator`.

**Permissions on the data.** Upstream tables are attached with policies the
server enforces on every query. A client's SQL can't widen them, and credentials
stay on the server:

```python
pg = PostgresDatabase(
    host="...",
    policies=[
        policy("public.orders", "select", "status <> 'deleted'"),
        policy("public.orders", "insert", "true", check="amount > 0"),
    ],
)
```

With `n6kd` the same rules live in a YAML file, scoped per signed-in user:

```yaml
main.orders:
  select: "owner = $sub"
  insert: "owner = $sub"
```

### Reach

**Any DuckDB can query it:**

```sql
ATTACH 'n6k://localhost:7823/sales' AS db (TYPE n6k);
SELECT * FROM db.orders;
```

**Agents can use it.** An `@action` is a Python function the page can call, and
it is also an MCP tool. Its docstring and type hints are the tool schema:

```python
@action
def add_todo(title: str, session: Annotated[Session, db]) -> int:
    """Add a todo and return its id."""
    ...
```

**SQL writes reach Python.** A table can define `insert`, `update` and `delete`
functions, so an `INSERT` from any client lands in your code as rows.

## Your own frontend

The components are a React library. The same chart a `page.py` renders can live
in your own React or MDX page, pointed at the same server:

```mdx
<LineChart data="db.revenue_by_month" x="month" y="sum(revenue)"
  filters={[{ column: "category", operator: "=", value: category }]} />
```

`n6k build` generates TypeScript types from your tables, so `data=`, `x=` and
`y=` are checked against real columns. Behind the page, `data.py` can be swapped
for `n6kd` without touching the frontend.

## The migration path

No big-bang rewrite. You move one piece at a time, and each step still runs.

1. **Script.** A `page.py` with the compute inline. Same model as Streamlit:
   every control reruns everything.
2. **Add a `data.py`.** Move the compute into a `@db.rpc()`: a Python function
   that clients query as a table, with arguments. The page references it with
   `.ref()`, and controls that don't change its arguments re-query without
   calling Python:

   ```python
   @db.rpc()
   def simulate(sims: int, growth: float, vol: float) -> Table[Cone]:
       ...
   ```

   ```python
   cone = simulate.ref(sims=n, growth=growth, vol=vol)
   c.LineChart(query=cone, x="month", y="revenue", hue="band")
   ```

3. **Move the view to the frontend.** Rewrite the page as MDX or React. The
   `data.py` stays as it is.

After step 3 the frontend can live in any React app, and the server side can be
`data.py` or `n6kd`.

## Try it

```sh
uvx n6k init myapp
cd myapp
uv run n6k dev
```

In production, run `uv run n6k serve`: no reload, binds `0.0.0.0`, and
`--workers N` runs N processes.

For the quick tour, see [n6k vs Streamlit](vs-streamlit.md).

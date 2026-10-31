# Changelog

## 0.10.0

Breaking: a page's text traffic is now the driver's JSON-RPC 2.0 + MCP.

### Library users (Python)

- Actions are global: `from n6k_app import action`, `@action` / `@action("name")`. A page serves the actions its `data.py` binds (defined or imported); they run inside the page's catalogs, so imported members resolve. Docstring + type hints are the MCP tool schema. `Annotated[Session, db]` / `Annotated[<native Connection>, db]` are injected for that database; a bare `Session` / `Connection` means the page's only database. `@db.action` is removed.
- Action names must be unique across a page; `render` is reserved on a page with a `page.py`, `show_page` on a page with any page file.
- The HTTP `POST /<slug>` action endpoint is removed.
- The `nextpy` CLI is removed; use `n6k`.
- `n6k-duckdb` now requires the `[mcp]` extra.
- `Database.attach(con, catalog=, session=)` and `serving_authenticator(dbs, sessions)` take one `Session` per catalog; build them with `serving_sessions(dbs, con)`.
- `serve_databases(transport, name, databases, actions, render=None)` serves any connection; `discover_file` is replaced by `load_database_file(py_file) -> (databases, actions)`.

### `n6k database`

- Serves the file's actions over JSON-RPC + MCP alongside its databases, in both modes. A file with no `Database` is an error.
- HTTP: the only endpoint is `/ws`.
- `--listen` (`N6K_LISTEN`): n6k stream framing, `[4-byte BE payload length][1-byte type][payload]`, type `0x01` JSON-RPC, `0x02` n6k.
- `N6K_DB_SOCKET` (dialing a listening parent) is removed, from `n6k kernel` too; use `--listen`. `n6k kernel` now requires it.
- `n6k ws <socket>` dials a `--listen` worker once per WebSocket and maps the type to WebSocket text/binary.

### `n6k kernel`

- `process.queued`, `process.started` and `process.completed` are replaced by `process.status` with `state` `queued` (`code`, `thread`), `running`, or `completed` (`success`, `interrupted`, `added_vars`). Same moments, order and `seq`.
- `status` is renamed `state` in the `process.cell_state` reply's `cell` and the `process.cells` reply's `cells`.

### Clients (socket)

- One socket per page: `/<slug>/ws` (root page: `/ws`). `/<slug>/_page` is removed.
- Request `capabilities: ["jsonrpc", "mcp"]` in `FT_HELLO`; text before `FT_HELLO_ACK` is dropped.
- No tree is pushed. Call `render` with params `{"state": {...}}`; the result is the tree. A page error is a `fragment` tree, not a JSON-RPC error.
- Actions are JSON-RPC methods by name, params an object of keyword args. Errors: an action's `JsonRpcError` → its code and message; bad params → `-32602`; unknown method → `-32601`; any other exception → `-32603`.
- MCP on the same socket: `initialize`, `tools/list`, `tools/call`. Lists actions and, on a page with a page file, `show_page(state?)`: `{"type": "py", "tree"}` for a `page.py`, `{"type": "mdx"|"tsx", "source"}` otherwise. Not `render`.
- Close codes: `4404` for a page with no page file (`page.py`/`.mdx`/`.tsx`), databases or actions; `1011` when the page's databases can't be opened or its action names clash.

# `n6k database` with actions

`shop.py` defines one database (`sales`, table `orders`) and two actions: `order_count` and `items_after`.
Run everything from `server/`, with the local driver build:

```sh
export N6K_EXTENSION_DIR=/path/to/duckdb-n6k-protocol/build/release/extension
```

## HTTP

```sh
uv run n6k database examples/database-actions/shop.py --port 7833
```

```sh
uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws order_count
uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws items_after '{"min_id": 2}'
uv run python examples/database-actions/call.py ws://127.0.0.1:7833/ws tools/list
```

```sh
/path/to/duckdb-n6k-protocol/build/release/duckdb -unsigned -c "
LOAD '$N6K_EXTENSION_DIR/n6k_client/n6k_client.duckdb_extension';
ATTACH 'n6k://127.0.0.1:7833' AS x (TYPE n6k, catalog 'sales');
SELECT * FROM x.main.orders;"
```

## Unix socket, through `n6k ws`

```sh
uv run n6k database --listen /tmp/shop.sock examples/database-actions/shop.py
uv run n6k ws /tmp/shop.sock --port 7834
```

Then the same `call.py` / `duckdb` commands against port `7834`.

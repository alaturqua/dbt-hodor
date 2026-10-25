# DuckDB

hodor on `dbt-duckdb`. Covered by unit and end-to-end tests.

## Install

=== "uv"

    ```sh
    uv pip install "dbt-hodor[duckdb]" dbt-duckdb
    ```

=== "pip"

    ```sh
    pip install "dbt-hodor[duckdb]" dbt-duckdb
    ```

## Profile

```yaml
hodor_demo:
  target: local
  outputs:
    local:
      type: duckdb
      path: warehouse.duckdb
      schema: main
      threads: 2
      hodor_store: duckdb          # state goes to a local DuckDB file
      hodor_path: .hodor/state.duckdb
      # state table: main.hodor_state in .hodor/state.duckdb
```

The [local quickstart](../getting-started.md#local-quickstart-on-duckdb) builds a project on this profile.

## State store

The default store, `delta`, turns hodor off, so set `hodor_store` to [`duckdb`](../stores/duckdb.md) or [`postgres`](../stores/postgres.md). See [State stores](../stores/index.md).

## Data version

DuckDB has no table history. The token is the row count plus an order-independent sum of row hashes, computed with one query per table. That is fine for local test data sizes.

## Limits

- The fingerprint is one query per table, which is fine for local test data sizes.

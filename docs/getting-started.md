# Getting started

!!! warning "Early development"

    `dbt-hodor` is available on PyPI. Be cautious before enabling reuse in production: run in shadow mode for at least a week of scheduled runs, review its decisions, and investigate any false reuses first. See [the rollout guidance](operations.md#rollout).

## Install

hodor needs Python 3.10 or newer, `dbt-core` 1.12.x and `sqlglot` 30.22 or newer (below 31). dbt installs it as a plugin: dbt imports every installed `dbt_*` module that has a `plugins` attribute, so no dbt configuration is needed to load hodor.

=== "uv"

    ```sh
    uv pip install dbt-hodor
    # with the local DuckDB state store
    uv pip install "dbt-hodor[duckdb]"
    ```

=== "pip"

    ```sh
    pip install dbt-hodor
    # with the local DuckDB state store
    pip install "dbt-hodor[duckdb]"
    ```

=== "Databricks job"

    A job installs the wheel from a Unity Catalog volume. Upload it with `databricks fs cp`, then list it as a library:

    ```yaml
    libraries:
      - whl: /Volumes/<catalog>/<schema>/<volume>/dbt_hodor-<version>-py3-none-any.whl
    ```

    Or install directly from PyPI:

    ```yaml
    libraries:
      - pypi:
          package: "dbt-hodor"
    ```

The wheel is pure Python (`py3-none-any`), so the same file installs on Windows and on Linux job clusters.

The `duckdb` extra adds the `duckdb` package, which the local state store needs. Your adapter is not a dependency of hodor. hodor supports the `databricks`, `duckdb` and `trino` adapters and turns itself off on any other. With `trino`, the state store must be `duckdb` or `postgres`: the `delta` store needs the `databricks` adapter, and on any other adapter hodor turns itself off with a warning.

## Local quickstart on DuckDB

This example follows the end-to-end test project in `tests/e2e/test_duckdb.py`. Install `dbt-duckdb` next to hodor.

`profiles.yml`:

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
```

`dbt_project.yml`:

```yaml
name: hodor_demo
version: "1.0"
profile: hodor_demo
models:
  hodor_demo:
    +state:
      lag_tolerance: 0s
```

Two models in `models/`:

```sql
-- models/stg_orders.sql
select id, amount from {{ source('raw', 'orders') }}
```

```sql
-- models/fct_orders.sql
{{ config(materialized='table') }}
select id, amount * 2 as amount2 from {{ ref('stg_orders') }}
```

```yaml
# models/schema.yml
models:
  - name: fct_orders
    columns:
      - name: id
        data_tests: [not_null]
```

Declare the source `raw.orders` in `models/sources.yml` and create that table in `warehouse.duckdb`.

### First runs

The default mode is `shadow`: hodor decides and records, but every node builds. Set `HODOR_MODE=on` to let it reuse nodes.

```sh
export HODOR_MODE=on      # PowerShell: $env:HODOR_MODE = "on"
# or for one run only:
dbt build --vars '{hodor_mode: on}'
dbt build
dbt build
```

The first run has no history, so every node builds and hodor records it (`no_history`). The log shows the hodor reason in the bracket of each model line:

```text
dbt-hodor <installed version> is enabled for dbt build
dbt-hodor <installed version>: mode on, 0 build records for hodor_e2e/local
4 of 8 OK created sql table model main.fct_orders .............................. [OK (no_history) in 0.05s]
7 of 8 PASS not_null_fct_orders_id ............................................. [PASS in 0.09s]
dbt-hodor: 0 reused, 8 built, 0 fail-open errors
```

In the second run, with no new data and no code change, you see lines like these:

```text
dbt-hodor <installed version>: mode on, 8 build records for hodor_e2e/local
3 of 8 REUSED sql view model main.stg_orders ................................... [REUSED (view_unchanged) in 0.01s]
4 of 8 REUSED sql table model main.fct_orders .................................. [REUSED (no_new_data) in 0.01s]
7 of 8 PASS not_null_fct_orders_id ............................................. [PASS (replayed) in 0.02s]
dbt-hodor: 5 reused, 3 built, 0 fail-open errors
```

The `dbt-hodor:` line at the end is the summary. A built model shows its reason, for example `[OK (new_upstream_data) in 0.07s]`. Only the printed line changes: `run_results.json` keeps dbt's own message for builds. Warnings start with `dbt-hodor:`.

In `shadow` mode every node builds, and the bracket shows what hodor would have done:

```text
dbt-hodor <installed version>: mode shadow, 8 build records for hodor_e2e/local
4 of 8 OK created sql table model main.fct_orders .............................. [OK (shadow, could reuse: no_new_data) in 0.06s]
dbt-hodor: 3 could be reused, 5 need a build (shadow: all built), 0 fail-open errors
```

### Change something

- Insert a row into `raw.orders`, then run again. `fct_orders` rebuilds with reason `new_upstream_data`. `stg_orders` stays reused, because a view always shows current data (`view_unchanged`).
- Edit the SQL of `fct_orders`, then run again. It rebuilds with reason `code_changed`.
- Run `dbt build --full-refresh`. Everything builds (`full_refresh`).

Ask hodor why a node was reused or built. `dbt-hodor explain` reads the state of the project and target in the current folder and shows the latest run:

```sh
uv run dbt-hodor explain -s fct_orders
```

```text
dbt-hodor explain: hodor_demo/local, invocation 65b9ef08-... (2026-10-21 01:34 UTC), 1 node
model.hodor_demo.fct_orders: build (new_upstream_data)
  last build   2026-10-21 01:30 UTC, invocation 9d86be32-...
  code hash    same (qualify:6c1f02aa)
  config hash  same (e56f2a83)
  upstream     warehouse.raw.orders: 2:86347873... -> 3:22211632...
```

See [operations](operations.md#why-a-node-was-reused-or-built) for the options.

Next: [decision rules](how-it-works/decision-rules.md), [configuration](configuration.md).

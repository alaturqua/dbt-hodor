# Adapters

hodor supports the `databricks`, `trino`, `snowflake` and `duckdb` adapters and turns itself off on any other. Your adapter is not a dependency of hodor: install it next to hodor.

| Adapter | Tested version | Data version from | Default store | Other stores | Test status |
| --- | --- | --- | --- | --- | --- |
| [Databricks](databricks.md) | `dbt-databricks` 1.12.7 | Delta commit history | `delta` | `duckdb`, `postgres` (alias `lakebase`) | live tests on a Databricks workspace |
| [Trino](trino.md) | `dbt-trino` 1.10.6 | Iceberg snapshot history or Delta Lake commit history | `trino` | `duckdb`, `postgres` | Iceberg live on Trino 476 in Docker; Delta Lake unit tests only |
| [Snowflake](snowflake.md) | `dbt-snowflake` 1.12.4 | `SYSTEM$LAST_CHANGE_COMMIT_TIME` plus the `created` and `last_ddl` times | none (`delta` turns hodor off) | `duckdb`, `postgres` | live test passed on Snowflake 10.38 |
| [DuckDB](duckdb.md) | `dbt-duckdb` | content fingerprint | none (`delta` turns hodor off) | `duckdb`, `postgres` | unit and end-to-end tests |

A store and adapter pairing outside this table turns hodor off for the run with a warning. For example, `delta` needs the `databricks` adapter and `trino` needs the `trino` adapter. See [State stores](../stores/index.md).

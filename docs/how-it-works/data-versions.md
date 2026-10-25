# Data versions

A data version is an opaque text token. The engine only compares tokens for equality. Each adapter has a metadata provider that produces them.

## Per adapter

- [Databricks](../adapters/databricks.md#data-version): the version and commit time of the latest data-change commit in the Delta history.
- [Trino](../adapters/trino.md#data-version): the Iceberg snapshot and metadata log, or the Delta Lake commit history.
- [Snowflake](../adapters/snowflake.md#data-version): the `SYSTEM$LAST_CHANGE_COMMIT_TIME` token with the `created` and `last_ddl` times.
- [DuckDB](../adapters/duckdb.md#data-version): the row count plus an order-independent sum of row hashes.

## Unknown versions

If a version cannot be read, the table is unknown, and every node that depends on it builds (reason `unknown_upstream`, except views).

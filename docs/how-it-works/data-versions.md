# Data versions

A data version is an opaque text token. The engine only compares tokens for equality. Each adapter has a metadata provider that produces them.

## Per adapter

Each adapter page describes its token, what moves it and its limits.

### Delta (Databricks)

The version and commit time of the latest data-change commit in the Delta history. See [Databricks](../adapters/databricks.md#data-version).

### Trino (Iceberg and Delta Lake connectors)

The Iceberg snapshot and metadata log, or the Delta Lake commit history. See [Trino](../adapters/trino.md#data-version).

### Snowflake

The `SYSTEM$LAST_CHANGE_COMMIT_TIME` token with the `created` and `last_ddl` times. See [Snowflake](../adapters/snowflake.md#data-version).

### DuckDB (content fingerprint)

The row count plus an order-independent sum of row hashes. See [DuckDB](../adapters/duckdb.md#data-version).

## Unknown versions

If a version cannot be read, the table is unknown, and every node that depends on it builds (reason `unknown_upstream`, except views).

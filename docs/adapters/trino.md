# Trino

hodor on `dbt-trino`, tested with `dbt-trino` 1.10.6. The Iceberg connector was tested live on Trino 476 in Docker; the Delta Lake connector is covered by unit tests only.

## Install

=== "uv"

    ```sh
    uv pip install dbt-hodor dbt-trino
    ```

=== "pip"

    ```sh
    pip install dbt-hodor dbt-trino
    ```

## Profile

On the `trino` adapter the default store is `trino`: the state table lives in the target's catalog, in the schema `hodor_schema`, which must exist. A `profiles.yml` entry:

```yaml
hodor_demo:
  target: trino
  outputs:
    trino:
      type: trino
      host: localhost
      port: 8080
      user: dbt
      database: iceberg
      schema: demo
      threads: 2
      hodor_schema: ops
      # state table: iceberg.ops.hodor_state
```

To keep the state in a local DuckDB file instead, set `hodor_store: duckdb` and `hodor_path: .hodor/state.duckdb`.

## State store

The default store is [`trino`](../stores/trino.md). The other stores are [`duckdb`](../stores/duckdb.md) and [`postgres`](../stores/postgres.md). See [State stores](../stores/index.md).

## Data version

hodor runs `select * from "<catalog>"."<schema>"."<table>$history"` and reads the columns it gets back. The Iceberg branch was tested live against Trino 476 in Docker (see [Development](../development.md)). The Delta Lake branch is covered by unit tests only.

- **Iceberg** (columns `made_current_at`, `snapshot_id` and `is_current_ancestor`):
    - Token: the `snapshot_id` and the `made_current_at` time of the row with the latest `made_current_at` among the rows where `is_current_ancestor` is true, plus the time and file of the latest entry in `"<table>$metadata_log_entries"`. hodor does not take the newest committed snapshot.
    - Rollback: a rollback adds no row. It sets `is_current_ancestor` to false on the rolled-back snapshots, so the token moves back to an older snapshot and changes.
    - Schema change (tested with `ADD COLUMN`): it adds no snapshot but writes a new metadata file, so the log part of the token changes. Any other metadata commit moves the token too, which can cause an extra build.
    - Unknown: a history without a current-ancestor row, two current snapshots with the same time, two metadata log entries with the same time and different files, an empty metadata log, or a time hodor cannot read.
- **Delta Lake** (columns `version`, `timestamp` and `operation`): the same rules as Delta on Databricks apply, including the list of operations that never count. The Trino history has no `operationMetrics`, so every operation outside that list counts as a data change.
- **Other connectors**: Hive, memory and the rest have no `$history` table. The query fails, the table is unknown, and every node that depends on it builds. A table whose history is empty or has other columns is unknown too.

A view is hashed from its definition in `<catalog>.information_schema.views`.

Limit: hodor reads the whole `$history` table of each relevant table, not a window of 20 commits. On the Trino adapter the state store can be `trino`, `duckdb` or `postgres`.

## Limits

- hodor reads the whole `$history` table of each relevant table, not a window of 20 commits.
- Connectors other than Iceberg and Delta Lake (Hive, memory and the rest) have no `$history` table, so every node that depends on them builds.
- The Delta Lake connector is covered by unit tests only.

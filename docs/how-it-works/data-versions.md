# Data versions

A data version is an opaque text token. The engine only compares tokens for equality. Each adapter has a metadata provider that produces them.

## Delta (Databricks)

hodor runs `DESCRIBE HISTORY <table> LIMIT 20` and reads `version`, `timestamp`, `operation` and `operationMetrics` of each commit. It runs one query per relevant table, in parallel on `hodor_metadata_threads` threads, and caches the result for the run. After hodor builds a table, it reads that table's history once more, so downstream nodes in the same run see the new version.

### What counts as a data change

Known maintenance and metadata operations never count:

`OPTIMIZE`, `VACUUM START`, `VACUUM END`, `ANALYZE`, `COMPUTE STATS`, `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES`, `CHANGE COLUMN`, `ADD CONSTRAINT`, `DROP CONSTRAINT`, `UPGRADE PROTOCOL`, `SET TAGS`, `UNSET TAGS`, `CLUSTER BY`, `REORG`, `FSCK`.

Every other operation counts as a data change, unless its change metrics are all zero. The change metrics are `numOutputRows`, `numRemovedFiles`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numDeletedRows`, `numUpdatedRows` and `numCopiedRows`. A commit without any of them counts as a change. `numRemovedFiles` is there because a `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 but still empties the table.

The token is the version and the commit time of the latest data-change commit, for example `7@1792465200000`. The commit time matters: a table that is dropped and recreated, or swapped in from a staging table (`use_safer_relation_operations` with `use_materialization_v2`), restarts its versions, so the version alone can repeat after a real change. If the 20-commit window holds no data-change commit, the version is unknown, and dependents build.

Unknown operations count as changes on purpose. A new Delta operation name can cause extra builds, never wrong reuses.

!!! info "Checked on a workspace"

    Checks on a Unity Catalog workspace confirmed the rules above. Zero-row WRITE, MERGE, UPDATE and DELETE commits show all row metrics at 0. A `CREATE OR REPLACE TABLE AS SELECT` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 and `numRemovedFiles` above 0. `COMMENT ON TABLE` is recorded as `SET TBLPROPERTIES`, a column comment as `CHANGE COLUMN`. ANALYZE writes no commit. `DESCRIBE HISTORY` on a view fails, so a view used as a source counts as unknown. `information_schema.tables.last_altered` does not move on data writes, so hodor reads the history of every table. Delta paths on Volumes were not tested.

## Trino (Iceberg and Delta Lake connectors)

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

## Snowflake

hodor runs two queries per table. The first reads `created` and `last_ddl` from `<database>.information_schema.tables` (base tables only), as nanoseconds since the epoch. The second calls `SYSTEM$LAST_CHANGE_COMMIT_TIME` on the table, addressed by the name Snowflake stores. The token is `<commit>|<created>|<last_ddl>`. Relation keys are lowercase and Snowflake stores unquoted names in uppercase, so hodor finds the table with `lower()` and then quotes the stored name.

What moves each part, checked on Snowflake 10.38 by the live test `test_snowflake_token_moves_on_each_change`:

| Change | commit token | `created` | `last_ddl` |
| --- | --- | --- | --- |
| insert, update (even 0 rows), delete, truncate, merge | moves | no | no |
| `CREATE OR REPLACE TABLE`, CTAS, `SWAP WITH` | moves (a plain `CREATE OR REPLACE` of an empty table goes back to 0) | moves | moves |
| `ALTER TABLE ADD COLUMN`, `COMMENT` | no | no | moves |

A plain read moves nothing. The live lifecycle test also builds with dbt through insert, `ADD COLUMN` and `CREATE OR REPLACE` (see [Development](../development.md)). A `COMMENT` and a 0-row update move the token, so they cause an extra build, never a wrong reuse.

- **Unknown:** no matching base table, more than one match, a dynamic, hybrid or Iceberg table (the query leaves them out, because their change tokens were not tested), a null or non-integer value, or an error from `SYSTEM$LAST_CHANGE_COMMIT_TIME` (for example a missing table). Dependents build.
- **Views:** a view is hashed from its definition in `<database>.information_schema.views`. Two views whose names differ only in case give an unknown version, on every adapter.
- **Not supported:** stages and `@stage` reads are untraceable inputs, so the node builds. The `dynamic_table` materialization is unsupported, so those models always build.
- **State store:** on the `snowflake` adapter the state store can be `duckdb` or `postgres`. The default, `delta`, turns hodor off.

## DuckDB (content fingerprint)

DuckDB has no table history. The token is the row count plus an order-independent sum of row hashes, computed with one query per table. That is fine for local test data sizes.

## Unknown versions

If a version cannot be read, the table is unknown, and every node that depends on it builds (reason `unknown_upstream`, except views).

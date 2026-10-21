# Data versions

A data version is an opaque text token. The engine only compares tokens for equality. Each adapter has a metadata provider that produces them.

## Delta (Databricks)

hodor runs `DESCRIBE HISTORY <table> LIMIT 20` and reads `version`, `timestamp`, `operation` and `operationMetrics` of each commit. It runs one query per relevant table, in parallel on `hodor_metadata_threads` threads, and caches the result for the run. After hodor builds a table, it reads that table's history once more, so downstream nodes in the same run see the new version.

### What counts as a data change

Known maintenance and metadata operations never count:

`OPTIMIZE`, `VACUUM START`, `VACUUM END`, `ANALYZE`, `COMPUTE STATS`, `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES`, `CHANGE COLUMN`, `ADD CONSTRAINT`, `DROP CONSTRAINT`, `UPGRADE PROTOCOL`, `SET TAGS`, `UNSET TAGS`, `CLUSTER BY`, `REORG`, `FSCK`.

Every other operation counts as a data change, unless its row metrics are all zero. The row metrics are `numOutputRows`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numDeletedRows`, `numUpdatedRows` and `numCopiedRows`. A commit without any of them counts as a change.

The token is the version and the commit time of the latest data-change commit, for example `7@1792465200000`. The commit time matters: a table that is dropped and recreated, or swapped in from a staging table (`use_safer_relation_operations` with `use_materialization_v2`), restarts its versions, so the version alone can repeat after a real change. If the 20-commit window holds no data-change commit, the version is unknown, and dependents build.

Unknown operations count as changes on purpose. A new Delta operation name can cause extra builds, never wrong reuses.

!!! info "Checked on a workspace"

    A spike on a Unity Catalog workspace (2026-10-20) confirmed the rules above. Zero-row WRITE, MERGE, UPDATE and DELETE commits show all row metrics at 0. `COMMENT ON TABLE` is recorded as `SET TBLPROPERTIES`, a column comment as `CHANGE COLUMN`. ANALYZE writes no commit. `DESCRIBE HISTORY` on a view fails, so a view used as a source counts as unknown. `information_schema.tables.last_altered` does not move on data writes, so hodor reads the history of every table. Delta paths on Volumes were not tested.

## DuckDB (content fingerprint)

DuckDB has no table history. The token is the row count plus an order-independent sum of row hashes, computed with one query per table. That is fine for local test data sizes.

## Unknown versions

If a version cannot be read, the table is unknown, and every node that depends on it builds (reason `unknown_upstream`, except views).

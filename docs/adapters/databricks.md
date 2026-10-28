# Databricks

hodor on `dbt-databricks`, tested with `dbt-databricks` 1.12.7. Tested live on a Databricks workspace; see [Development](../development.md).

## Install

=== "uv"

    ```sh
    uv pip install dbt-hodor dbt-databricks
    ```

=== "pip"

    ```sh
    pip install dbt-hodor dbt-databricks
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

## Profile

```yaml
# profiles.yml
my_project:
  target: prod
  outputs:
    prod:
      type: databricks
      catalog: my_catalog
      host: <workspace_host>
      http_path: <sql_warehouse_http_path>
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      schema: <schema>
      hodor_mode: shadow
      hodor_schema: ops
      # state table: my_catalog.ops.hodor_state
```

## State store

The default store is [`delta`](../stores/delta.md). The other stores are [`duckdb`](../stores/duckdb.md) and [`postgres`](../stores/postgres.md) (alias `lakebase`). See [State stores](../stores/index.md).

## Data version

hodor runs `DESCRIBE HISTORY <table> LIMIT 20` and reads `version`, `timestamp`, `operation` and `operationMetrics` of each commit. It runs one query per relevant table, in parallel on `hodor_metadata_threads` threads, and caches the result for the run. After hodor builds a table, it reads that table's history once more, so downstream nodes in the same run see the new version. If those 20 commits are all maintenance commits (see below), for example after many `OPTIMIZE`, `VACUUM` or tag commits by predictive optimization, hodor reads up to 1000 commits once more for that table.

### What counts as a data change

Known maintenance and metadata operations never count:

`OPTIMIZE`, `VACUUM START`, `VACUUM END`, `ANALYZE`, `COMPUTE STATS`, `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES`, `CHANGE COLUMN`, `ADD CONSTRAINT`, `DROP CONSTRAINT`, `UPGRADE PROTOCOL`, `SET TAGS`, `UNSET TAGS`, `CLUSTER BY`, `REORG`, `FSCK`.

Every other operation counts as a data change, unless its change metrics are all zero. The change metrics are `numOutputRows`, `numRemovedFiles`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numDeletedRows`, `numUpdatedRows` and `numCopiedRows`. A commit without any of them counts as a change. `numRemovedFiles` is there because a `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 but still empties the table.

The token is the version and the commit time of the latest data-change commit, for example `7@1792465200000`. The commit time matters: a table that is dropped and recreated, or swapped in from a staging table (`use_safer_relation_operations` with `use_materialization_v2`), restarts its versions, so the version alone can repeat after a real change. If the history holds no data-change commit within the latest 1000 commits, or within Delta's history retention, the version is unknown, and dependents build.

Unknown operations count as changes on purpose. A new Delta operation name can cause extra builds, never wrong reuses.

!!! info "Checked on a workspace"

    Checks on a Unity Catalog workspace confirmed the rules above. Zero-row WRITE, MERGE, UPDATE and DELETE commits show all row metrics at 0. A `CREATE OR REPLACE TABLE AS SELECT` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 and `numRemovedFiles` above 0. `COMMENT ON TABLE` is recorded as `SET TBLPROPERTIES`, a column comment as `CHANGE COLUMN`. ANALYZE writes no commit. `DESCRIBE HISTORY` on a view fails, so a view used as a source counts as unknown. `information_schema.tables.last_altered` does not move on data writes, so hodor reads the history of every table. Delta paths on Volumes were not tested.

## Limits

- A view used as a source has an unknown version, so dependents build.
- Delta paths on Volumes were not tested.

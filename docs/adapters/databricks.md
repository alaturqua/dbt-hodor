# Databricks

hodor on `dbt-databricks`, tested with `dbt-databricks` 1.13.0. Tested live on a Databricks workspace; see [Development](../development.md).

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

## Jobs

A Databricks `dbt_task` job needs every command to start with `dbt`, so a wrapper CLI is not an option. hodor loads inside the dbt process, so the commands stay plain `dbt` commands.

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

## Clones and dbt v2

On dbt-core, hodor clones an unchanged table or incremental model from the base target with the adapter's `create_or_replace_clone`, a shallow clone on Databricks. Live tests on a Databricks workspace covered a shallow clone from the base target, `pre_clone` (the clone, then an incremental `MERGE` on top of it), a base table written since its build, which is not cloned, and a changed model built over its own clone of a table with automatic liquid clustering. See [Cloning](../how-it-works/decision-rules.md#cloning-from-the-base-target).

On dbt v2 (`databricks` and `spark` dialects), clones are off unless `dbt-hodor serve` starts with `--allow-clones` (or `HODOR_ALLOW_CLONES=true`), because of the auto clustering limit below. With clones on, the server answers with `CREATE OR REPLACE TABLE ... SHALLOW CLONE`. dbt v2 data versions are epoch timestamps, not Delta tokens: see [dbt v2](../dbt-v2.md). A live test ran dbt 2.0.8 on a Databricks SQL warehouse: dbt ran the shallow clone and reported the node as cloned from the other environment.

## Data version

hodor runs `DESCRIBE HISTORY <table> LIMIT 20` and reads `version`, `timestamp`, `operation` and `operationMetrics` of each commit. It runs one query per relevant table, in parallel on `hodor_metadata_threads` threads, and caches the result for the run. After hodor builds a table, it reads that table's history once more, so downstream nodes in the same run see the new version. If those 20 commits are all maintenance commits (see below), for example after many `OPTIMIZE`, `VACUUM` or tag commits by predictive optimization, hodor reads up to 1000 commits once more for that table.

### What counts as a data change

Known maintenance and metadata operations never count:

`OPTIMIZE`, `VACUUM START`, `VACUUM END`, `ANALYZE`, `COMPUTE STATS`, `SET TBLPROPERTIES`, `UNSET TBLPROPERTIES`, `ADD CONSTRAINT`, `DROP CONSTRAINT`, `UPGRADE PROTOCOL`, `SET TAGS`, `UNSET TAGS`, `CLUSTER BY`, `REORG`, `FSCK`.

Every other operation counts as a data change, unless its change metrics are all zero. The change metrics are `numOutputRows`, `numRemovedFiles`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numDeletedRows`, `numUpdatedRows` and `numCopiedRows`. A commit without any of them counts as a change. `numRemovedFiles` is there because a `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 but still empties the table.

The token is the version and the commit time of the latest data-change commit, for example `7@1792465200000`. The commit time matters: a table that is dropped and recreated, or swapped in from a staging table (`use_safer_relation_operations` with `use_materialization_v2`), restarts its versions, so the version alone can repeat after a real change. If the history holds no data-change commit within the latest 1000 commits, or within Delta's history retention, the version is unknown, and dependents build.

Unknown operations count as changes on purpose. A new Delta operation name can cause extra builds, never wrong reuses.

!!! info "Checked on a workspace"

    Checks on a Unity Catalog workspace confirmed the rules above. Zero-row WRITE, MERGE, UPDATE and DELETE commits show all row metrics at 0. A `CREATE OR REPLACE TABLE AS SELECT` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 and `numRemovedFiles` above 0. `COMMENT ON TABLE` is recorded as `SET TBLPROPERTIES`. A column comment, a widened column type (`ALTER COLUMN ... TYPE` with type widening), a changed nullability and a moved column (`ALTER COLUMN ... FIRST`) are all recorded as `CHANGE COLUMN` with no metrics, so `CHANGE COLUMN` counts as a data change: a new comment on a column of an upstream table rebuilds the nodes that read it. `RENAME COLUMN` has its own operation and counts too. ANALYZE writes no commit. `DESCRIBE HISTORY` on a view fails, so a view used as a source counts as unknown. `information_schema.tables.last_altered` does not move on data writes, so hodor reads the history of every table. Delta paths on Volumes were not tested.

## Limits

- A source that is not a Delta table (a foreign table, a view) has an unknown version, so dependents build.
- Not yet run in a large production project.
- `dbt-databricks` 1.13.0 merges `tblproperties` across config levels. A project that sets them at more than one level can see one build with `config_changed` after upgrading the adapter.
- The live tests on 1.13.0 do not cover views that use `*`, `tblproperties` set at several levels, `transaction: false` hooks, table tags or `persist_docs`.
- Delta paths on Volumes were not tested.
- On dbt v2, a table cloned into a dev target cannot be replaced in place when its base table has automatic liquid clustering (`clusterByAuto`; the test workspace set it on tables created without `CLUSTER BY`). The shallow clone keeps the property, and dbt v2 replaces the table with `CREATE OR REPLACE TABLE ... AS SELECT`, which fails with `CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR`. In the live test this happened when the model's code changed after the clone, and `--full-refresh` failed the same way. Dropping the table in the dev target let the model build. The server runs no SQL, so it cannot drop the clone itself. This is why `dbt-hodor serve` clones only with `--allow-clones`. On dbt-core this does not happen: dbt-databricks 1.12.7 drops a shallow clone before it replaces it, and the live test rebuilt a changed model over its auto-clustered clone.

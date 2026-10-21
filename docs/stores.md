# State stores

The state store holds one append-only table, `hodor_state`. The store loads the latest build records at run start and appends new records. It makes no decisions.

!!! danger "The state table decides what is skipped"

    Anyone who can write the table can force reuse. Grant `MODIFY` on it only to the job service principals. Everyone else gets `SELECT`.

| Store | Location | Created on first use |
| --- | --- | --- |
| `delta` | `<hodor_database>.<hodor_schema>.<hodor_table>` | the table only. The schema must exist, because creating one needs catalog-level grants |
| `duckdb` | table `<hodor_schema>.<hodor_table>` in the file `hodor_path` | the file, the schema and the table |
| `postgres` (alias `lakebase`) | `<hodor_schema>.<hodor_table>` in the Postgres database `hodor_database` | the schema and the table |

## delta

The default store on Databricks. Status: built, tested live on a Databricks workspace.

- Table: `<hodor_database>.<hodor_schema>.<hodor_table>`, for example `my_catalog.ops.hodor_state`. At run start hodor checks that the table exists (`SELECT * FROM <table> LIMIT 0`). Only when it is missing does hodor run `CREATE TABLE IF NOT EXISTS`.
- Layout: `CLUSTER BY (target_name, node_key)`, so the run-start read skips most files as history grows.
- The schema must exist. hodor never creates it.
- Job principals often hold `MODIFY` but not `CREATE TABLE` on the schema. Then an admin creates the table once, manually. Print the statement with:

    ```sh
    uv run dbt-hodor ddl --database my_catalog --schema ops
    ```

    If hodor cannot create the table, it turns itself off for the run with a warning that names this command.
- Writes: batched `INSERT INTO ... VALUES`, 500 rows per statement, through dbt's own adapter connection. hodor needs no other credentials or drivers.
- Concurrency: blind appends from concurrent jobs do not conflict in Delta.
- Grants: `MODIFY` (and `SELECT`) for the job service principals, `SELECT` for everyone else.

## duckdb

For local development and tests. Status: built and tested.

- One file, default `.hodor/state.duckdb`. It needs the `duckdb` package (`pip install dbt-hodor[duckdb]`).
- hodor creates the file, the schema (default `main`) and the table on first use.
- One process writes at a time, because DuckDB locks the file. A second dbt process that cannot open the file runs fail-open: everything builds, nothing is recorded, and hodor warns.
- Export to parquet: `COPY hodor_state TO 'x.parquet'`.

## postgres

Any Postgres, including Databricks Lakebase. Status: built, tested end to end against Lakebase. `lakebase` is accepted as an alias for `postgres`.

The point of this store is that state lives outside the warehouse: point reads and appends without SQL warehouse time, and a dbt project on any adapter can keep its state in a Postgres you already run.

- Table: `<hodor_schema>.<hodor_table>` in the database `hodor_database`. hodor creates the schema and the table on first use, so the connecting role needs `CREATE` on the database.
- It needs `psycopg` 3: `pip install dbt-hodor[postgres]`, or `dbt-hodor[lakebase]` which adds `databricks-sdk` for the token.
- Writes: one `executemany` per flush. Reads: one statement at run start.
- Concurrency: appends from concurrent jobs do not conflict.
- Connections are opened once per run and reopened once on a dropped connection or an expired token.

=== "Lakebase"

    `hodor_endpoint` is a Lakebase endpoint resource path. It supplies the host, the user and an OAuth token, so no password is stored anywhere. The token lasts an hour and is fetched again whenever hodor reconnects.

    ```yaml
    hodor_store: lakebase
    hodor_endpoint: projects/<project>/branches/<branch>/endpoints/<endpoint>
    hodor_database: databricks_postgres
    hodor_schema: ops
    ```

    Workspace credentials come from `databricks-sdk`, so `DATABRICKS_CONFIG_PROFILE`, or `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, select the workspace. Find the endpoint path with `databricks postgres list-endpoints projects/<project>/branches/<branch>`.

=== "Any other Postgres"

    ```yaml
    hodor_store: postgres
    hodor_host: db.example.com
    hodor_port: 5432
    hodor_user: hodor
    hodor_password: "{{ env_var('HODOR_PASSWORD') }}"
    hodor_database: hodor
    hodor_schema: ops
    hodor_sslmode: require
    ```

    Keep the password in an environment variable, not in `profiles.yml`. `HODOR_PASSWORD` sets it directly. Use `hodor_sslmode: disable` only for a local container.

## Record schema

Portable types only: text and bigint, with JSON stored as text. Times are epoch milliseconds in UTC. The same DDL works on Delta, DuckDB and Postgres; only the text type name differs.

| Column | Type | Meaning |
| --- | --- | --- |
| `record_id` | text | uuid |
| `invocation_id` | text | dbt invocation |
| `recorded_at_ms` | bigint | warehouse clock |
| `project_name`, `target_name` | text | dbt project and target |
| `node_key` | text | relation FQN in lower case for models; `<unique_id>#<sha of the tested FQNs>` for tests |
| `unique_id`, `resource_type` | text | dbt node |
| `mode` | text | `shadow` or `on` |
| `decision` | text | what the engine decided: `build` or `reuse` |
| `reason` | text | [reason code](how-it-works/decision-rules.md) |
| `action` | text | what actually happened: `build` or `reuse`. In shadow mode always `build` |
| `status` | text | dbt status: success, error, pass, warn, fail (or `reused`) |
| `text_hash`, `code_hash`, `unrendered_hash`, `config_hash`, `hash_version` | text | [hashing](how-it-works/hashing.md) |
| `sql_facts` | text (JSON) | stored SQL analysis for the fast path |
| `data_version` | text | data version token of the target after the build |
| `data_changed_at_ms` | bigint | commit time of `data_version`, when known |
| `built_at_ms` | bigint | start of the build that wrote this record (null for reuse) |
| `upstreams` | text (JSON) | `{table: data_version}` used for this decision |
| `test_result` | text (JSON) | status, failures, message (tests only) |
| `hodor_version` | text | package version |

At run start hodor loads the latest successful `build` record per `node_key`, for the project and target, within `hodor_lookback_days`. Successful means status `success`, `pass`, `warn` or `fail`. Shadow-mode builds count, so shadow mode builds the history that `on` mode needs.

## Retention

`dbt-hodor prune` deletes the old records of one project and target. It is a dry run unless `--yes`, and it counts first, so the dry run reports the same number the real run deletes.

```sh
uv run dbt-hodor prune                  # what would go, keeping hodor_lookback_days
uv run dbt-hodor prune --yes            # delete those records
uv run dbt-hodor prune --keep-days 30 --yes
```

It works on every store, reads the project and target the way `dbt-hodor explain` does, and never touches the records of another project or target that shares the table. Keeping fewer days than `hodor_lookback_days` drops records the next run still needs, so those nodes build once with `no_history`; the dry run warns when that would happen. On Delta it does not run `VACUUM`, because that has its own retention rules.

To clean up by hand instead, pick a cutoff in epoch milliseconds:

=== "Delta"

    ```sql
    DELETE FROM my_catalog.ops.hodor_state
    WHERE recorded_at_ms < <cutoff>;
    VACUUM my_catalog.ops.hodor_state;
    ```

=== "DuckDB"

    ```sql
    DELETE FROM main.hodor_state WHERE recorded_at_ms < <cutoff>;
    ```

=== "Postgres"

    ```sql
    DELETE FROM ops.hodor_state WHERE recorded_at_ms < <cutoff>;
    VACUUM ops.hodor_state;
    ```

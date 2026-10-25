# Records and retention

## Record schema

Portable types only: text and bigint, with JSON stored as text. Times are epoch milliseconds in UTC. The same DDL works on Delta, DuckDB, Postgres and Trino; only the type names differ (Delta also clusters the table).

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
| `reason` | text | [reason code](../how-it-works/decision-rules.md) |
| `action` | text | what actually happened: `build` or `reuse`. In shadow mode always `build` |
| `status` | text | dbt status: success, error, pass, warn, fail (or `reused`) |
| `text_hash`, `code_hash`, `unrendered_hash`, `config_hash`, `hash_version` | text | [hashing](../how-it-works/hashing.md) |
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

=== "Trino"

    ```sql
    DELETE FROM iceberg.ops.hodor_state
    WHERE recorded_at_ms < <cutoff>;
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

# Run flow

hodor loads inside the dbt process, so dbt is started as usual and no wrapper command is needed. For why this matters on Databricks jobs, see [Databricks](../adapters/databricks.md#jobs).

![hodor loads with dbt, reads the build records and data versions once, then for each node reuses or builds, queues a record, writes the records and prints a summary line](../assets/run-flow-light.svg#only-light)
![hodor loads with dbt, reads the build records and data versions once, then for each node reuses or builds, queues a record, writes the records and prints a summary line](../assets/run-flow-dark.svg#only-dark)

## Steps

1. **Plugin start.** With mode `off`, or for any command other than `run`, `build`, `test`, `seed` or `snapshot`, hodor installs nothing. If upstream `dbt_state` is also installed, hodor turns itself off with a warning, because both patch the same methods. On any adapter other than `databricks`, `duckdb`, `snowflake` or `trino`, hodor is off with a warning. It is also off with a warning when the `delta` store is set on an adapter other than `databricks`.
2. **Run start.** hodor opens the store and loads the latest record of every node in one query. It reads the warehouse clock in epoch milliseconds, so `lag_tolerance` does not depend on the local clock. It reads data versions in parallel, on `hodor_metadata_threads` threads. When the run has `on-run-start` hooks, it skips this step, because dbt runs those hooks afterwards and they may write to the tables it would read. Each node then reads its versions when it runs.
3. **Each model.** dbt has already compiled the node. hodor builds the node facts and calls the engine.
    - Reuse (mode `on`): hodor returns dbt's `Reused` status without calling the materialization. Hooks do not run unless `execute_hooks_on_any_reuse` is `true`.
    - Build: hodor calls the original dbt code, then reads the table's new data version and queues a record. It reads the new version after every build, also for nodes it always builds (Python models, microbatch), so downstream nodes in the same run never compare a version from before the build.
4. **Each test.** Same flow. A replayed test returns its recorded status (pass, warn or fail) and failure count. The result line says `(replayed)`.
5. **Records** are queued in memory. They are written every 200 records and at the end of the run, also after errors and interrupts.
6. **Summary line.** `dbt-hodor: N reused, M built, K fail-open errors`.

## Fail open

An exception anywhere in hodor for a node is logged as a warning, and the node builds as plain dbt would (reason `hodor_error`). hodor does not fail a dbt run, with three exceptions: a `state:` selector that cannot compare fails the run; an incremental node whose partial clone cannot be dropped fails that node; and on dbt-core, `--defer` without `--state` fails the run when hodor cannot supply the base target. See [Decision rules](decision-rules.md#state-selection) and [Defer without --state](decision-rules.md#defer-without-state). See also [Operations](../operations.md#fail-open).

## Import cost

dbt imports every `dbt_*` module on every command, including `ls` and `parse`. `import dbt_hodor` therefore loads nothing heavy. sqlglot and duckdb load only when hodor is active for one of those commands.

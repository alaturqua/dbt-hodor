# Operations

## Modes

| Mode | Behavior |
| --- | --- |
| `off` | hodor does nothing. |
| `shadow` (default) | hodor decides and records, but every node builds. Use it to check the decisions before you trust them. |
| `on` | hodor reuses nodes. |

Installing the package never skips anything until someone sets `on`.

Set the mode with `hodor_mode` in `profiles.yml`, with `HODOR_MODE`, or for one run with `--vars '{hodor_mode: off}'`. In `shadow` mode the summary line reads `dbt-hodor: N could be reused, M need a build (shadow: all built), K fail-open errors`. In `on` mode it reads `dbt-hodor: N reused, M built, K fail-open errors`.

## Rollout

1. Run `shadow` in each environment for about a week of scheduled runs. Shadow builds are history, so `on` can reuse right away afterwards.
2. Check the shadow records for **false reuses**: a record with mode `shadow`, decision `reuse`, and a `data_version` that differs from the node's previous build record. Fix every false reuse before you go on.
3. Switch the environment to `on`. Go environment by environment, for example dev, then test, then prod, each with `shadow` first.
4. Track the share of reused nodes, the run's wall time, and the number of false reuses (target: zero).

!!! note "Limits of the shadow check"

    The check is strong for incremental models: a build with no new input writes no rows. It is weak for `table` models, because `CREATE OR REPLACE` writes a new version with rows even when the content is identical. For tables, shadow mode measures the would-reuse rate but cannot prove each decision.

## Why a node was reused or built

`dbt-hodor explain` shows each node of one run with its decision and reason. It also shows the last build that the decision compared with: that build's time and invocation, the code and config hashes then and now, and each upstream data version then and now. It only reads the state table: it runs no dbt command and creates nothing.

```sh
dbt-hodor explain                          # every node of the latest run
dbt-hodor explain -s fct_orders -s dim_country
dbt-hodor explain --invocation <invocation_id> --json
```

| Option | Meaning |
| --- | --- |
| `-s`, `--select` | A unique_id, a relation or a node name. A name matches every version of a model. Repeat it for more nodes. |
| `--invocation` | The dbt `invocation_id` of the run. Default: the latest run of the project and target. |
| `--json` | Print the same data as JSON, for scripts and CI. |
| `--project-dir`, `--profiles-dir`, `--profile`, `-t`/`--target` | As in dbt. Unset options use dbt's defaults and `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR` and `DBT_TARGET`. |

hodor finds the store the way a dbt run does: it reads `profiles.yml` and `dbt_project.yml`, and `HODOR_*` env vars override the profile keys. The `delta` store is read through dbt's own Databricks connection, so the same credentials apply. `--vars` is not read.

In shadow mode, a line such as `mode shadow: hodor decided reuse` shows a node that hodor would have reused. Use it for step 2 of the rollout.

## Deleting old records

`dbt-hodor prune` deletes the records of this project and target that are older than the window you keep. It is a dry run unless you pass `--yes`.

```sh
dbt-hodor prune                      # what would go, keeping hodor_lookback_days
dbt-hodor prune --keep-days 30 --yes # delete anything older than 30 days
```

| Option | Meaning |
| --- | --- |
| `--keep-days N` | Keep the records of the last N days. Default: `hodor_lookback_days`. |
| `--yes` | Actually delete. Without it, prune only reports what it would delete. |
| `--project-dir`, `--profiles-dir`, `--profile`, `-t`/`--target` | As for `explain`. |

Records of other projects and targets in the same table are never touched. Keeping fewer days than `hodor_lookback_days` deletes records the next run still needs, so those nodes build once with `no_history`. The dry run warns when that would happen. On Delta, run `VACUUM` yourself afterwards if you want the files reclaimed.

On the `trino` store with an Iceberg catalog, every insert and every delete makes a snapshot, and old snapshots and small files stay until you remove them. To reclaim space, run these as a user that may alter the table:

```sql
ALTER TABLE iceberg.ops.hodor_state EXECUTE optimize;
ALTER TABLE iceberg.ops.hodor_state EXECUTE expire_snapshots(retention_threshold => '7d');
```

hodor does not run these. A Delta Lake catalog has its own `optimize` and `vacuum` procedures.

## Fail open

Every failure leads to a build, never to a reuse. hodor never fails a dbt run.

| Failure | Behavior |
| --- | --- |
| Exception while deciding a node | warning, normal build, record with decision `build` and reason `hodor_error` |
| Store cannot be opened or read at run start | warning, the whole run builds as normal dbt; records are still attempted |
| Store write fails | warning; the next run builds the affected nodes (`no_history`) |
| Metadata query fails for a table | that table is unknown, so its dependent nodes build |
| Invalid config value | warning, the default is used |
| Upstream `dbt_state` installed as well | hodor disables itself with a warning |
| Interrupt (Ctrl+C, job cancel) | queued records are flushed if possible; missing records only cause extra builds |

## Known limits

- **Adapter macro changes.** A change to dbt's or the adapter's own macros, for example a dbt-databricks upgrade, does not show in any hash. Run once with `HODOR_MODE=shadow` after such an upgrade: shadow builds every node and records the builds. `off` records nothing, so the old records would still count. Project and package macros that a materialization or a hook runs are in the config hash.
- **Volatile SQL stamps.** A volatile function in a stamp column (an output column of the final `SELECT` that reads no column, such as `current_date() as report_date`) does not count, so on reuse it keeps the value of the last build. Set `evaluate_volatile_sql: true` where a stamp must follow the clock. `is_member()` is approximated by the current user.
- **First incremental run.** The first incremental run after a full build compiles different SQL (the `is_incremental()` branch), so it builds once more.
- **CTE renames** count as code changes and cause one extra build.
- **Untraceable inputs.** `read_files(...)`, other table functions and non-Delta sources are unknown, so their dependents always build.
- **`--empty` runs.** dbt-hodor is off for the whole run: dbt builds every node and hodor records nothing. An empty run loads seeds and tables with zero rows, and a record of it could let a later run reuse them. The next normal run builds the nodes whose data changed. Snapshots are not emptied and keep their data.
- **DuckDB state file.** One process at a time.
- **Databricks.** Not yet run in a large production project. Delta paths on Volumes are not tested.

## Scope

Supported: models materialized as `table`, `incremental` (append, merge, delete+insert, insert_overwrite, replace_where; an unset strategy counts as merge) and `view`, seeds, snapshots, and data tests (generic and singular). Adapters: `dbt-databricks` 1.12.7, `dbt-duckdb`, `dbt-trino` 1.10.6 (Iceberg and Delta Lake connectors only; see [Data versions](adapters/trino.md#data-version)) and `dbt-snowflake` 1.12.4 (state in DuckDB or Postgres; see [Data versions](adapters/snowflake.md#data-version)), with dbt-core 1.12.x.

Always built as plain dbt:

- the node configs `pre_clone`, `allow_clones` and `defer_to_target` (for `--defer` and `dbt clone` in CI, see [Slim CI](slim-ci.md))
- Python models
- microbatch models
- materialized views and streaming tables
- metric views
- custom materializations
- custom incremental strategies (reason `unsupported`)
- unit tests
- dbt v2 (Fusion)

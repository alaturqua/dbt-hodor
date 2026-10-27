# dbt-hodor design

Status: approved 2026-10-20
Name: **hodor**, for "Hold Old Data, Or Rebuild". Distribution `dbt-hodor`, Python module `dbt_hodor`.

## 1. Goal

dbt-hodor is a free, self-hosted alternative to dbt Labs' dbt State. During a dbt run it decides for each selected node whether to **reuse** the existing object or **build** it. It reuses a node when the node's logic is unchanged and its upstream data has not changed since the last build, or changed within the node's `lag_tolerance`.

Requirements:

- Runs inside dbt-core 1.12.x with dbt-databricks 1.12.7 on Unity Catalog. Development and tests use dbt-core 1.12.5.
- Needs no dbt platform account and sends nothing to external services.
- Stores state in a Delta table (default on Databricks), in a Trino table, in a Postgres database such as Databricks Lakebase, or in a local DuckDB file.
- Uses the dbt State model configs (`state:` block), so projects can switch between the two.
- Works in Databricks `dbt_task` jobs. Every command in a `dbt_task` must start with `dbt` (`databricks/sdk/service/jobs.py:1562`), so a wrapper CLI is not an option. hodor must load inside the dbt process.
- Supported adapters: `databricks` for real environments, and `duckdb` (dbt-duckdb) for local development and tests. Local work runs end to end on DuckDB, with no Databricks access. `trino` (dbt-trino) is supported for the Iceberg and Delta Lake connectors (Delta Lake connector: unit tests only). `snowflake` (dbt-snowflake 1.12.4) is supported with the state in DuckDB or Postgres. hodor disables itself on any other adapter, and on any adapter other than `databricks` when the store is `delta`.

Main target: large scheduled dbt runs on Databricks that rebuild every model in scope on every run, even when most inputs have not changed.

## 2. Background

- dbt State consists of an open-source client (Apache-2.0, `github.com/dbt-labs/dbt-state`, commit `b0f3030`) and a closed gRPC service. The client collects inputs (compiled SQL, upstream last-modified times, configs) and runs the actions. The service makes every decision: SQL normalization and hashing, freshness and `lag_tolerance`, clone candidates, and explain text. hodor reuses the client's dbt integration and replaces the service with local code.
- dbt-core 1.12.5 has no dbt State logic. It does define `RunStatus.Reused` (`dbt/artifacts/schemas/results.py:68,77`): this status counts as success, does not skip children, is not retried, and is shown in the run summary.
- dbt plugins load automatically from any installed top-level module named `dbt_*` that has a `plugins` list (`dbt/plugins/manager.py:84-85,134-140`). The plugin hooks run at parse time only, so changing node execution needs monkeypatches, which is what upstream does. All upstream patch points exist unchanged in dbt-core 1.10.19 to 1.12.6.
- The module names `dbt_state` and `dbt_run_cache` are only loaded behind a flag in dbt-core 1.12 betas and 1.13, so hodor uses `dbt_hodor`.
- dbt-databricks adapter responses carry no `rows_affected` (`dbt/adapters/databricks/handle.py:92-97`). Data changes must therefore come from the Delta commit log.

## 3. Scope

### In scope

- Per-node decision for `dbt run`, `dbt build`, `dbt test`, `dbt seed` and `dbt snapshot`: reuse or build.
- Models materialized as `table`, `incremental` (append, merge, delete+insert, insert_overwrite, replace_where) and `view`. An unset incremental strategy counts as merge, the dbt-databricks default (upstream wrongly maps it to append). Custom strategies build as unsupported. These are the dbt-databricks 1.12.7 strategies (`valid_incremental_strategies`, `impl.py:962`) except `microbatch`.
- Seeds and snapshots. Both follow the table rules. A seed's code hash is the sha256 of its CSV file with line endings normalized: dbt's own checksum covers only the path for seeds above `--maximum-seed-size-mib`. `dbt seed --show` always builds, because it prints each result's loaded table.
- Data tests (generic and singular): the recorded result is replayed when nothing they test has changed.
- Model configs `lag_tolerance`, `require_fresh_data_from`, `compare_unrendered_code`, `execute_hooks_on_any_reuse`.
- `--full-refresh` always builds.
- Modes `off`, `shadow` (decide and record, but always build) and `on`.
- State stores `delta`, `trino`, `duckdb` and `postgres` (alias `lakebase`).
- `dbt-hodor explain` and `dbt-hodor prune`.

### Out of scope, always built as normal dbt

- The node configs `pre_clone`, `allow_clones` and `defer_to_target`, and `state:*` selectors backed by hodor. dbt's own `--defer` is supported (see [Slim CI](slim-ci.md)).
- Python models, microbatch models, materialized views, streaming tables, metric views, custom materializations and unit tests. Reasons: microbatch windows depend on the run time, so a reuse could skip a new window; Python models have no SQL to trace inputs from; dbt-databricks already turns an unchanged metric view into `select 1`; unit tests may come later.
- dbt v2 (Fusion).

Configs that are out of scope are accepted and ignored, with one warning per run.

## 4. Architecture

```text
dbt process
 └─ dbt_hodor (auto-loaded plugin)
     ├─ plugin.py     initialize(): mode check, command gate, guard, install patches
     ├─ patches.py    ModelRunner.execute, TestRunner execute and result line, run-end flush
     ├─ config.py     state: keys, hodor settings, precedence
     ├─ hashing.py    normalized SQL and config hashes (sqlglot, dialect of the target's adapter)
     ├─ lineage.py    upstream tables per node: dbt parents, sources, external inputs
     ├─ metadata.py   data versions: tokens and Delta history rules
     ├─ providers.py  per adapter queries: Databricks, Trino, Snowflake, DuckDB
     ├─ engine.py     pure decision rules (no I/O)
     ├─ store/        StateStore protocol: delta.py, trino.py, duckdb.py, postgres.py
     └─ cli.py        dbt-hodor ddl, explain, prune
```

Each unit has one job:

- **engine.py** is a pure function. It takes the node facts, the latest record and the upstream versions, and returns a decision with a reason code. All decision rules live here and are tested without dbt or a warehouse.
- **hashing.py** turns compiled SQL plus config into stable hashes.
- **lineage.py** and **metadata.py** answer one question per table: what is its current data version, and when did its data last change?
- **store/** loads the latest records at run start and appends new records. It knows nothing about decisions.
- **plugin.py**, **state_config.py** and **patches.py** are the only modules that touch dbt internals. `state_config.py` holds the one upstream-derived piece of logic, the `state:` config merge, on its own so the attribution is precise. They are derived from upstream `plugin.py` and `runner.py`, keep their Apache-2.0 headers, and are trimmed to what hodor needs. Nothing is taken from upstream's `run_cache.py`: hodor's engine replaces it.

The upstream gRPC, auth, telemetry, dispatcher, session and version-check code (about 2,000 lines) is not carried over. Neither are the upstream request and response models: the engine uses small dataclasses of its own.

### 4.1 Run flow

1. **initialize()** reads the mode. With `off`, or for any command other than `run`, `build` or `test`, it installs nothing. If upstream `dbt_state` is also installed, hodor disables itself with a warning, to avoid patching the same methods twice. Otherwise it installs the patches.
2. **First node, before execution:**
   - open the store and load the latest record for every selected node with one query;
   - read the warehouse clock in epoch milliseconds with one query (`unix_millis(current_timestamp())` on Databricks, `epoch_ms(current_timestamp)` on DuckDB);
   - prefetch metadata (section 7.3).
3. **Each model node** (inside the patched `ModelRunner.execute`; dbt has already compiled the node):
   - build the node facts;
   - call the engine;
   - **reuse:** return `RunResult.from_node(node, RunStatus.Reused, "<reason>")` without calling the materialization. Hooks do not run unless `execute_hooks_on_any_reuse` is true;
   - **build:** call the original `execute`, then read the table's new version and data version from the metadata provider, update the run's metadata cache, and queue a record.
4. **Each test node:** the same flow. A replayed test returns its recorded `TestStatus` (pass, warn or fail) with the recorded failure count and a "replayed" message. `TestStatus` has no `Reused` value, so the patched result line shows the replay.
5. **Records** are queued in memory and flushed in batches: every 200 records, and at the end of the run, including after errors and interrupts.
6. **Run summary:** one line, `hodor: N reused, M built, K fail-open errors`.

## 5. Decision rules

The engine checks the following rules in order. The first rule that matches decides. Reason codes go into the record and the dbt log line.

| # | Condition | Decision | Reason code |
| --- | --- | --- | --- |
| 1 | mode is `shadow` | build anyway; the record keeps the decision and reason from the other rules, with mode `shadow` | reason from the other rules |
| 2 | `--full-refresh`, or the node config sets `full_refresh: true` | build | `full_refresh` |
| 3 | resource type, materialization or incremental strategy is out of scope (section 3) | build | `unsupported` |
| 4 | no earlier successful build record for this node | build | `no_history` |
| 5 | the relation is missing from the warehouse (dbt's relation cache) | build | `relation_missing` |
| 6 | the code hash differs from the record (with `compare_unrendered_code`: rendered and unrendered hashes both differ) | build | `code_changed` |
| 7 | the config hash differs from the record | build | `config_changed` |
| 8 | tables, incrementals and views: the target's current data version differs from the data version recorded after its last build (something wrote data to it outside this node's builds). Maintenance commits such as OPTIMIZE do not change the data version, so they never trigger this rule. A view's data version is a hash of its definition in the warehouse | build | `target_modified` |
| 9 | a view whose final `SELECT *` reads directly from a `ref()` or `source()` | build | `select_star_view` |
| 10 | the node is a view (its definition is unchanged, see rule 6) | reuse | `view_unchanged` |
| 11 | any upstream table has an unknown data version (section 7.2) | build | `unknown_upstream` |
| 12 | a view between this node and its upstream tables has other code than at the node's last build | build | `upstream_code_changed` |
| 13 | no upstream changed since the last build (`any`), or not all of them changed (`all`) | reuse | `no_new_data` |
| 14 | upstream changed, but the last build is younger than `lag_tolerance` | reuse | `within_lag_tolerance` |
| 15 | otherwise | build | `new_upstream_data`, or `volatile_sql` when only volatile values changed |

Rule 10 follows dbt State: a view always shows current upstream data, so new data alone never requires rebuilding a view. It comes before rule 11 because a view stores no data, so an unknown upstream does not make a view stale. Rule 12 covers the other side: a node that reads through a view builds when the code hash of that view's last build changed. A view without a build record counts as unknown (rule 11).

- **"Upstream changed"**: an upstream table's current data version differs from the data version recorded for it at the node's last build. Versions are compared for equality, not order, and the token holds the commit time, so a dropped and recreated table (Delta versions restart at 0) still counts as changed.
- **"Last build"**: the `built_at_ms` of the latest record with action `build`. Reuse records do not reset the clock.
- **Tests** use every rule except the view rules (`select_star_view`, `view_unchanged`) and `within_lag_tolerance`. Their upstreams are the nodes they test. `lag_tolerance` is always 0 for tests, as in upstream.
- **Fail open.** An exception anywhere in hodor for a node is logged as a warning, and the node builds as normal dbt would (reason `hodor_error`). hodor never fails a dbt run.

## 6. Hashing

### Code hash

1. Take the node's compiled SQL.
2. Replace this run's `invocation_id` and `run_started_at` values with fixed placeholders. Many projects inline `'{{ invocation_id }}'` into every model, for example as an audit column.
3. Parse with sqlglot in the dialect of the target's adapter (the adapter type, for example `databricks`). Run `qualify(dialect=<adapter type>, validate_qualify_columns=False, expand_stars=False, identify=True, canonicalize_table_aliases=True)`. This normalizes keyword and identifier case, quoting and table aliases.
4. Generate SQL without comments and hash it with sha256.
5. If `qualify` fails, hash the parsed SQL generated without comments. If parsing fails, hash the compiled text with whitespace collapsed. Both fallbacks can only cause extra builds, never wrong reuses.

CTE renames still count as changes, which costs one extra build. `canonicalize_internal_names` (sqlglot) is not used. Without a schema it gave `with s as (select a, b from t) select a, b from s` and `with s as (select z, b from t) select z as a, b from s` the same output (tested with sqlglot 30.22.0), which would cause wrong reuses.

### Fast path

- `text_hash` is the sha256 of the compiled SQL after step 2. When it equals the record's `text_hash`, hodor takes the SQL analysis from the record: code hash, select-star flag and table references (`sql_facts`). sqlglot does not run.
- For a changed node, sqlglot runs once: one parse feeds the code hash, the select-star check (rule 9) and the table references (section 7.2).
- Measured on a sample of compiled model files (sqlglot 30.22.0, pure Python), projected to 10,000 models:
  - sha256 takes 0.01 ms per file, about 0.1 s in total;
  - a full sqlglot analysis takes about 26 ms on average and several seconds for files near 1 MB, about 4 minutes of single-threaded CPU in total;
  - every sampled file passed `qualify`.
- A run with no code changes therefore spends about 0.1 s on hashing instead of about 4 minutes. The first run, and runs after large macro changes, pay the sqlglot cost for the changed nodes only.

### Unrendered hash

Used only with `compare_unrendered_code`. It is the sha256 of `raw_code`, the `macro_sql` of every macro in `depends_on.macros` (resolved recursively), and `unrendered_config`.

### Config hash

The sha256 of the node config (which holds the contract settings and `row_filter`), the column data types, constraints, `column_mask` and `databricks_tags`, the model-level constraints, and the `macro_sql` of project and package macros that the materialization or the hooks run. These keys are removed first because they do not change the produced data: `databricks_compute`, `incremental_apply_config_changes`, `tags`, `meta`, `docs`, `state`, `enabled`, `persist_docs`, column descriptions.

### Hash version

Every record carries `hash_version`, which is hodor's own algorithm version. A record with a different `hash_version` counts as `code_changed`. The sqlglot version range is pinned, and golden tests catch output drift before a sqlglot upgrade.

### Consequences

- A macro change that leaves a node's compiled SQL unchanged does not rebuild that node. `state:modified.macros` fans such changes out to every model that uses the macro.
- A materialization-macro change, for example a dbt-databricks upgrade, is not visible in the compiled SQL. For such upgrade runs, run once with `HODOR_MODE=shadow`, which builds every node and records the builds. This is documented as a known limit.
- The first incremental run after a full build compiles different SQL (the `is_incremental()` branch). That causes one extra build, as with dbt State.

## 7. Upstream data changes

### 7.1 Data version of a Delta table

A data version is an opaque text token. The engine only compares tokens for equality. Each adapter has a metadata provider that produces the tokens.

**Databricks (Delta history).** `DESCRIBE HISTORY <table>` lists commits with `version`, `timestamp`, `operation` and `operationMetrics`.

- Known maintenance and metadata operations never count as a data change: OPTIMIZE, VACUUM START, VACUUM END, ANALYZE, COMPUTE STATS, SET TBLPROPERTIES, UNSET TBLPROPERTIES, CHANGE COLUMN (column comments from `persist_docs`), ADD CONSTRAINT, DROP CONSTRAINT, UPGRADE PROTOCOL, SET TAGS, UNSET TAGS, CLUSTER BY, REORG, FSCK.
- Every other operation counts as a data change, unless its change metrics are all zero. The change metrics are `numOutputRows`, `numRemovedFiles`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numDeletedRows`, `numUpdatedRows` and `numCopiedRows`. A commit without any of them counts as a change. `numRemovedFiles` is there because a `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows` at 0 but still empties the table.
- The token is the version and the commit time of the latest data-change commit, for example `7@1792465200000`. The commit time matters: a table that is dropped and recreated, or swapped in from a staging table (`use_safer_relation_operations` with `use_materialization_v2`), restarts its versions, so the version alone can repeat after a real change. If the history window (`LIMIT 20`) holds no data-change commit, hodor reads up to 1000 commits once more; if those hold none either, the version is unknown, so dependents build.
- Unknown operations count as changes on purpose: a new Delta operation name can cause extra builds, never wrong reuses. The spike (section 11) confirms operation names and how zero-row writes appear.

**DuckDB (content fingerprint).** DuckDB has no table history. The token is the row count plus an order-independent sum of row hashes, computed with one query per table. That is fine for local test data sizes.

### 7.2 Upstream tables of a node

- **dbt parents** (`depends_on.nodes`):
  - tables and incremental models give their own data version;
  - views and ephemeral models are expanded through the manifest to their own upstream tables, recursively;
  - for metric views and other unsupported types, the version is unknown.
- **Sources:** their Delta data version. A non-Delta source (foreign table, view) is unknown.
- **External inputs:** table references in the compiled SQL that match no parent or source.
  - ``delta.`<path>` `` gives the Delta data version of that path;
  - `read_files(...)`, other table functions and any other reference are unknown.
  - Consequence: models that read files (CSV through `read_files`) always build; models that read Delta paths or sources can be reused.
- An unknown version anywhere makes the node build (rule 11).

### 7.3 Cost control

- One query per catalog against `system.information_schema.tables` gives `last_altered` for all relevant tables. A table whose `last_altered` is not newer than the time it was last checked is unchanged and needs no history query. This shortcut is used only if the spike shows that `last_altered` moves on every commit that writes data. The spike showed it does not (section 11.1), so the shortcut is not used.
- Every other table gets `DESCRIBE HISTORY <t> LIMIT 20` (`LIMIT 1000` once more when the 20 hold only maintenance commits), run in parallel on a small thread pool (default 8) and cached for the run.
- After hodor builds a table, it reads that table's history once and updates the cache, so downstream nodes in the same run see the new version.
- `metadata.py` stays behind a small interface (`versions(tables) -> {table: DataVersion}`), so the shortcut can be switched on or off without touching the engine.

## 8. State store

### 8.1 Record schema (`hodor_state`, append-only)

Portable types only: text and bigint, with JSON stored as text. Times are epoch milliseconds in UTC (bigint), which avoids session time zone problems on every backend. The same DDL works on Delta, DuckDB and Postgres; only the text type name differs (`STRING`, `VARCHAR`, `TEXT`).

| Column | Type | Meaning |
| --- | --- | --- |
| record_id | text | uuid |
| invocation_id | text | dbt invocation |
| recorded_at_ms | bigint | warehouse clock |
| project_name, target_name | text | dbt project and target |
| node_key | text | relation FQN in lower case for models; `<unique_id>#<sha of the tested FQNs>` for tests |
| unique_id, resource_type | text | dbt node |
| mode | text | `shadow` or `on` |
| decision | text | what the engine decided: `build` or `reuse` |
| reason | text | reason code from section 5 |
| action | text | what actually happened: `build` or `reuse`. In shadow mode always `build` |
| status | text | dbt status: success, error, pass, warn, fail |
| text_hash, code_hash, unrendered_hash, config_hash, hash_version | text | section 6 |
| sql_facts | text (JSON) | stored SQL analysis for the fast path: select-star flag, table references, Delta paths, opaque inputs |
| data_version | text | data version token of the target after the build (section 7.1) |
| data_changed_at_ms | bigint | commit time of `data_version`, when known |
| built_at_ms | bigint | start of the build that wrote this record (null for reuse) |
| upstreams | text (JSON) | `{table: data_version}` used for this decision |
| test_result | text (JSON) | status, failures, message (tests only) |
| hodor_version | text | package version |

**Load at run start**: the latest successful `build` record per `node_key`, for the project and target, within the lookback window (`hodor_lookback_days`, default 90), in one query. Successful means action `build` (or `carry`, a copy of a build record that a long-reused node writes after half the window) and status `success`, `pass`, `warn` or `fail`. The query uses a `ROW_NUMBER()` subquery, not `QUALIFY`, because Postgres has no `QUALIFY`. Shadow-mode builds count, so shadow mode builds the history that `on` mode needs.

**False reuse** (section 14): a record with mode `shadow`, decision `reuse`, and a `data_version` that differs from the node's previous build record.

**Retention**: `dbt-hodor prune` deletes the records of one project and target older than a kept window (default `hodor_lookback_days`). It counts first and deletes only with `--yes`, so a dry run reports the same number. It never touches another project or target in the same table, and it does not run VACUUM on Delta, which has its own retention rules. Manual cleanup stays possible: `DELETE FROM hodor_state WHERE recorded_at_ms < <cutoff>`.

### 8.2 `delta` store (default on Databricks)

- **Table:** one per environment catalog, in an operations schema chosen with `hodor_schema`: `<catalog>.<schema>.hodor_state`, for example `my_catalog.ops.hodor_state`. Created on first use if the job identity has `CREATE TABLE` on the schema; otherwise an admin creates it once with `dbt-hodor ddl`.
- **Layout:** `CLUSTER BY (target_name, node_key)`, so the run-start read and the lookback filter skip most files as history grows.
- **Writes:** batched `INSERT INTO ... VALUES`, 500 rows per statement, run through dbt's own adapter connection. hodor needs no other credentials or drivers.
- **Concurrency:** blind appends from concurrent jobs do not conflict in Delta.
- **Security:** anyone who can write this table can force reuse in prod. Grant MODIFY only to the job service principals; everyone else gets SELECT.
- **Input validation:** the table name is checked against `^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$` and quoted. Values are escaped.

### 8.3 `duckdb` store (local)

- One file, default `.hodor/state.duckdb`. Needs the optional `duckdb` dependency (`pip install dbt-hodor[duckdb]`).
- One process writes at a time, because DuckDB locks the file. A second dbt process that cannot open the file runs fail-open: everything builds and nothing is recorded, with a warning.
- DuckDB reads and writes parquet natively, so no separate parquet store is needed. For export: `COPY hodor_state TO 'x.parquet'`.

### 8.4 `postgres` store

Any Postgres, Databricks Lakebase included. `lakebase` is an alias for `postgres`.

- Postgres through `psycopg` 3 (`dbt-hodor[postgres]`). For Lakebase, `hodor_endpoint` is an endpoint resource path and `dbt-hodor[lakebase]` adds `databricks-sdk` (0.81 or newer, for `w.postgres`), which supplies the host, the user and the OAuth token.
- **Token refresh:** a Lakebase token lasts 1 hour and a large daily run can take longer, so the credential is fetched again on every reconnect. One reconnect is attempted on an `OperationalError`, which also covers a connection dropped while idle.
- **Benefit:** point reads and appends without SQL warehouse time, and state for a project on any adapter.
- **Design rule:** the same table schema, and the same `load` / `append` interface as the other stores.
- hodor creates the schema and the table on first use, so the connecting role needs `CREATE` on the database.

## 9. Configuration

### 9.1 Node configs (dbt State compatible, under `state:`)

| Key | Default | Accepted values |
| --- | --- | --- |
| lag_tolerance | `45m` | duration string (`0s`, `45m`, `4h`, `1d`, `1w`), integer seconds, or `{count, period}` (the dbt-core 1.12.5 `ModelState` schema form) |
| require_fresh_data_from | `any` | `any`, `all` |
| compare_unrendered_code | `false` | boolean |
| execute_hooks_on_any_reuse | `false` | boolean. `execute_hooks_on_reuse`, the spelling in the dbt-core 1.12.5 schema, is also accepted |
| evaluate_volatile_sql | auto | boolean. Auto: volatile functions that decide rows or values are upstreams (`volatile:date`, `volatile:session`, `volatile:always`), stamp columns are not. `true`: stamps count too. `false`: nothing counts |
| pre_clone | ignored | warning once |

- **Where to set them:** in YAML (`dbt_project.yml` `+state:` or the model properties file). dbt-core 1.12.5 warns (D027) about these keys in SQL `config()`.
- **Merging:** hodor installs upstream's merge patch, so `state:` is merged key by key across project, folder and model levels (upstream `plugin.py:170-229`).

### 9.2 hodor settings

Set as keys on the active `profiles.yml` target. An environment variable of the same name overrides the profile key.

| Profile key | Env var | Default | Values |
| --- | --- | --- | --- |
| hodor_mode | HODOR_MODE | `shadow` | `off`, `shadow`, `on` |
| hodor_store | HODOR_STORE | `trino` on the `trino` adapter, `delta` on every other adapter | `delta`, `duckdb`, `postgres`, `trino` |
| hodor_database | HODOR_DATABASE | the target's `catalog` (or `database`), for `delta` and `trino` only | Databricks or Trino catalog, or Postgres database (required, no default: it is unrelated to the dbt target); not used by `duckdb` |
| hodor_schema | HODOR_SCHEMA | none for `delta`, `trino` and `postgres` (required); `main` for `duckdb` | schema name |
| hodor_table | HODOR_TABLE | `hodor_state` | table name |
| hodor_path | HODOR_PATH | `.hodor/state.duckdb` | DuckDB database file |
| hodor_metadata_threads | HODOR_METADATA_THREADS | `8` | integer 1-64 |
| hodor_lookback_days | HODOR_LOOKBACK_DAYS | `90` | integer 1-3650; a node not built within this window builds once (`no_history`) |

- **Default mode:** `shadow`, so installing the package never skips anything until someone opts in.
- **Store location:** database, schema and table are separate settings, each checked against `^[A-Za-z0-9_][A-Za-z0-9_-]*$` and always quoted. `delta` has no default schema, so hodor writes nothing to a catalog until someone names one; without it hodor is off with a warning. `delta` never creates the schema (that needs catalog-level grants); `duckdb` and `postgres` create schema and table.
- **Env prefix:** `HODOR_` keeps clear of dbt's reserved `DBT_ENGINE_` prefix.
- **Per run:** `hodor_<name>` keys in `--vars` override the profile key. Precedence: env var, `--vars`, profile key, default. A plugin cannot add CLI flags: dbt parses them before plugins load (`dbt/cli/requires.py:333`), so `--no-hodor` fails with `No such option`. `--manage-state` does not exist in dbt-core 1.12, and `DBT_ENGINE_MANAGE_STATE` warns there (`dbt/env_vars.py:31-41`). Both switch on dbt Labs' dbt State in 1.13, so hodor does not read them. YAML booleans `true`/`false` (unquoted `on`/`off`) are accepted for `hodor_mode`.
- **Open item:** whether dbt-databricks accepts unknown keys in a profile target is checked in the spike. If it does not, the settings move to `dbt_project.yml` `vars: {hodor: {...}}`, still with env var override.

### 9.3 CLI

`dbt-hodor explain [-s <unique_id or name>] [--invocation <id>] [--json] [--summary]` reads decisions from the store configured for the current project and target:

- `duckdb`: directly;
- `delta`: through `databricks-sql-connector`, using the profile's connection settings. The connector is already a dependency of dbt-databricks.

It prints each decision with its reason, the hashes compared and the upstream versions.

How it works:

- It loads the profile and project with dbt's own loaders (`load_profile`, `load_project`, `RuntimeConfig.from_parts`), so `env_var()`, targets and `HODOR_*` overrides work as in a run. `--vars` is not read.
- The `delta` store is read through the dbt adapter connection, not a second connector. The store opens read-only: DuckDB with `read_only=True`, Delta without the probe and `CREATE TABLE`.
- Default: the latest invocation of the project and target. Each record is compared with the latest successful build before the invocation's first record, inside `hodor_lookback_days`, which is the same window the run loaded.
- `-s` matches a unique_id, a relation or a node name, and filters inside one invocation. With no match it exits with a hint to pass `--invocation`.

Every dbt log line for a reused node also states the reason. For example: `REUSED no_new_data (last build 2026-10-19 04:12 UTC)`.

## 10. Error handling

| Failure | Behavior |
| --- | --- |
| Exception while deciding a node | warning, normal build, record with decision `build` and reason `hodor_error` |
| Store cannot be opened or read at run start | warning, the whole run builds as normal dbt; records are still attempted |
| Store write fails | warning; the next run builds the affected nodes (`no_history`) |
| Metadata query fails for a table | that table is unknown, so its dependent nodes build |
| Invalid config value | warning, the default is used |
| Upstream `dbt_state` installed as well | hodor disables itself with a warning |
| Interrupt (Ctrl+C, job cancel) | queued records are flushed if possible; missing records only cause extra builds |

Every failure leads to a build, never to a reuse. That is the safety rule.

## 11. Spike (before engine work)

Run on the dev workspace, using a Databricks profile the user chooses, in a scratch schema:

1. Does `information_schema.tables.last_altered` move on: INSERT with rows, INSERT with 0 rows, MERGE with rows, MERGE with 0 rows, CREATE OR REPLACE TABLE AS SELECT, OPTIMIZE, VACUUM, ALTER TBLPROPERTIES, COMMENT?
2. For the same operations: does `DESCRIBE HISTORY` show a commit, with which `operation` and which `operationMetrics`?
3. Does `DESCRIBE HISTORY delta.`/Volumes/...`` work, and how long does `DESCRIBE HISTORY ... LIMIT 20` take per table on an etl warehouse?
4. Does dbt-databricks 1.12.7 accept `hodor_*` keys in a profile target?
5. Can the job identity create and insert into `<catalog>.<schema>.hodor_state`? (Read the grants only.)
6. Does `dbt_artifacts.upload_results` handle the status `reused`?

### 11.1 Spike results (2026-10-20, a Unity Catalog dev workspace, scratch schema)

1. **`last_altered` does not move on data writes.** It stayed the same after INSERT (2 rows and 0 rows), MERGE (0 and 1 rows), UPDATE, DELETE and INSERT OVERWRITE. It moved on CREATE TABLE, SET TBLPROPERTIES, column and table comments, ANALYZE, CREATE OR REPLACE TABLE AS SELECT and CLUSTER BY. The shortcut in section 7.3 is therefore not used; every table gets `DESCRIBE HISTORY`.
2. **History matches the rules in section 7.1.** Zero-row WRITE, MERGE, UPDATE and DELETE commits appear with all row metrics at 0 (MERGE: `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numOutputRows` all 0), so they are not data changes. A `CREATE OR REPLACE TABLE AS SELECT` or `INSERT OVERWRITE` that writes 0 rows shows `numOutputRows=0` but `numRemovedFiles` above 0, so it counts. A one-row MERGE shows `numTargetRowsUpdated=1`, `numOutputRows=1`. `COMMENT ON TABLE` is recorded as `SET TBLPROPERTIES`, a column comment as `CHANGE COLUMN`. ANALYZE writes no commit. OPTIMIZE on a single-file table writes no commit. VACUUM writes `VACUUM START` and `VACUUM END`. `operationMetrics` values are strings.
3. `DESCRIBE HISTORY` takes about 2 s per call through the CLI, including CLI start-up; the statement time itself is lower. `DESCRIBE HISTORY` on a view fails with `EXPECT_TABLE_NOT_VIEW`, so the version is unknown and dependents build. Delta paths on Volumes were not tested.
4. dbt-databricks accepts `hodor_*` keys in a profile target (validation passes) but drops them from the credentials. hodor reads them from `profiles.yml` itself and renders them with dbt's `ProfileRenderer`.
5. A typical job identity has `USE SCHEMA`, `SELECT`, `MODIFY` and `EXECUTE` on its operations schema, but not `CREATE TABLE`. The Delta store therefore probes the table first and creates it only when missing; an admin creates it once with `dbt-hodor ddl`. Every identity with `MODIFY` on that schema can write the state table, so check who holds it.
6. dbt_artifacts 2.11.1 uploads `status` as text, so reused models are stored with status `reused`. Its own models filter on `status = 'success'` (for example `dim_dbt__current_models.sql:35`, `fct_dbt__model_performance.sql:37`), so reused runs drop out of performance and consumption figures, and "current models" keeps the last real build. Replayed tests keep `pass`, `warn` or `fail`, with an execution time near 0.

The results feed sections 7.1, 7.3 and 9.2 before the engine is built.

## 12. Testing

- **Engine:** table-driven unit tests, one case per rule and reason code, plus `any`/`all` and `lag_tolerance` boundaries.
- **Hashing:** golden tests for whitespace, comments, alias and CTE renames, `invocation_id` replacement, `select *` view detection and the parse-failure fallback. A coverage run over a large project's compiled SQL reports the parse success rate.
- **Config:** every `lag_tolerance` form, precedence between env vars and profile keys, ignored keys.
- **Store contract:** one shared test suite run against DuckDB (in memory), and a SQL-generation suite for Delta (statements asserted as text).
- **Patches:** tests against the installed dbt-core (the version range in `pyproject.toml`) that check every patched symbol exists with the expected signature, so a dbt upgrade fails in CI instead of at run time.
- **Local end to end (default for development):** a small dbt project in `tests/e2e/` run with dbt-duckdb and the DuckDB state store through `dbtRunner`. First run builds and records, second run reuses, a changed upstream rebuilds, a changed model rebuilds, tests replay. Needs no Databricks access and runs in every test run.
- **Databricks integration:** the same scenarios on a live workspace in a scratch schema that is dropped afterwards (`pytest -m databricks`, `tests/integration/test_databricks.py`). [Development](development.md#tests) lists what it covers.

## 13. Packaging (uv)

- uv manages the project and builds it. Layout: `uv init --lib` (src layout), build backend `uv_build`, `requires-python = ">=3.10"` (dbt-core 1.12.5 requires 3.10 or newer), development on Python 3.12 to match DBR 18.
- **Dependencies:** `dbt-core>=1.12,<1.13` and `sqlglot>=30.22,<31`. No adapter is a hard dependency; hodor disables itself on adapters other than databricks, duckdb, snowflake and trino.
- **Extras:** `duckdb` (`duckdb>=1.0`), `postgres` (`psycopg[binary]>=3.2`) and `lakebase` (adds `databricks-sdk>=0.81` for the Lakebase token).
- **Dev group:** pytest, ruff, duckdb, dbt-duckdb (local warehouse for end-to-end tests), `dbt-databricks==1.12.7` and `dbt-trino==1.10.6` and `dbt-snowflake==1.12.4` (to test against the real adapter code).
- **Console script:** `dbt-hodor = dbt_hodor.cli:main`.
- **Build:** `uv build` makes the wheel and the sdist. Both install paths are supported and documented:
  - a wheel in a UC volume, uploaded with `databricks fs cp`, and installed by jobs with `libraries: - whl: /Volumes/<catalog>/<schema>/<volume>/dbt_hodor-<version>-py3-none-any.whl`;
  - a package index, installed with `libraries: - pypi: {package: "dbt-hodor"}`, plus `repo:` for a private index.
  - The wheel is pure Python (`py3-none-any`), so the same file installs on Windows and on Linux job clusters.

### 13.1 License and provenance

Apache-2.0. Derived code is confined to `plugin.py` and `patches.py`, each carrying the
upstream copyright header and a note of what was modified. Every other module is written new
for hodor, which has no runtime or package dependency on upstream.

Attribution and trademarks: [License](license.md) and `NOTICE`.

## 14. Rollout

1. dev: `shadow` for at least a week of scheduled runs.
2. Check the shadow records. A **false reuse** is a node recorded with decision `reuse` whose actual build in the same run created a data change (section 8.1). Fix every false reuse before moving on.
   - This check is strong for incremental models: a build with no new input writes no rows. It is weak for `table` models, because `CREATE OR REPLACE` writes a new version with rows even when the content is identical. For tables, shadow mode measures the would-reuse rate but cannot prove each decision.
3. dev: `on`. Test or staging: `shadow`, then `on`. prod: `shadow`, then `on`.
4. Success measures: the share of nodes reused in the main daily run, the run's wall time, and zero false reuses.

## 15. Performance

hodor must cost far less than the work it saves. Budget and rules:

- **No code changes (steady state):** no sqlglot, only one sha256 per node (section 6, fast path). hodor's CPU per node stays under 1 ms, not counting metadata queries.
- **Warehouse queries per run:**
  - state store: 1 read, plus 1 insert per 500 records;
  - metadata: 1 `information_schema.tables` query per catalog, plus `DESCRIBE HISTORY` only for tables whose `last_altered` moved, if the spike validates `last_altered`;
  - if it does not, 1 `DESCRIBE HISTORY` per relevant table, on `hodor_metadata_threads` threads.
- **Caching:** a table's data version is read at most once per run (single-flight cache), and again only after hodor builds that table. Each node's base tables are resolved once per run (memoized lineage).
- **Imports:** dbt imports every `dbt_*` module on every command, including `ls` and `parse`. So `import dbt_hodor` loads nothing heavy; sqlglot and duckdb load only when hodor is active for `run`, `build` or `test`.
- **GIL:** sqlglot is CPU-bound Python and does not run in parallel across dbt threads. That is why the fast path matters more than threading.
- **Benchmarks** live in the test suite (marked `benchmark`, skipped by default) and run before each release: hashing per file, and the decision path for 10,000 synthetic nodes.

## 16. Review decisions

1. State table schema: an existing operations schema, set with `hodor_schema` (decided 2026-10-20).
2. Wheel source for jobs: both a UC volume and a package index (decided 2026-10-20).
3. Default mode `shadow` (decided 2026-10-20). Each environment runs in shadow for about a week, then switches to `on` per target.

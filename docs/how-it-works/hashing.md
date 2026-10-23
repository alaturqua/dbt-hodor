# Hashing

hodor hashes each node's logic, so it can tell whether the code changed.

## Code hash

1. Take the node's compiled SQL.
2. Replace this run's `invocation_id` and `run_started_at` values with fixed placeholders. Many models inline `'{{ invocation_id }}'`, which would otherwise change the SQL on every run.
3. Parse with sqlglot (the adapter's dialect: `databricks` or `duckdb`). Replace each relation name that belongs to a node of the project, or of the `--state` manifest, with the node's `unique_id`, so a different catalog or schema per target hashes the same. A name that two nodes share stays as it is. Then run `qualify` with `validate_qualify_columns=False`, `expand_stars=False`, `identify=True` and `canonicalize_table_aliases=True`. This normalizes keyword and identifier case, quoting and table aliases.
4. Generate SQL without comments and hash it with sha256.
5. If `qualify` fails, hash the parsed SQL generated without comments. If parsing fails, hash the compiled text with whitespace collapsed. Both fallbacks can only cause extra builds, never wrong reuses.

A seed has no SQL. Its code hash is the sha256 of the CSV file, with `\r\n` read as `\n` so Windows and Linux checkouts hash the same. hodor reads the file itself, because dbt's own checksum covers only the path for seeds above its size limit (`--maximum-seed-size-mib`).

## Volatile functions

The same parse finds the volatile functions in the SQL and sorts them into three classes:

| Class | Functions | Value |
| --- | --- | --- |
| `date` | `current_date()`, `curdate()`, `today()` | the warehouse's current date |
| `session` | `current_user()`, `session_user()`, `user()`, `is_member()`, `is_account_group_member()`, `current_catalog()`, `current_database()`, `current_schema()`, `current_timezone()`, `current_version()`, `current_metastore()` | user, catalog, schema, time zone and version |
| `always` | `current_timestamp()`, `now()`, `getdate()`, `sysdate()`, `localtimestamp()`, `current_time`, `unix_timestamp()` without arguments, `rand()`, `randn()`, `uuid()`, `random_uuid()`, `monotonically_increasing_id()`, `shuffle()`, and `{{ invocation_id }}` and `{{ run_started_at }}` (the placeholders of step 2) | the invocation id: it changes on every run, so `lag_tolerance` decides |

Each use is either a stamp or logic. A stamp is an output column of the final `SELECT` that reads no column, for example `current_timestamp() as _loaded_at`, `'{{ invocation_id }}' as loaded_by` or `cast(current_date() as string) as load_date`. Every other use is logic: filters, joins, `CASE`, values computed from columns, CTEs and subqueries.

Each class used as logic becomes an upstream group of the node (`volatile:date`, `volatile:session`, `volatile:always`) with the current value as its data version: the date, the session values, or the invocation id. The record stores it with the other upstreams, and the [decision rules](decision-rules.md) compare it like a table's version. hodor reads the date and session values in one warehouse query per run, only when a node needs them. If the query fails, the values are unknown and those nodes build (`unknown_upstream`). `is_member()` is approximated by the current user: a change in group membership alone is not seen.

`evaluate_volatile_sql` changes which uses count: unset (auto) counts logic only, `true` counts stamps too, `false` counts nothing, as in dbt State's default.

Jinja values other than `invocation_id` and `run_started_at`, such as `{{ modules.datetime.datetime.now() }}` or `{{ run_started_at.strftime('%Y-%m-%d') }}`, change the compiled SQL itself, so the node builds with `code_changed` whenever the rendered value changes.

## Fast path

`text_hash` is the sha256 of the compiled SQL after step 2. When it equals the `text_hash` in the node's last record, hodor takes the SQL analysis from the record (code hash, select-star flag, table references, volatile functions). sqlglot does not run.

For a changed node, sqlglot runs once. One parse feeds the code hash, the select-star check and the table references.

!!! info "Measured cost"

    Measured on a sample of compiled model files with sqlglot 30.22.0: sha256 takes about 0.01 ms per file, a full sqlglot analysis about 26 ms on average. For 10,000 models, a run with no code changes spends about 0.1 s on hashing instead of about 4 minutes of single-threaded CPU.

## Config hash

The sha256 of the node config (which holds the contract settings and Databricks settings such as `row_filter`), the column data types, constraints, column masks (`column_mask`) and column tags (`databricks_tags`), and the model-level constraints (for example a primary key over two columns). These keys are removed first because they do not change the produced data: `databricks_compute`, `incremental_apply_config_changes`, `tags`, `meta`, `docs`, `state`, `enabled`, `persist_docs` and column descriptions. Column data types are compared without case and spaces, so `DECIMAL(10, 2)` and `decimal(10,2)` are the same type.

The config hash also holds the `macro_sql` of the project and package macros that the node's materialization or its hooks run, found through dbt's macro dependencies. A project override of `create_table_as` that adds an audit column, a materialization from a package, or a macro called in a `post_hook` therefore rebuilds the nodes that run it, with `config_changed`. An override used by every table rebuilds every table. dbt's and the adapter's own macros are left out, so an adapter upgrade does not rebuild every node.

## Unrendered hash

Used only with `compare_unrendered_code`. It is the sha256 of `raw_code`, the `macro_sql` of every macro in `depends_on.macros` (resolved recursively), and `unrendered_config`.

## Hash version

Every record carries `hash_version`, the version of hodor's own hashing algorithm. A record with a different `hash_version` counts as `code_changed`. When it changes, every node builds once. [Upgrading](../upgrading.md) lists the releases that change it. The sqlglot version range is pinned, and golden tests catch output drift before a sqlglot upgrade.

## Consequences and limits

- A change to a macro that the model SQL calls, including a package upgrade such as dbt_utils or dbt_date, rebuilds the node only when the compiled SQL changes.
- A change to dbt's or the adapter's own macros, for example a dbt-databricks upgrade, does not show in any hash. Run once with `HODOR_MODE=shadow` after such an upgrade: shadow builds every node and records the builds. `off` records nothing, so the old records would still count. Project and package macros that a materialization or a hook runs are in the config hash.
- The first incremental run after a full build compiles different SQL (the `is_incremental()` branch). That causes one extra build.
- CTE renames count as changes. This costs one extra build. hodor does not use sqlglot's `canonicalize_internal_names`, because without a schema it gave two different queries the same output, which would cause wrong reuses.

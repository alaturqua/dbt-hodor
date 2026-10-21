<!-- markdownlint-disable MD033 MD041 -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/hodor-logo.svg">
    <img src="docs/assets/hodor-logo-light.svg" width="420" alt="dbt-hodor: Hold Old Data, Or Rebuild">
  </picture>
</p>

<h1 align="center">dbt-hodor</h1>

<p align="center">
  <a href="https://github.com/alaturqua/dbt-hodor/actions/workflows/ci.yml"><img src="https://github.com/alaturqua/dbt-hodor/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/python-3.10%20to%203.12-blue" alt="Python 3.10 to 3.12">
  <img src="https://img.shields.io/badge/dbt--core-1.12-orange" alt="dbt-core 1.12">
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Apache-2.0">
  <a href="https://alaturqua.github.io/dbt-hodor/"><img src="https://img.shields.io/badge/docs-online-informational" alt="Documentation"></a>
</p>

**[Documentation](https://alaturqua.github.io/dbt-hodor/)** | **[Interactive guide](https://alaturqua.github.io/dbt-hodor/guide/)** with a run simulator | **[Design spec](https://alaturqua.github.io/dbt-hodor/design/)**

dbt-hodor is a free, self-hosted alternative to dbt Labs' [dbt State](https://docs.getdbt.com/docs/deploy/dbt-state-about). It runs inside dbt. For every selected node it decides whether to reuse the object that is already in the warehouse or to build it again. A node is reused when its logic has not changed and its upstream data has not changed since the last build.

It needs no dbt platform account and sends nothing to external services. State lives in a Delta table on Databricks, in a Postgres database such as Databricks Lakebase, or in a local DuckDB file.

> **Status: early development.** hodor runs inside dbt and passes end-to-end tests on dbt-duckdb. Tested live on a Databricks Unity Catalog workspace with a SQL warehouse and dbt-databricks 1.12.7 (2026-10-20 and 2026-10-21): reuse of tables, incremental models (including `replace_where`), views, seeds and snapshots, replayed tests, maintenance commits (OPTIMIZE, zero-row writes, SET TBLPROPERTIES) that keep nodes reused, and real new data or a changed seed file that rebuilds them. The live test runs twice, with `use_materialization_v2` off and on, and also covers contracts with a primary key, a row filter, column masks, date-window and stamp models, and a table swapped in by `use_safer_relation_operations`. The same live suite passes on a second, independent Databricks workspace (2026-10-21). The `postgres` store is tested end to end against Databricks Lakebase. It has not run in a large production project yet. Start in shadow mode.

## Why "hodor"

The name is a backronym: **H**old **O**ld **D**ata, **O**r **R**ebuild. That is the only decision hodor makes, once per node.

hodor holds the door in front of your warehouse. A node gets through to a rebuild only when its code changed or new data arrived upstream. Everything else keeps the data it already has.

## How it works

For each node in `dbt run`, `dbt build`, `dbt test`, `dbt seed` or `dbt snapshot`, hodor:

1. **Hashes the logic.** For a seed this is the CSV file. Otherwise the compiled SQL is normalized with [sqlglot](https://github.com/tobymao/sqlglot) (Databricks dialect), so case, quoting, comments and table aliases do not count as changes. Values that differ on every run, such as `invocation_id`, are replaced first. The node config is hashed as well.
2. **Checks the upstream data.** Each upstream table has a data version. On Databricks it comes from the Delta commit history; maintenance commits such as `OPTIMIZE` or `VACUUM` do not count as data changes. On DuckDB it is a content fingerprint.
3. **Decides.** It compares both against the record of the node's last build and returns `reuse` or `build`, with a reason code.
4. **Records.** It appends one record per node to the state table.

A run with no code changes skips sqlglot entirely: when the compiled SQL is byte-identical to the last build, hodor reuses the stored analysis. For 10,000 compiled models, hashing then takes about 0.1 seconds instead of about 4 minutes.

### Safety: fail open

Every failure leads to a build, never to a reuse. An error while deciding, a store that cannot be read, a table whose data version is unknown, an input hodor cannot trace (for example `read_files(...)`): in each case the node builds as plain dbt would. hodor never fails a dbt run.

### Reason codes

| Reason | Decision |
| --- | --- |
| `full_refresh` | build |
| `unsupported` | build |
| `no_history` | build |
| `relation_missing` | build |
| `code_changed` | build |
| `config_changed` | build |
| `target_modified` | build |
| `select_star_view` | build |
| `view_unchanged` | reuse |
| `unknown_upstream` | build |
| `no_new_data` | reuse |
| `within_lag_tolerance` | reuse |
| `new_upstream_data` | build |
| `volatile_sql` | build |
| `hodor_error` | build |

The rules are checked in this order; the first match decides. `dbt-hodor explain -s <model>` shows the reason for one node, with the hashes and upstream data versions it compared. The full rules are in the [design spec](docs/design.md#5-decision-rules).

## Modes

| Mode | Behavior |
| --- | --- |
| `off` | hodor does nothing. |
| `shadow` (default) | hodor decides and records, but every node builds. Use it to check the decisions before you trust them. |
| `on` | hodor reuses nodes. |

Installing the package never skips anything until someone sets `on`.

## Configuration

### Node configs

hodor reads the same `state:` block as dbt State, so a project can switch between the two. Set it in YAML:

```yaml
# dbt_project.yml
models:
  my_project:
    +state:
      lag_tolerance: 4h
      require_fresh_data_from: any
```

| Key | Default | Values |
| --- | --- | --- |
| `lag_tolerance` | `45m` | `0s`, `45m`, `4h`, `1d`, `1w`, integer seconds, or `{count: 4, period: hour}` |
| `require_fresh_data_from` | `any` | `any`: build when any upstream has new data. `all`: build only when all of them have. |
| `compare_unrendered_code` | `false` | `true`: build only when both the rendered and the unrendered code changed |
| `execute_hooks_on_any_reuse` | `false` | `true`: run pre- and post-hooks on a reused node. `execute_hooks_on_reuse` is also accepted. |
| `evaluate_volatile_sql` | auto | Auto: volatile functions such as `current_date()`, `current_user()` and `current_timestamp()` count as an upstream that changes, unless they only fill a stamp column. `true`: stamp columns count too. `false`: they never count (the dbt State default). |

`pre_clone`, `allow_clones` and `defer_to_target` are accepted and ignored, with a warning.

### Settings

Set them on the `profiles.yml` target, or for one run with `--vars '{hodor_mode: off}'`. Precedence: environment variable, then `--vars`, then the profile key.

| Profile key | Env var | Default | Values |
| --- | --- | --- | --- |
| `hodor_mode` | `HODOR_MODE` | `shadow` | `off`, `shadow`, `on` |
| `hodor_store` | `HODOR_STORE` | `delta` | `delta`, `duckdb`, `postgres` (alias `lakebase`) |
| `hodor_database` | `HODOR_DATABASE` | the target's `catalog` (or `database`) | Databricks catalog; Postgres database (later). Not used by `duckdb` |
| `hodor_schema` | `HODOR_SCHEMA` | none for `delta` (required); `main` for `duckdb` | schema name |
| `hodor_table` | `HODOR_TABLE` | `hodor_state` | table name |
| `hodor_path` | `HODOR_PATH` | `.hodor/state.duckdb` | DuckDB database file, for `duckdb` |
| `hodor_metadata_threads` | `HODOR_METADATA_THREADS` | `8` | 1-64 |
| `hodor_lookback_days` | `HODOR_LOOKBACK_DAYS` | `90` | 1-3650 |

Name parts may contain letters, digits, `_` and `-`. hodor always quotes them.

### Where the state lives

| Store | Location | Created on first use |
| --- | --- | --- |
| `delta` | `<hodor_database>.<hodor_schema>.<hodor_table>` | the table only. The schema must exist, because creating one needs catalog-level grants |
| `duckdb` | table `<hodor_schema>.<hodor_table>` in the file `hodor_path` | the file, the schema and the table |
| `postgres` (alias `lakebase`) | `<hodor_schema>.<hodor_table>` in the Postgres database `hodor_database` | the schema and the table |

`delta` has no default schema on purpose: hodor writes nothing to a catalog until someone names the schema. Without it, hodor turns itself off with a warning.

```yaml
# profiles.yml
my_project:
  target: prod
  outputs:
    prod:
      type: databricks
      catalog: my_catalog
      # ... connection settings ...
      hodor_mode: shadow
      hodor_schema: ops
      # state table: my_catalog.ops.hodor_state
    local:
      type: duckdb
      path: dev.duckdb
      hodor_store: duckdb
      # state table: main.hodor_state in .hodor/state.duckdb
```

The state table decides what gets skipped in prod. Grant `MODIFY` on it only to the job service principals.

## Scope of v1

Supported: models materialized as `table`, `incremental` (append, merge, delete+insert, insert_overwrite, replace_where; an unset strategy counts as merge) and `view`, seeds, snapshots, and data tests (generic and singular), on `dbt-databricks` 1.12.7 and `dbt-duckdb`, with dbt-core 1.12.x.

Model versions: each version is its own node with its own table and records. Moving `latest_version` rebuilds the models that `ref()` it.

Always built as plain dbt: clones and deferral, Python models, microbatch, materialized views, streaming tables, metric views, custom materializations, custom incremental strategies and unit tests.

Known limit: a change to dbt's or the adapter's own macros, for example a dbt-databricks upgrade, does not show in any hash. Run once with `HODOR_MODE=off` after such an upgrade. Project and package macros that a materialization or a hook runs are in the config hash.

## Development

Requires [uv](https://docs.astral.sh/uv/). Local development and tests run on DuckDB, with no Databricks access.

```sh
uv sync
uv run pytest
uv run ruff check
uv run ruff format
```

The live Databricks test is opt-in: see [docs/development.md](docs/development.md#tests).

Build a wheel with `uv build`. The package is not published to any index yet.

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) has the setup, the
checks to run, and what a decision change needs. A wrong reuse, where hodor skipped a node
it should have rebuilt, is a security-grade bug: report it privately through the Security tab.

## License

Apache-2.0. [NOTICE](NOTICE) carries the attribution for the derived files and the trademark
notice; [LICENSE](LICENSE) is the full licence text.

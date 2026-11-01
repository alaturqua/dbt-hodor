# dbt-core and dbt v2

hodor runs on two dbt engines, and it hooks into them in different ways.

- **dbt-core (dbt 1.x, Python).** hodor is a plugin inside the dbt process. It patches dbt, reads the `profiles.yml` target and the `state:` node configs, and decides while each node runs.
- **dbt v2 (dbt-oss 2.0.8, Rust engine, also called Fusion).** dbt v2 has no plugin interface. Its built-in state client asks a service whether to run each node. `dbt-hodor serve` is that service: a local server that dbt talks to over gRPC on `127.0.0.1`. It applies the same decision rules and keeps hodor's records.

Use this page to see what differs. For dbt-core, start with [Getting started](getting-started.md) and [Configuration](configuration.md). For dbt v2, start with [dbt v2](dbt-v2.md). The decisions and reason codes are the same where the inputs are the same: see [Decision rules](how-it-works/decision-rules.md).

## Comparison

| Topic | dbt-core | dbt v2 | More |
| --- | --- | --- | --- |
| Supported dbt version | `dbt-core` 1.12.x, Python 3.10 or newer (CI tests 3.10 and 3.12) | dbt 2.0.8 only. For any other version the server answers that it is not supported, and dbt builds. | [Getting started](getting-started.md#install), [dbt v2](dbt-v2.md#what-is-supported) |
| Install | `pip install dbt-hodor` plus your adapter | `dbt-hodor[v2]`, which adds `grpcio` and `protobuf`. Without the extra, `dbt-hodor serve` exits with a message. | [Getting started](getting-started.md#install), [dbt v2](dbt-v2.md#install-and-start) |
| How it is turned on | Installed in the same Python environment as dbt; dbt loads it as a plugin. `hodor_mode` is `shadow` (default), `on` or `off`. | Start `dbt-hodor serve`, then set `DBT_ENGINE_MANAGE_STATE=true` and `RUN_CACHE_API_URL=<host:port>` in the shell that runs dbt. | [Configuration](configuration.md#settings), [dbt v2](dbt-v2.md#install-and-start) |
| Adapters | `databricks`, `trino`, `snowflake`, `duckdb` | Dialects `databricks`, `spark`, `snowflake`, `bigquery`, `redshift`. Any other dialect builds. | [Adapters](adapters/index.md), [dbt v2](dbt-v2.md#what-is-supported) |
| State stores | `delta`, `trino`, `duckdb`, `postgres` (alias `lakebase`) | `duckdb` or `postgres`. `delta` and `trino` need a dbt connection and are not available. | [State stores](stores/index.md), [dbt v2](dbt-v2.md#install-and-start) |
| How settings are read | `hodor_*` keys on the `profiles.yml` target, `HODOR_*` variables, `--vars`, and `state:` node configs | Flags of `dbt-hodor serve` and `HODOR_*` variables. No `profiles.yml`, no `state:` node configs. See [Settings](#settings). | [Configuration](configuration.md), [dbt v2](dbt-v2.md#install-and-start) |
| Data versions | An adapter token: Delta commit history, Iceberg or Delta Lake history on Trino, Snowflake commit and DDL times, a content fingerprint on DuckDB | The last modification time of each table, an integer in epoch milliseconds, that dbt sends. hodor assumes a resolution of one second. | [Data versions](how-it-works/data-versions.md), [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| Lag tolerance | `lag_tolerance` node config, default `45m` | The tolerance dbt sends: 2700 seconds, or `RUN_CACHE_FRESHNESS_TOLERANCE`. The `lag_tolerance` node config is not read. | [Configuration](configuration.md#node-configs), [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| Volatile SQL | `evaluate_volatile_sql` node config; automatic by default | A model with a volatile function that decides rows or values always builds (`unknown_upstream`). `evaluate_volatile_sql` is not read. | [Configuration](configuration.md#node-configs), [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| Other reuse differences | The hash is made environment-neutral by node | The hash is of the literal SQL. A table name dbt did not list builds (`unknown_upstream`). A view always builds. A version less than one second from the last build builds (`same_second_read`). See [Decisions](#decisions). | [Decision rules](how-it-works/decision-rules.md), [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| Carry | A reused node whose build record is older than half of `hodor_lookback_days` gets a copy of it | No carry records. A node reused longer than the lookback window builds once. | [Records](stores/records.md), [dbt v2](dbt-v2.md#limits) |
| Seeds | Decided like other nodes | Built when the values hash or the config changes. An outside write to a seed table is noticed one run late. See [Seeds](#seeds). | [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| `--full-refresh` | Builds every node | dbt sends no flag, so a table model can be reused. An incremental model that dbt runs as a full table is recorded afterwards (`full_refresh`). | [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core) |
| State selection (`state:`) | hodor answers `state:` selectors when `--state` is not given and the target names a base target (`defer_to_target`) | The server answers them. It reads the base target's records again for each selection. See [State selection](#state-selection). | [Decision rules](how-it-works/decision-rules.md#state-selection), [Slim CI](slim-ci.md#without-a-state-manifest), [dbt v2](dbt-v2.md#state-selection) |
| Defer | Without `--state`, unselected nodes point at the relation the base target's latest build wrote | The server answers dbt's request for deferred relations from the same records | [Decision rules](how-it-works/decision-rules.md#defer-without-state), [dbt v2](dbt-v2.md#defer) |
| Clone | On by default (`allow_clones`) where the adapter reports `can_clone_table`; `pre_clone` is supported. See [Clones](#clones). | Off unless the server starts with `--allow-clones`. `pre_clone` has no effect. | [Decision rules](how-it-works/decision-rules.md#cloning-from-the-base-target), [Databricks](adapters/databricks.md#clones-and-dbt-v2), [dbt v2](dbt-v2.md#clones) |
| `explain` | `dbt-hodor explain` reads the state table | `dbt state explain` asks the running server. `dbt-hodor explain` cannot open a DuckDB file while the server holds it. | [Operations](operations.md), [dbt v2](dbt-v2.md#limits) |
| Shadow mode | Yes, and it is the default | No. The server always decides and always records. `HODOR_MODE` is ignored. | [Operations](operations.md), [dbt v2](dbt-v2.md#install-and-start) |
| Tested live | End-to-end tests on DuckDB. Live tests on a Databricks workspace, including a clone and `pre_clone`, on Trino with the Iceberg connector, and on a Lakebase Postgres store. The live test for Snowflake did not run for the current version. | 13 live tests with dbt 2.0.8 on a Databricks SQL warehouse, with the server on a DuckDB store. The `postgres` store has not run behind the server live. | [Development](development.md#tests), [dbt v2](dbt-v2.md) |

## Settings

dbt-core reads settings from `profiles.yml`, `HODOR_*` variables, `--vars` and the `state:` node configs. The dbt v2 server reads these:

- Flags: `--project-dir`, `--port`, `--store`, `--path` and `--allow-clones`.
- Variables: the store settings (`HODOR_STORE`, `HODOR_PATH`, `HODOR_DATABASE`, `HODOR_SCHEMA`, `HODOR_TABLE`, and `HODOR_ENDPOINT`, or `HODOR_HOST` with `HODOR_PORT`, `HODOR_USER`, `HODOR_PASSWORD` and `HODOR_SSLMODE`), `HODOR_LOOKBACK_DAYS` and `HODOR_ALLOW_CLONES`. A flag overrides the variable.
- dbt's own variables: `RUN_CACHE_FRESHNESS_TOLERANCE` and `RUN_CACHE_DEFER_TO`.

The server does not read `hodor_mode`, the `hodor_*` keys of a `profiles.yml` target, or the `state:` node configs (`lag_tolerance`, `require_fresh_data_from`, `compare_unrendered_code`, `execute_hooks_on_any_reuse`, `evaluate_volatile_sql`, `defer_to_target`, `allow_clones`, `pre_clone`). The base target is the one dbt names as its defer target. Values and defaults are in [Configuration](configuration.md#settings).

## Decisions

The rules in [Decision rules](how-it-works/decision-rules.md) apply on both engines. On dbt v2 the inputs are less exact: data versions are whole-second timestamps, the hash covers the literal SQL, and dbt 2.0.8 sends a view without its SQL. Where the server cannot be sure, the node builds. The details and reason codes are in [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core).

## Seeds

On dbt v2, dbt sends no timestamp for a seed's own table. The server takes the table's existence and version from the seed's last build. An outside write to a seed table is caught one run late (the next run rebuilds it with `target_modified`), when a model that reads the seed is decided, and not at all when no model reads it. A dropped seed table is not seen. See [dbt v2](dbt-v2.md#how-a-decision-differs-from-dbt-core).

## State selection

Both engines compare each node with the latest successful record of the base target and fail the selection when they cannot compare. hodor does not fail open here. On dbt-core, an incremental node whose partial clone cannot be dropped also fails, and `--defer` without `--state` fails the run when hodor cannot supply the base target; see [Decision rules](how-it-works/decision-rules.md#cloning-from-the-base-target) and [Defer without --state](how-it-works/decision-rules.md#defer-without-state).

On dbt v2:

- The server reads the base target's records from the store again for each selection. Decisions, defer and clones use the records loaded at the first request, plus those the server wrote itself.
- The base target must share the store with the target that runs. A base target with no records in the store gives `new` for every node.
- dbt 2.0.8 sends no macros hash and no persisted descriptions hash, and sends no relation with the base records, so `state:modified.macros`, `state:modified.relation` and `state:modified.persisted_descriptions` fail the selection.
- A node whose latest base record was written by the other engine (`v1` from dbt-core, `v2` from the server) counts as modified.
- The selection also fails when the base records are not read within 30 seconds.
- A source has no record, so it is `new` and `modified`, and `state:modified+` selects everything downstream of it. The DuckDB end-to-end test checks this on dbt-core. On dbt v2 it applies when dbt sends sources with the selection, which no test checked.

See [Decision rules](how-it-works/decision-rules.md#state-selection) and [dbt v2](dbt-v2.md#state-selection).

## Clones

On dbt-core, `allow_clones` defaults to `true`, but a node clones only when the adapter reports `can_clone_table` (the Databricks adapter does) and the target names a base target. On dbt v2, clones are off unless `dbt-hodor serve` starts with `--allow-clones` (or `HODOR_ALLOW_CLONES=true`), and the flag applies to every node. A model that reads a source is not cloned, and a node with `pre_clone` gets a plain build.

The default on dbt v2 follows a Databricks limit. A table cloned into another target from a base table with automatic liquid clustering cannot be replaced in place by dbt v2: when the model changes later, its build fails until the table in the current target is dropped. On dbt-core this does not happen, and the live test rebuilt a changed model over its auto-clustered clone. See [Databricks](adapters/databricks.md#limits).

## Tests

What ran is listed in [Development](development.md#tests).

- dbt-core: end-to-end tests on dbt-duckdb; live tests on a Databricks workspace (the lifecycle, slim CI, a shallow clone and `pre_clone`); a live test on Trino with the Iceberg connector. The Trino Delta Lake connector has unit tests only. The live test on a Lakebase Postgres store ran for the current version; the live test for Snowflake exists but did not.
- dbt v2: requests recorded from dbt 2.0.8, unit and server tests, and 13 live tests with dbt 2.0.8 on a Databricks SQL warehouse. The `postgres` store has not run behind the server live; see [Postgres and Lakebase](stores/postgres.md).

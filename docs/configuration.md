# Configuration

hodor has two kinds of configuration: node configs under `state:`, and settings on the `profiles.yml` target.

## Node configs

hodor reads the same `state:` block as dbt State, so a project can switch between the two. Set it in YAML (`dbt_project.yml` or a model properties file). hodor merges `state:` key by key across project, folder and model levels.

```yaml
# dbt_project.yml
models:
  my_project:
    +state:
      lag_tolerance: 4h
      require_fresh_data_from: any
snapshots:
  my_project:
    +state:
      lag_tolerance: 0s
```

| Key | Default | Values |
| --- | --- | --- |
| `lag_tolerance` | `45m` | `0s`, `45m`, `4h`, `1d`, `1w`, integer seconds, or `{count: 4, period: hour}` |
| `require_fresh_data_from` | `any` | `any`: build when any upstream has new data. `all`: build only when all of them have. |
| `compare_unrendered_code` | `false` | `true`: build only when both the rendered and the unrendered code changed |
| `execute_hooks_on_any_reuse` | `false` | `true`: run pre- and post-hooks on a reused node. `execute_hooks_on_reuse` is also accepted. All hooks are rendered before the first one runs, so a hook cannot query what an earlier hook wrote. |
| `evaluate_volatile_sql` | auto | Auto: volatile functions such as `current_date()`, `current_user()` and `current_timestamp()` count as an upstream that changes, unless they only fill a stamp column. `true`: stamp columns count too. `false`: they never count (the dbt State default). |

`pre_clone`, `allow_clones` and `defer_to_target` are accepted and ignored, with a warning. Unknown keys are ignored with a warning. An invalid value warns and keeps the default.

Duration units: `s`, `m`, `h`, `d`, `w` and their long forms (`second`, `minutes`, `hours`, `day`, `weeks`).

## Settings

Set them as keys on the active `profiles.yml` target. An environment variable overrides the profile key. hodor reads the keys from `profiles.yml` itself and renders them the way dbt renders the profile, so `{{ env_var('...') }}` works.

| Profile key | Env var | Default | Values |
| --- | --- | --- | --- |
| `hodor_mode` | `HODOR_MODE` | `shadow` | `off`, `shadow`, `on` |
| `hodor_store` | `HODOR_STORE` | `trino` on the `trino` adapter, `delta` on every other adapter | `delta`, `duckdb`, `postgres` (alias `lakebase`), `trino`. `delta` needs the `databricks` adapter and `trino` needs the `trino` adapter; with any other pairing hodor is off for the run. On `duckdb`, set `duckdb` or `postgres` |
| `hodor_database` | `HODOR_DATABASE` | the target's `catalog` (or `database`), for `delta` and `trino` only | Databricks or Trino catalog, or Postgres database. Required for `postgres`: a Postgres database is unrelated to the dbt target, so it has no default. Not used by `duckdb` |
| `hodor_schema` | `HODOR_SCHEMA` | none for `delta`, `trino` and `postgres` (required); `main` for `duckdb` | schema name |
| `hodor_table` | `HODOR_TABLE` | `hodor_state` | table name |
| `hodor_path` | `HODOR_PATH` | `.hodor/state.duckdb` | DuckDB database file, for `duckdb`. A relative path is relative to the dbt project root. |
| `hodor_metadata_threads` | `HODOR_METADATA_THREADS` | `8` | 1-64 |
| `hodor_lookback_days` | `HODOR_LOOKBACK_DAYS` | `90` | 1-3650. A node not built within this window builds once (`no_history`). |

These apply to the `postgres` store only. Set either `hodor_endpoint` (Lakebase) or `hodor_host`.

| Profile key | Env var | Default | Values |
| --- | --- | --- | --- |
| `hodor_endpoint` | `HODOR_ENDPOINT` | none | A Lakebase endpoint path, `projects/<p>/branches/<b>/endpoints/<e>`. It supplies the host, the user and an OAuth token, so `hodor_host`, `hodor_user` and `hodor_password` are not needed |
| `hodor_host` | `HODOR_HOST` | none | Postgres host name, for any Postgres that is not Lakebase |
| `hodor_port` | `HODOR_PORT` | `5432` | 1-65535 |
| `hodor_user` | `HODOR_USER` | none | Postgres role to connect as |
| `hodor_password` | `HODOR_PASSWORD` | none | Keep it in an environment variable, not in `profiles.yml` |
| `hodor_sslmode` | `HODOR_SSLMODE` | `require` | Any psycopg sslmode. Use `disable` only for a local container |

With neither `hodor_endpoint` nor `hodor_host`, the `postgres` store warns and hodor turns off for the run.

Name parts (`hodor_database`, `hodor_schema`, `hodor_table`) may contain letters, digits, `_` and `-`, and must not start with `-`. hodor always quotes them. An invalid value warns and falls back to the default.

An empty environment variable is ignored, so the profile key applies.

### Per run: `--vars`

dbt-core 1.12 accepts no new command-line flags from a plugin: it reads the flags before any plugin loads, so `dbt build --no-hodor` fails with `No such option`. Use dbt's own `--vars` instead. Any `hodor_<name>` key in `--vars` overrides the profile key for that run:

```sh
dbt build --vars '{hodor_mode: off}'
dbt build --vars '{hodor_mode: on, hodor_lookback_days: 30}'
```

Precedence, highest first: `HODOR_<NAME>` environment variable, `--vars`, profile key, default. The environment variable stays the job-level switch: `HODOR_MODE=off` turns hodor off whatever `--vars` says.

YAML reads unquoted `on` and `off` as booleans. hodor accepts both forms for `hodor_mode`.

dbt State's own switches, `--manage-state` and `DBT_ENGINE_MANAGE_STATE`, do not apply. dbt-core 1.12 has neither: the flag fails, and the variable triggers dbt's `environment-variable-namespace-deprecation` warning. On dbt 1.13 and later they turn on dbt Labs' own dbt State.

!!! warning "`delta` and `trino` have no default schema"

    With the `delta` or `trino` store, hodor needs `hodor_schema`, and a database from `hodor_database` or the target's catalog. Without them, hodor turns itself off for the run with a warning. It writes nothing to a catalog until someone names the schema.

## Example profile

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

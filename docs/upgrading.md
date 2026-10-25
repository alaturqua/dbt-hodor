# Upgrading

What changes for a running project from one release to the next. Each release lists breaking changes first: things that make nodes build, change the records, or need a change in your setup. The full list of commits is in the [changelog](https://github.com/alaturqua/dbt-hodor/blob/main/CHANGELOG.md).

Until 1.0, the settings and the record schema may change in a minor release.

## Unreleased

### Behavior changes

- **`--empty` runs are not recorded.** dbt-hodor is off for the whole run. Before, an `--empty` run wrote build records, and a later normal run could reuse the zero-row tables. If you ran `--empty` on 0.2.0, run `dbt build --full-refresh` once: it builds every node and writes new records. A node that sets `full_refresh: false` is not rebuilt by the flag.
- **The default store follows the adapter.** With no `hodor_store`, the `trino` adapter uses the `trino` store. `databricks` keeps `delta`. On `duckdb` the default stays `delta`, which turns hodor off until you set `hodor_store`. A `hodor_store` you set always wins.

### New

- **Trino adapter.** hodor runs on `dbt-trino` 1.10.6 with the Iceberg and Delta Lake connectors. Data versions come from the Iceberg snapshot history and the Delta Lake commit history; see [Data versions](how-it-works/data-versions.md#trino-iceberg-and-delta-lake-connectors). The Iceberg connector was tested live on Trino 476 in Docker; the Delta Lake connector has unit tests only.
- **`trino` store.** The state table lives in a Trino catalog, written through dbt's connection, in the catalog `hodor_database` (default: the target's `database`) and the schema `hodor_schema`. Without `hodor_schema`, hodor is off for the run with a warning, as for `delta`. See [State stores](stores.md#trino).

## 0.2.0

### Breaking changes

- **Every node builds once after the upgrade.** The code hash no longer depends on catalog and schema names (see [Hashing](how-it-works/hashing.md)), so `hash_version` is now `2`. Records written by 0.1.x count as `code_changed`. Plan the first run after the upgrade as a full build, or run it in `shadow` mode first.

### Behavior changes

- **New reason code `upstream_code_changed`.** A node that reads through a view builds when that view has other code than at the node's last build, also when no upstream table has new data. Changes through macros and variables count. Before, such a node was reused. Dashboards or alerts that list reason codes need the new one. See [Decision rules](how-it-works/decision-rules.md).
- **A table rebuilt with 0 rows counts as new data.** A `CREATE OR REPLACE TABLE` or `INSERT OVERWRITE` that writes no rows empties the table, and its dependents now build. Before, they were reused with the old data.
- **Rule numbers from 12 on went up by one** to make room for the new rule. The reason codes of the other rules did not change.
- **A view that reads another relation than at its last build builds** (`code_changed`), for example with and without `--defer`, also with `compare_unrendered_code`.
- **Views are covered by `target_modified`.** A view's data version is now a hash of its definition in the warehouse, so a view replaced by a run without hodor builds once.
- **Rendered hooks are part of the config hash.** A hook whose SQL changes through `var()`, `env_var()` or `target` now builds the node (`config_changed`).
- **Views without records count as unknown.** A node that reads through a view hodor has no build record of builds (`unknown_upstream`) until hodor has built that view once. A base view read through `--defer` uses its compiled SQL from the `--state` manifest instead. Before, dbt's checksum of the view's file was compared, which missed changes through macros and variables.
- **Fail-open after the decision.** When replaying a recorded test result fails, the test runs (`hodor_error`) instead of failing the run. A hook that fails while it runs on reuse fails the node, as before, so no hook runs twice.

### New

- **Slim CI.** `--defer` and `--favor-state` are supported, and each push of a pull request reuses what the last push built. A table made by `dbt clone` builds once, then is reused. See [Slim CI](slim-ci.md).

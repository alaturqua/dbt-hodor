# Decision rules

The engine checks the rules in this order. The first rule that matches decides. The reason code goes into the record and the dbt log line.

| # | Condition | Decision | Reason code |
| --- | --- | --- | --- |
| 1 | mode is `shadow` | build anyway; the record keeps the decision and reason from the other rules | reason from the other rules |
| 2 | `--full-refresh`, or the node config sets `full_refresh: true` | build | `full_refresh` |
| 3 | resource type, materialization or incremental strategy is out of scope | build | `unsupported` |
| 4 | no earlier successful build record for this node | build | `no_history` |
| 5 | the relation is missing from the warehouse | build | `relation_missing` |
| 6 | the code hash differs from the record (with `compare_unrendered_code`: rendered and unrendered hashes both differ) | build | `code_changed` |
| 7 | the config hash differs from the record | build | `config_changed` |
| 8 | tables, incrementals, seeds, snapshots and views: the target's data version differs from the one recorded after its last build | build | `target_modified` |
| 9 | a view whose final `SELECT *` reads directly from a `ref()` or `source()` | build | `select_star_view` |
| 10 | the node is a view (its definition is unchanged, see rule 6) | reuse | `view_unchanged` |
| 11 | any upstream table has an unknown data version | build | `unknown_upstream` |
| 12 | a view between this node and its upstream tables has other code than at the node's last build | build | `upstream_code_changed` |
| 12a | dbt v2 only: an upstream table's data version is less than 1000 ms from the data version recorded after the node's last build; or an unchanged upstream's version is no more than 1000 ms before the last build started, or later; or one of these versions is missing or not an integer | build | `same_second_read`, or `unknown_upstream` |
| 13 | no upstream changed since the last build (`any`), or not all of them changed (`all`) | reuse | `no_new_data` |
| 14 | upstream changed, but the last build is younger than `lag_tolerance` | reuse | `within_lag_tolerance` |
| 15 | otherwise | build | `new_upstream_data`, or `volatile_sql` when only volatile values changed |

A failure inside hodor gives `hodor_error` (build).

On dbt v2, the `dbt-hodor serve` state server also builds with `store_slow` when the records of the node's project and target are not loaded within 2 seconds of the request. A decision never waits longer for the store. A `state:` selection on dbt v2 is the exception: it waits up to 30 seconds for the base target's records and fails the run when they are not there (see [State selection](#state-selection)).

On dbt v2, the lag tolerance of a model is dbt's freshness tolerance for it (`freshness_tolerance_seconds` in the request, 2700 seconds by default), not `lag_tolerance`; 0 means no tolerance. A model with a volatile function builds with `unknown_upstream`, never `volatile_sql`, and a table name in the SQL that dbt did not list with a timestamp (for example an unqualified name) also gives `unknown_upstream`. A request in a dialect hodor cannot read, or without the node state, gives `unsupported`. See [dbt v2](../dbt-v2.md). Seeds use the default `lag_tolerance`, which does not matter for them because a seed has no upstream.

![The rules in order: each rule that matches decides build or reuse with its reason code, otherwise the next rule is checked](../assets/decision-rules-light.svg#only-light)
![The rules in order: each rule that matches decides build or reuse with its reason code, otherwise the next rule is checked](../assets/decision-rules-dark.svg#only-dark)

The diagram leaves out rule 12a (`same_second_read`), which applies on dbt v2 only and comes between rules 12 and 13.

## Terms

- **Same second read.** dbt v2 data versions are epochs in milliseconds, assumed to have a resolution of one second. hodor builds in two cases, because the timestamps cannot tell whether the last build read a write. First, an upstream table's timestamp is less than 1000 ms from the data version recorded after the node's last build. Second, an upstream is unchanged since the last build, but its timestamp is no more than 1000 ms before that build started (`built_at_ms`), or later. Delta versions used by dbt-core are not times, so this rule is off there.

- **Upstream changed.** An upstream table's current data version differs from the version recorded for it at the node's last build. hodor compares for equality, not order, and the token holds the commit time, so a dropped and recreated table, or one swapped in from a staging table, still counts as changed.
- **Last build.** The `built_at_ms` of the latest record with action `build`. Reuse records do not reset the clock. A carried copy of a build record (action `carry`, see [Records](../stores/records.md)) keeps the original `built_at_ms`.
- **`require_fresh_data_from`.** `any` builds when any upstream has new data. `all` builds only when all of them have.
- **Views.** A view always shows current upstream data, so new data alone never rebuilds it (rule 10). Rule 10 comes before rule 11, because a view stores no data and an unknown upstream does not make it stale. A view's code hash includes the names of the relations its SQL reads, so a view that reads another relation than at its last build (for example with and without `--defer`) builds with `code_changed`, also with `compare_unrendered_code`.
- **Views in between.** When a node reads through views, the record keeps the code hash of each view. For a view of this target, that is the code hash of its last build. For a base target's view read through `--defer`, it is the hash of the view's compiled SQL in the `--state` manifest; a view further up is the base target's when this target has no record of it. Both come from compiled SQL, so changes through macros and variables count. If one of them changed, the node builds (`upstream_code_changed`, rule 12), even when no upstream table has new data, with any `lag_tolerance` and with `require_fresh_data_from: all`. A view of this target that hodor has no build record of, or a base view without compiled SQL in the manifest, counts as unknown, so the node builds (`unknown_upstream`).
- **Seeds and snapshots.** Both store their own data, so they follow the table rules, including `target_modified`. A seed's code hash is the sha256 of its CSV file, and a seed has no upstream, so it builds only when the file, its config or its table changed. A snapshot's upstreams are the nodes and sources it selects from. `lag_tolerance` applies to snapshots too: set it under `snapshots:` in `dbt_project.yml`.
- **Seeds on dbt v2.** dbt v2 sends no timestamp for a seed's own table. When a model's request lists a seed's table with a timestamp other than the one recorded at the seed's last build, the server appends a record for the seed with action `stale`, status `success` and reason `target_modified`. The seed's next request builds with `target_modified`, also after a server restart. So an outside write to a seed table is caught one run late, and not at all when no model reads the seed.
- **Model versions.** Each version of a versioned model is its own node, with its own relation (for example `customers_v2`) and its own records. A child's `ref('customers')` compiles to the relation of `latest_version`, so moving `latest_version` rebuilds the children with `code_changed`.
- **Volatile SQL.** A volatile function whose value decides rows or values (in `WHERE`, `JOIN`, a CTE, or a column computed from other columns) counts as an extra upstream: `volatile:date`, `volatile:session` or `volatile:always` (see [Hashing](hashing.md#volatile-functions)). It goes through the upstream rules (from `unknown_upstream` on) like a table, so `where d >= current_date() - 7` builds once a day, and a `current_timestamp()` filter builds when the last build is older than `lag_tolerance`. A new date or user counts as new data even with `require_fresh_data_from: all`. A stamp column, an output column of the final `SELECT` that reads no column (`current_timestamp() as _loaded_at`), does not count: on reuse it keeps the value of the last build. Views compute these functions when they are read, so they get no volatile upstreams.
- **Tests.** Tests use every rule except the view rules (`select_star_view`, `view_unchanged`) and `within_lag_tolerance`. Their upstreams are the nodes they test. `lag_tolerance` is always 0 for tests.
- **`target_modified`.** Something wrote to the table outside this node's builds. Maintenance commits such as `OPTIMIZE` do not change the data version, so they never trigger it. A view's data version is a hash of its definition in the warehouse, so a view replaced by a run without hodor builds once.

## Cloning from the base target

A node that would build can be cloned from the same node's table in the base target instead, when `allow_clones` is not false. The reason is `cloned_from_base`, and the record has action `clone`. hodor decides to clone only when all of these hold:

- the node is a table or an incremental model, and rules 2 to 15 decided build. A build for `full_refresh` or `hodor_error` is never cloned.
- the base target has a successful build record for the node (a clone record is not a source), with the same config hash and hash version, and the same code hash. Both hashes are computed with each relation name that belongs to one node replaced by that node's unique_id, because schema names differ between targets. A relation name that belongs to no node, or to two, stays literal in the hash. This is dbt-core. On dbt v2 both hashes are of the literal SQL, so models whose SQL names a target-specific schema rarely match.
- each upstream, mapped to its node, has the same data version in the base record and now. A parent built in this target has its own table and version, so it blocks the clone. On dbt-core, sources can match, and parents read from the base target through defer cannot (see below). On dbt v2 it is the other way round: a parent read from the base target through defer can match, and a model that reads a source is never cloned, because a source has no record to map its name to a node. An upstream table that maps to no node, or has an unknown version, gives no clone.
- the node has no volatile SQL in a filter or join, and no session function (`current_user()`, `current_schema()`) in a stamp column. A `date` or `always` stamp column keeps the value of the base build, as on reuse.
- the base table's data version equals the base record's, that is, nothing wrote to it since that build. This is an input to the decision, not something hodor reads in the rules.

With `pre_clone: true`, an incremental node whose code changed gets `pre_cloned_from_base`: clone the base table, then run the model on top of the clone. The code check is skipped; all other checks still apply. With unchanged code the reason is `cloned_from_base`. Any input that is unknown gives a build with the reason from the rules above.

On dbt-core, hodor clones while the model runs, with the adapter's macros, and never in shadow mode. Without a base target, with the base target equal to the current one, or with `allow_clones: false`, it reads nothing for clones. Otherwise it renders the adapter's `can_clone_table` once per run. When that is false (the default, and so on DuckDB and Trino), it reads nothing more and the node builds as decided, with no clone record. Otherwise it reads the base target's records once per run, from the same state store. It maps upstream table names to nodes through those records and through the current manifest, so a source that both targets read under one name can match. A parent read from the base target through `--defer` does not match, so its child builds. Before a clone it reads the base table's data version. When that differs from the base record's, or is unknown, the node builds as decided. Otherwise:

- `create_or_replace_clone` writes the node's relation from the base table. A view, or a table the adapter cannot replace by a clone in place, is dropped first, and grants and persisted docs are applied, as in dbt's clone materialization.
- `cloned_from_base`: the clone is the whole job. The log line reads `CLONED <node>`, the run result `CLONED (cloned_from_base)`, and the record has action `clone` and the new table's data version. Hooks run on a clone only with `execute_hooks_on_reuse`, as on reuse.
- `pre_cloned_from_base`: after the clone the model is compiled again, so `is_incremental()` sees the clone, and runs on top of it. The record is a build with reason `pre_cloned_from_base`.
- after the clone statement hodor reads the base table's data version again. If it changed or is unknown, something wrote to the base table during the clone, and hodor treats it as a failed step.
- a failed step warns once for the node and the node builds. When the relation may have been written, hodor drops it and compiles the model again first, so an incremental model does not run on top of a partial clone. If that drop fails, a table still builds, as its materialization replaces the relation, and an incremental model fails with an error record.

A clone record is not a build record for the next decision of the same target, so every run of that target clones again or builds. That is a limit: a cloned node is never a reuse source in its own target. After `pre_cloned_from_base` the next run of the node builds once more.

On dbt-core, limits of the clone path:

- A node that reads a parent through `--defer` never clones; it builds.
- An external base table is cloned with the plain `create_or_replace_clone` statement. If it fails, the node builds.
- The adapter must report `can_clone_table`. The Databricks adapter does. The default is false, so nothing clones on DuckDB or Trino. Snowflake and other adapters have not been tested with clones.

Tests that ran: the DuckDB end-to-end tests cover the clone path with project macros that copy the table, and the fallback to a build. On Databricks, live tests cover a shallow clone from the base target, `pre_clone` (the clone, then an incremental `MERGE` on top of it), and a base table written since its build, which is not cloned. On dbt v2, recorded-request and server tests cover the clone answer, and a live test on Databricks covers a shallow clone from the base target and a clone dbt registers itself; see [dbt v2](../dbt-v2.md#clones).

## State selection

`state:modified`, `state:new`, `state:old`, `state:unmodified` and the `modified.*` sub-selectors compare each node with the latest successful record of the same node in the base target (`defer_to_target`), whatever its action: a build, a reuse, a carry or a clone. A failed build is not compared. A node with no record is `new`. A node whose record was written by the other engine (dbt-core records have `engine` `v1`, dbt v2 records `v2`) counts as modified. Without `--state`, on dbt-core, hodor answers the selector; with `--state`, or without a base target, dbt-core's own method answers.

Anything that stops the comparison fails the run with a `cannot compare` error: hodor does not fail open here, because a guessed selection could skip the models that changed. The only other failures are the failed drop after a clone, above, and a `--defer` without `--state` that hodor cannot answer, below.

- A node whose `state:` sets another `defer_to_target` than the target's makes the selection fail. There is one base target per selection.
- A source has no record, so every source is always `new` and `modified`. `state:modified+` and `state:new+` therefore select everything downstream of every source. With `-s`, this selects more than dbt-core's own comparison, never less. With `--exclude state:modified+`, everything downstream of every source is excluded, and `state:unmodified` and `state:old` leave out the sources and everything downstream of them. The DuckDB end-to-end test checks it on dbt-core; on dbt v2 it applies when dbt sends sources with the selection, which no test checked.
- A record hashes the macros a node had at parse time, as the selection does. Compiling a generic test adds macros to it, so without this an unchanged test would count as modified. Records written before this version hash the compiled test, so an unchanged test counts as modified until the base target has run it again.
- Contracted models that were removed since the base build are not checked.
- On dbt v2 the base target is the one dbt names as its defer target, and the wait for the records is up to 30 seconds.

## Defer without `--state`

With `--defer` and no `--state` on dbt-core, hodor points each unselected node at the relation its latest successful base build wrote. Limits:

- When hodor cannot supply the base target, it does not defer and dbt-core fails the run with its own error that `--state` or `--defer-state` is required. This happens when the store cannot be read, when hodor is off (`hodor_mode: off`), or when the base target is the current target. hodor warns first. This is the third exception to fail open: reading the current target's tables as if they were the base target's would be a wrong answer that looks right.
- A parent that was never built in the base target is not deferred, and dbt fails as it does without hodor.
- A node that was deleted or disabled in the base target still defers to its old table while its record is inside the lookback window.
- Selected nodes, and UDFs, get no deferred relation.
- A node whose `state:` names another `defer_to_target`, with no base target on the profile target, is never deferred.
- Records written before `node_hashes` existed (before this version) are not used until the base target has recorded again.
- A node that the base target renamed is not deferred to the old name.

## Reason codes in the log

A reused node shows its reason, for example `REUSED (no_new_data)`. A built node shows it too, for example `OK (new_upstream_data)`. A cloned node shows `CLONED (cloned_from_base)`. A replayed test shows `PASS <test> (replayed)`.

On dbt v2 the server's answer carries the reason. Besides the codes above, the server uses these descriptions: `store_slow` and `hodor_error` for builds, `cloned_from_base` for a clone, `clone_registered` when it accepts a clone dbt asked for itself, and `clones_off` or `unsupported` when it declines one. Clones are off unless the server starts with `--allow-clones`; see [dbt v2](../dbt-v2.md#clones).

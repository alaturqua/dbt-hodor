# dbt v2

For a side-by-side view of dbt-core and dbt v2, see [dbt-core and dbt v2](dbt-core-vs-dbt-v2.md).

dbt v2 (the Rust engine, also called Fusion) has no plugin interface, so hodor cannot run inside it. dbt v2 does have a state client that asks a service whether to run each node. `dbt-hodor serve` is that service: a local gRPC server that answers with hodor's decision rules and keeps hodor's records.

On dbt-core hodor patches dbt. On dbt v2 it runs as a separate process that dbt talks to. The decisions, reason codes and records are the same where the inputs are the same; the differences are listed below.

Tests: the server and the request mapping are tested with requests recorded from dbt 2.0.8 (`tests/fixtures/v2_requests.jsonl`) and with unit and server tests on DuckDB. A live test runs dbt 2.0.8 against a Databricks SQL warehouse with the server on a DuckDB store (`pytest -m dbt_v2`, see [Development](development.md)). A second live test runs the `dbt-hodor serve --store postgres` process on local Postgres 17 in Docker and on a Lakebase endpoint that was already awake, with a gRPC client in place of dbt (`pytest -m postgres tests/integration/test_v2_postgres.py`): there was no dbt binary in those runs, so the `postgres` store behind dbt 2.0.8 has not run live. A wake from scale to zero was not measured. The live tests with dbt start the server with clones on. The default (clones off) is covered by server tests (`clones_off`, `HODOR_ALLOW_CLONES`), not live. The other four dialects have no live test.

## Install and start

```sh
uvx --from 'dbt-hodor[v2]' dbt-hodor serve
```

The `v2` extra installs `grpcio>=1.81.1` and `protobuf>=6.33.5`. Without it, `dbt-hodor serve` exits with a message.

The server listens on `127.0.0.1` and prints the two environment variables dbt reads:

```text
DBT_ENGINE_MANAGE_STATE=true
RUN_CACHE_API_URL=127.0.0.1:50051
```

Set both in the shell that runs dbt, then run dbt as usual. In the live test, dbt 2.0.8 started its state client with or without `dbt-cloud: project-id` in `dbt_project.yml`, and with a value that is not a number. hodor uses the value only to check `state:` selectors: when a selector asks for a different project id than the one in `dbt_project.yml`, the selection fails. State selection was tested with a project id set, not without one.

```yaml
# dbt_project.yml
name: my_project
dbt-cloud:
  project-id: "1"
```

Stop the server with Ctrl+C. It writes the records it still has queued first, and waits up to 10 seconds for the store. A decision still waiting for records at that point answers build.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--project-dir` | `DBT_PROJECT_DIR`, else `.` | The dbt project. The server reads its `name` and `dbt-cloud.project-id`. |
| `--port` | `50051` | Port on `127.0.0.1`; `0` picks a free one, and the server prints it. |
| `--store` | `HODOR_STORE`, else `duckdb` | `duckdb` or `postgres`. `HODOR_STORE` also accepts `lakebase`. |
| `--path` | `HODOR_PATH`, else `.hodor/state.duckdb` in the project | DuckDB file, for `duckdb`. The flag overrides `HODOR_PATH`. |
| `--allow-clones` | `HODOR_ALLOW_CLONES`, else off | Clone unchanged tables from the base target. See Clones below. |

The server has no `profiles.yml` target to read. The `postgres` store takes its settings from the `HODOR_*` environment variables (`HODOR_DATABASE`, `HODOR_SCHEMA`, `HODOR_TABLE`, and `HODOR_ENDPOINT`, or `HODOR_HOST` with `HODOR_PORT`, `HODOR_USER`, `HODOR_PASSWORD` and `HODOR_SSLMODE`; see [Configuration](configuration.md#settings)). The `delta` and `trino` stores need a dbt connection and are not available. `HODOR_MODE` is ignored: the server always decides and always records, and has no shadow mode. A DuckDB file belongs to one process at a time, so `dbt-hodor explain` and `prune` cannot run on it while the server is up.

To run dbt v2 without the state client, set `DBT_ENGINE_MANAGE_STATE` to `false` or pass `--no-manage-state`. In the live test, either one kept dbt from sending the server any request, and dbt built the selected nodes. The server does not see such a run, so the next run with the server builds a table that run wrote (`target_modified`).

dbt reads its own state client settings from environment variables. Two of them change what the server decides: `RUN_CACHE_FRESHNESS_TOLERANCE` (seconds; dbt sends it as the lag tolerance, see below) and `RUN_CACHE_DEFER_TO` (the defer target, `prod` when unset).

## What is supported

- **Client.** dbt 2.0.8 only. The server answers `ValidateClientVersion` with "not supported" for any other version. In the live test, dbt then warned that the service does not support its version, sent no other request and built the selected nodes.
- **Adapters.** The dialects dbt v2 starts its state client for: `databricks`, `spark`, `snowflake`, `bigquery` and `redshift`. hodor analyzes the SQL of each of them with sqlglot. A request in any other dialect builds (`unsupported`). The recorded requests are all from `databricks`; the other four dialects are covered by unit tests of the SQL analysis and the clone statements, not by recorded dbt runs.
- **Nodes.** Table and incremental models, and seeds. Snapshots are mapped too, but only unit tests cover them; no recorded request or live test has one. A view model always builds (`unsupported`): dbt 2.0.8 sends its request without the SQL, so hodor cannot hash it. A model that reads a view is decided as usual: dbt sends the view's query and lists the view's own tables. Nodes of an installed package are recorded under the root project's name, which is what dbt sends. A request that lacks the node state or the target table builds with `unsupported`.
- **State store.** `duckdb` or `postgres`, as above. dbt-core and the server can write to the same store; see Records below.

## How a decision differs from dbt-core

The rules are the [decision rules](how-it-works/decision-rules.md). On dbt v2 these inputs differ.

- **Data versions are timestamps.** dbt v2 sends the last modification time of each table as an integer epoch in milliseconds, not the Delta or Snowflake token that dbt-core uses. hodor stores it as the data version and assumes a resolution of one second. An upstream whose version is less than 1000 ms from the version recorded for the node's last build may or may not be in that build, so the node builds (`same_second_read`). The node also builds for that reason in a second case: an upstream is unchanged since the last build, but its version is no more than 1000 ms before that build started, or after it started. The start is the time the server received the node from dbt. For a build dbt ran without asking and reported afterwards, for example under `--full-refresh`, it is the time `ValidateClientVersion` arrived for the dbt invocation, because dbt may report the build long after it ran. That is never later than the real start, so the node can build once more than needed. When no invocation was seen, the start is unknown, and a node with upstream tables builds on its next run. This second check applies to unchanged upstreams only; the first applies to every upstream. The versions come from the warehouse's clock and the start from the server's clock, so the check assumes the server's clock does not run ahead of the warehouse's. A missing or non-integer version gives `unknown_upstream`.
- **The lag tolerance is dbt's.** dbt sends a freshness tolerance with each request: 2700 seconds by default, or `RUN_CACHE_FRESHNESS_TOLERANCE`. hodor uses it as `lag_tolerance`, so by default a model whose upstream changed is reused while its last build is less than 45 minutes old (`within_lag_tolerance`), as with the default `lag_tolerance` on dbt-core. The `lag_tolerance` node config is not read on dbt v2. The live test sets the tolerance to 0.
- **Volatile SQL builds.** The server cannot read the warehouse's date or user, so a model with a volatile function that decides rows or values (`current_date()`, `current_user()`) always builds (`unknown_upstream`). `evaluate_volatile_sql` is not read on dbt v2. Stamp-only volatile columns (`current_timestamp()` written to a column) do not count as new data, as with `auto` on dbt-core. Not tested live.
- **Names dbt did not list build.** hodor compares the table names in the SQL with the tables dbt sent with a timestamp. A name that is not in that list, for example an unqualified table name, is an input hodor cannot trace, so the node builds (`unknown_upstream`).
- **The code hash is of the literal SQL.** dbt v2 sends the compiled SQL with its schema names. The hash is not made environment-neutral by node, as on dbt-core, so the same model in two targets rarely hashes equal. See Clones below.
- **Seeds.** A seed builds when its values hash changes (`code_changed`), and its config like a model's: in the live test a `column_types` change built the seed with `config_changed`. dbt v2 sends no timestamp for a seed's own table, so the server takes the seed table's existence and version from the seed's last build: a dropped seed table is not seen. An outside write to a seed table is noticed when a model that reads the seed is decided: the server then marks the seed, and the seed's next run builds (`target_modified`). So that write is caught one run late, and not at all when no model reads the seed. With a store shared between servers, a seed's reuse is caught one run late too.
- **`--full-refresh`.** dbt 2.0.8 sends no full-refresh flag. In the live test it asked the server about a table model as usual, and the server reused it when nothing had changed; on dbt-core, `--full-refresh` builds every node. An incremental model ran as a full table (`FULL`) without asking, and dbt reported it afterwards through `RecordExecutions`; the server records it with the reason `full_refresh`. A failed full refresh was not reported.

## Records

The server loads the records of a project and target from the store in the background, at the first request for that target in a dbt run, and loads them again at the first request of each later run (each `ValidateClientVersion` starts a new run). The reload starts from an empty copy, so a record that was pruned, deleted or has left the lookback window is gone from the next run. Only the records this server wrote that are not yet written to the store are kept and added to the reload, so a record still queued for writing is not lost. A record another process writes while a run is going, a second server or a dbt-core run, is not seen by that run's decisions, defer or clones; the next run sees it. State selection reads the base target's records from the store again for each selection.

Decisions, state selection and defer share one wait of 30 seconds for the store. For decisions it is one budget per project and target and per dbt run: the first request that needs the records starts the clock, and later requests of that run wait only until the same deadline. A hung store therefore delays a run once, and its other decisions build at once. One request never waits more than 30 seconds, including its check of the base target's records for a clone, so it stays below the 60 seconds dbt waits for an answer. When the records are not there in time, the node builds (`store_slow`); when the store cannot be opened or read, it builds (`hodor_error`). A failed read of the records is retried by the next request with the same deadline. A store that cannot be opened is opened again, in the background, at the next `ValidateClientVersion`; until it opens, decisions build with `hodor_error`, and a store that is down starts one loader for the run, not one per request. While the records of a new run are loading, a decision never answers from the earlier run's records: if the load is slow or fails, the node builds. `ValidateClientVersion` never waits on the store: in the live test, dbt waited about 60 seconds for that answer before it ran without the state client. The explain message of a `store_slow` or `hodor_error` decision says `records not loaded: <reason>`, because no record was compared.

Records written by the server carry `node_hashes` with `engine` set to `v2`. A record written by dbt-core has `engine` `v1`. The two do not match in state selection: a node whose latest record in the base target was written by the other engine counts as modified.

## State selection

`dbt build -s state:modified+` and the other `state:` selectors ask the server, which compares each node with the latest successful record of the base target, a reuse included. It reads those records from the store for each selection, and adds the records it wrote itself that the store does not hold yet. The criteria are `new`, `old`, `modified`, `unmodified` and the `modified.*` sub-selectors `body`, `configs` and `contract`.

dbt 2.0.8 sends no macros hash and no persisted descriptions hash, and it sends a node's relation only with the selection, as the relation in the session's target. So `modified` and `unmodified` compare the body, configs and contract hashes, and `state:modified.macros`, `state:modified.relation` and `state:modified.persisted_descriptions` fail the selection with an error. In the live test, a dev target compared with its base target selected nothing when the project was unchanged, the one model whose body changed, and a model whose `alias` changed (through its configs hash).

The base target is the target dbt names as its defer target. If the selector names one target and the session another, or if there is none, the selection fails with an error instead of guessing. The selection also fails when the records of the base target cannot be read, or are not read within 30 seconds. This is the one place where hodor does not fail open: a wrong guess would select the wrong models. dbt-core also raises an error when `state:` has nothing to compare with.

The base target must share the store with the target that runs: a base target with no records in this store gives `new` for every node. See [Slim CI](slim-ci.md).

## Defer

The server answers dbt's request for deferred relations with the exact relation each unselected node's latest successful build, carry or clone record in the base target wrote. It waits up to 30 seconds for the base target's records, as state selection does. It answers nothing when the base target is unknown or is the current target, and when the records are not there after 30 seconds or cannot be read. dbt then resolves those nodes on its own, and with `--favor-state` it can read the current target's tables; the answer is not an error. This was not tested live. A node that has no successful build in the base target is not deferred. A node the base target no longer builds, for example one that was deleted from the project, still defers to its old table as long as its record is inside the lookback window.

## Clones

Clones are off by default. Start the server with `--allow-clones`, or set `HODOR_ALLOW_CLONES=true`, to turn them on; an invalid value warns and keeps them off. They are off by default because on Databricks a dbt v2 clone of a table with automatic liquid clustering makes the next rebuild of that table fail; see [Databricks](adapters/databricks.md#limits).

With clones on, when a node would build and the base target holds a table with the same rows, the server answers with a clone instead. The reason is `cloned_from_base`; the conditions are listed under [Cloning from the base target](how-it-works/decision-rules.md#cloning-from-the-base-target). The answer carries the clone statement and `clone_required_last_modified_epoch`, the base table's data version from the base record. dbt runs the statement, and it is expected to compare that timestamp with the base table and build instead if the table was written since. That check is done by dbt, not by hodor, and it was not tested live: no live test rewrote the base table between the decision and the clone. If the clone fails, dbt builds and the server writes no record.

The clone statements are `CREATE OR REPLACE TABLE ... SHALLOW CLONE` on `databricks` and `spark`, and `CREATE OR REPLACE TABLE ... CLONE` on `snowflake` and `bigquery`. On `redshift` and other dialects the server cannot clone and the node builds. In the live test on Databricks, with the server started with clones on, dbt ran the shallow clone and reported the node as cloned from the other environment. The other dialects' clone statements are untested through dbt v2. On Databricks, a clone of a table with automatic liquid clustering cannot be replaced by dbt v2: when the model changes later, its build fails until the table in the current target is dropped. See [Databricks](adapters/databricks.md#limits).

Limits on dbt v2:

- `--allow-clones` applies to every node. The server reads no `profiles.yml`, and `allow_clones` and `pre_clone` in a node's `state:` config or on the target are not read. dbt sends the defer target even without `--defer`, so with clones on a node can be cloned on a run that does not defer. In the live test dbt sent the defer target `prod` on a run without `--defer`.
- `pre_clone` has no effect. The protocol has no "clone, then run the model" answer, so such a node gets a plain build.
- A model that reads a source is never cloned: the server maps upstream tables to nodes through the records of the two targets, and a source has no record.
- The code hash is of the literal SQL, schema names included. Two targets with different schema names usually produce different hashes for the same model, and the node is not cloned. A clone needs the same SQL text in both targets. Views between a node and its parents carry a `code:` key in the record that rarely matches across targets either.
- A clone record is not a build record for the next decision of the same target, as on dbt-core.
- Upstream versions on dbt v2 are whole seconds. A parent in the current target and its counterpart in the base target written in the same second compare equal, so a clone can be allowed although the two parents hold different rows.
- When dbt asks the server to register a clone it makes itself, for example an incremental model missing in a dev target, the server accepts it (`clone_registered` in the explain message) unless clones are off (`clones_off`), the dialect cannot clone, or the source or target is missing or the same table (`unsupported`). It accepts the request without a source timestamp. In the live test, with clones on, dbt cloned an incremental model missing in the dev target, registered the clone, and then ran its incremental append on top of it. A refused clone is answered as "unable to clone"; dbt is expected to build the model then, which no live test covered.

## Limits

- One dbt version (2.0.8). A new dbt v2 release needs a check of the protocol first.
- No `shadow` mode. The server reads only the store settings, `HODOR_LOOKBACK_DAYS` and `HODOR_ALLOW_CLONES` from the environment.
- Seeds, `--full-refresh`, clones, `pre_clone` and the lag tolerance behave as listed above.
- A store that cannot be read gives builds, never a failed run, except for `state:` selection.
- No carry records. On dbt-core, a reused node whose build record is older than half of `hodor_lookback_days` gets a copy of that record (see [Records](stores/records.md)); the server writes none. A node the server reuses for longer than the lookback window has no build record left in it, so it builds once.
- Overlapping runs on one server. A record carries the `x-dbt-invocation-id` header of the request that made it, so overlapping runs keep their own ids; a request without the header gets the id of the latest `ValidateClientVersion`. The tests send the header; whether dbt 2.0.8 sends it on every call was not checked live. Runs share the records and the wait budget: a second run's `ValidateClientVersion` makes the next decision of the first run wait for a reload, which can answer `store_slow` if the store is slow.
- In the live test, a stopped server and a server that never answers did not fail the run. With no server, dbt warned that its client initialization failed and built the selected nodes. With a server that accepted the connection and never answered, dbt waited about two minutes, then built. With a server that answered `ValidateClientVersion` but never answered a node's request, dbt waited about 60 seconds and then built that node.
- A failed build is not recorded. In the live test, a model whose SQL failed, a model whose post-hook failed and a failed `--full-refresh` of an incremental model sent no confirmation and no execution report, so the server wrote no success record. After the failed post-hook, the next run built the model (`target_modified`).
- `dbt state explain` asks the running server and prints its decisions with the reason codes (see Explain below). In the live test, `dbt-hodor explain` could not open the DuckDB file while `dbt-hodor serve` held it, and read it after the server stopped.

## Explain

`dbt state explain` asks the running server about the decisions of a run. In the live test dbt printed one line per node with the decision and the reason code, for example:

```text
SKIP_EXECUTION model.my_project.stg_orders - no_new_data
READY_TO_EXECUTE model.my_project.v_orders - unsupported
```

With each decision the server also sends one line per input it compared with the node's last build. For a node with no build record the only line is `no build record for this node`. Otherwise the lines are:

```text
code hash same as the last build
config hash same as the last build
<node table>: 1761900000000 (last build: 1761900000000)
<upstream table>: 1761903600000 (last build: 1761900000000)
```

`same as` reads `differs from` when the hash changed. The third line is the node's own table, with `unknown` when dbt sent no timestamp for it. Then comes one line per upstream table, with its timestamp now and at the last build (`None` when the last build did not read it). The numbers are examples. Server tests check these lines; the live test did not check how dbt prints them.

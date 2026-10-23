# Slim CI

A common slim CI setup builds only the changed models of a pull request (`-s state:modified+`) into a CI catalog or a CI schema. It reads everything else from a base target with `--defer --state <base manifest>`. The base target is whatever the pull request is compared with: the main catalog of the same environment, or prod. hodor works with that setup, and with dbt's own `dbt clone`. It adds two things:

- Each push to the pull request reuses what the last push built, when the code and the data it reads are unchanged.
- Renamed catalogs and schemas between targets do not count as code changes.

![A pull request builds its changed model in the CI catalog, reads unchanged parents from the base target through defer, and keeps its build records apart from the base target's](assets/slim-ci-light.svg#only-light)
![A pull request builds its changed model in the CI catalog, reads unchanged parents from the base target through defer, and keeps its build records apart from the base target's](assets/slim-ci-dark.svg#only-dark)

## Base and CI targets

The examples on this page use two targets in one environment, for example dev:

| Target | Builds into | Built by |
| --- | --- | --- |
| `base` | `analytics.sales`, the catalog the environment runs on | the scheduled job or the deploy of the main branch |
| `ci` | `analytics_ci.<ci_schema>`, a catalog for pull requests | the CI job of each pull request |

The names are examples. The base target can also be prod, when CI defers to prod's manifest. hodor needs nothing from the base target but the `--state` manifest that dbt reads anyway.

## Environment names are not code

The compiled SQL of a CI node names CI relations (`analytics_ci.<ci_schema>.orders`) or base relations (`analytics.sales.orders`, read through defer). Before hashing, hodor replaces every name that belongs to a node of the project or of the `--state` manifest with that node's `unique_id`. A different catalog, a schema per pull request, or a custom `generate_schema_name` therefore gives the same code hash. hodor does not parse schema names, so any naming works: `pr_<nr>_sales`, `mr_<iid>_sales`, `ci_<branch>`, a catalog per pull request, or one shared CI schema. Names of tables outside the project stay as they are. Views are the exception: a view keeps the names it reads, so its hash includes them, and a view that reads another relation than at its last build is rebuilt.

## Several pull requests at once

The same applies to merge requests: hodor does not know which CI system started the run.

- **A schema per pull request.** Records are kept per relation, so every CI schema has its own history. One pull request never reuses another one's table.
- **One shared CI schema.** A node is reused only when its code and the data it reads match the last build in that schema, whichever pull request made it. Two jobs running at the same time still overwrite each other's tables, as they would without hodor.
- **Base records stay untouched.** A CI run writes records only for its own target. The base target compares only with its own records, so a pull request never changes what the base target reuses on deploy.

## Reading the base target through defer

With `--defer`, dbt reads an unselected parent from the base target when the CI copy does not exist (or always, with `--favor-state`). hodor sees which relation the compiled SQL names and follows that one: a base view is expanded to the base tables behind it, using the `--state` manifest. The data versions it compares are therefore the versions of the tables the node really reads. When the base target gets new data, the pull request's nodes build on the next push; when it does not, they are reused.

A stale CI copy of a parent never hides new base data: with `--favor-state` the SQL names the base relation, and hodor compares the base target's data version.

A base view's code comes from its compiled SQL in the `--state` manifest, so a view changed through a macro or a variable counts as changed. When the manifest has no compiled SQL for a view, for example a manifest from `dbt parse`, the nodes that read through it build.

## dbt clone

`dbt clone --state <base manifest>` copies base tables into the CI schema (a shallow clone on Databricks). hodor does not take part in `dbt clone`: a cloned table has no hodor record, so when a later `dbt build` selects it, it builds once (`no_history`). From then on the CI record is its history, and the next push reuses it as long as nothing changed.

## Where CI records go

A CI run reads and writes only the records of its own target, in the state table set by the CI target's `hodor_*` keys. The CI target can use the base target's state table, a state table of its own, or one per pull request, for example a table named after it:

```yaml
ci:
  # ...
  hodor_table: "hodor_state_pr_{{ env_var('PR_NUMBER') }}"
```

A table per pull request goes when the CI job drops it. Records in a shared table stay until `dbt-hodor prune -t ci` deletes them (see [Operations](operations.md#deleting-old-records)). The base target's state table gets CI records only when the CI target writes to it.

## Deploying

A deploy to the base target, or to a higher environment, is an ordinary hodor run on that target, compared with that target's own records:

- a changed model builds (`code_changed`), and its children build when the data they read changed (`new_upstream_data`);
- an added model builds (`no_history`);
- an unchanged model is reused;
- a removed model is not touched. dbt does not drop its table, and hodor keeps its old records until `dbt-hodor prune`.

The hash is environment-neutral, but records are not shared between targets: each environment builds a changed model once, in its own catalog.

## Limits

- **Incremental models.** The first CI run of an incremental model compiles its non-incremental SQL, because the CI table does not exist yet. On the next push it builds once more (the incremental branch), then it is reused.
- **Views without records.** A CI view that hodor has no build record of counts as unknown, so the nodes that read through it build.
- **Tests.** [Development](development.md#tests) lists what the slim CI tests cover.

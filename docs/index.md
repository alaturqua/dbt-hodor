---
title: Hold Old Data, Or Rebuild
hide:
  - navigation
  - toc
---

<div class="hodor-hero" markdown>

![dbt-hodor: Hold Old Data, Or Rebuild](assets/hodor-logo-light.svg#only-light){ .hodor-logo }
![dbt-hodor: Hold Old Data, Or Rebuild](assets/hodor-logo.svg#only-dark){ .hodor-logo }

# dbt-hodor

<p class="hodor-lead">A free, self-hosted alternative to dbt State. It reuses the objects that are already in your warehouse and rebuilds only what changed.</p>

[Get started](getting-started.md){ .md-button .md-button--primary }
[Interactive guide](guide/index.html){ .md-button }

</div>

dbt-hodor is a free, self-hosted alternative to dbt Labs' [dbt State](https://docs.getdbt.com/docs/deploy/dbt-state-about). It runs inside dbt. For every selected node it decides whether to reuse the object that is already in the warehouse or to build it again. A node is reused when its logic has not changed and its upstream data has not changed since the last build.

It needs no dbt platform account and sends nothing to external services. State lives in a Delta table on Databricks, in a Postgres database such as Databricks Lakebase, or in a local DuckDB file.

!!! warning "Status: early development"

    hodor has end-to-end tests on dbt-duckdb and live tests on a Databricks SQL warehouse and on Databricks Lakebase; [the test docs](development.md#tests) list what they cover. It has not run in a large production project yet. Before enabling reuse in production, run in shadow mode for at least a week of scheduled runs and review its decisions. See [the rollout guidance](operations.md#rollout). Changes between releases, including the breaking ones, are in [Upgrading](upgrading.md).

## Why "hodor"

The name is a backronym: **H**old **O**ld **D**ata, **O**r **R**ebuild. That is the only decision hodor makes, once per node.

hodor holds the door in front of your warehouse. A node gets through to a rebuild only when its code changed or new data arrived upstream. Everything else keeps the data it already has.

## What it does for each node

1. **Hashes the logic.** The compiled SQL is normalized with [sqlglot](https://github.com/tobymao/sqlglot), so case, quoting, comments and table aliases do not count as changes.
2. **Checks the upstream data.** Each upstream table has a data version: the Delta commit history on Databricks, the `$history` table on Trino (Iceberg and Delta Lake connectors), the last commit time plus the table's creation and last DDL time on Snowflake, a content fingerprint on DuckDB.
3. **Decides.** It compares both against the record of the node's last build and returns `reuse` or `build`, with a reason code.
4. **Records.** It appends one record per node to the state table.

Every failure leads to a build, never to a reuse. hodor never fails a dbt run.

## Slim CI

A pull request that builds `state:modified+` into a CI catalog or a schema per pull request, under any naming scheme, and reads the rest from a base target with `--defer --state`, works with hodor as it is. The base target is what the pull request is compared with: the main catalog of the same environment, or prod.

- Each push to the PR reuses what the last push built, when the code and the data it reads are unchanged.
- Different catalog and schema names between targets are not code changes.
- A deploy compares only with the deployed target's own history.

![A pull request builds its changed model in the CI catalog, reads unchanged parents from the base target through defer, and keeps its build records apart from the base target's](assets/slim-ci-light.svg#only-light)
![A pull request builds its changed model in the CI catalog, reads unchanged parents from the base target through defer, and keeps its build records apart from the base target's](assets/slim-ci-dark.svg#only-dark)

The [Slim CI](slim-ci.md) page has the setup. The [interactive guide](guide/index.html#/9) plays the scenarios: pushes, new base data, `dbt clone`, the deploy.

## Explore

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } __Getting started__

    ---

    Install hodor and run the local DuckDB example.

    [:octicons-arrow-right-24: Getting started](getting-started.md)

- :material-scale-balance:{ .lg .middle } __Decision rules__

    ---

    The rules and their reason codes.

    [:octicons-arrow-right-24: Decision rules](how-it-works/decision-rules.md)

- :material-source-pull:{ .lg .middle } __Slim CI__

    ---

    Defer, CI schemas, `dbt clone`, deploys.

    [:octicons-arrow-right-24: Slim CI](slim-ci.md)

- :material-cog:{ .lg .middle } __Configuration__

    ---

    Node configs and settings.

    [:octicons-arrow-right-24: Configuration](configuration.md)

- :material-database:{ .lg .middle } __State stores__

    ---

    Delta on Databricks, Postgres or Lakebase, or a local DuckDB file.

    [:octicons-arrow-right-24: State stores](stores.md)

- :material-shield-check:{ .lg .middle } __Operations__

    ---

    Modes, rollout, fail-open behavior, known limits.

    [:octicons-arrow-right-24: Operations](operations.md)

- :material-play-circle:{ .lg .middle } __Interactive guide__

    ---

    A single-page walkthrough, with a run simulator and slim CI scenarios.

    [:octicons-arrow-right-24: Open the guide](guide/index.html)

</div>

## What is built and what is planned

| Part | State |
| --- | --- |
| Decisions for `table`, `incremental` and `view` models, seeds, snapshots and data tests | built, tested end to end on dbt-duckdb |
| `delta` store and Delta history data versions | built, tested live on a Databricks workspace (`pytest -m databricks`) |
| `duckdb` store and content fingerprint | built, tested |
| `dbt-hodor explain` CLI | built, tested end to end on dbt-duckdb; not yet run live on Databricks |
| `dbt-hodor prune` CLI | built, tested end to end on dbt-duckdb |
| `postgres` store, Lakebase included | built, tested end to end against Lakebase (`pytest -m postgres`) |
| Slim CI: `--defer`, `--favor-state`, environment-neutral hash | built, tested end to end on dbt-duckdb and live on Databricks |
| PyPI package | [published](https://pypi.org/project/dbt-hodor/) |

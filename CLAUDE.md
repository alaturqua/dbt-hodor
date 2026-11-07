@AGENTS.md

## Working in Claude Code

Work inline by default. Spawn an agent only when it pays for its cold start:

- `runner`: full pytest, mkdocs build or other long output; returns failures only.
- `reviewer`: fresh-eyes review of the final diff before a commit.
- `Explore`: broad searches spanning many files; not for one-symbol lookups.
- Several independent tasks that can run in parallel.

Debug with the `superpowers:systematic-debugging` skill inline. When you do spawn an
agent, give it the file paths and the check to run, so it does not re-explore.

Skills: `karpathy-guidelines` for every code change, `dbt-state-alternative-build` for
hodor settings and dbt flags, `git-workflow-and-versioning` for commits, branches and
releases.

Databricks work loads `databricks:databricks-core` first, then the one that matches:

- `databricks-dbsql`: SQL warehouse and the Delta state store.
- `databricks-unity-catalog`: catalog, schema and grants for the state table.
- `databricks-lakebase`: the Lakebase Postgres store and its endpoints.
- `databricks-python-sdk`: the `databricks-sdk` token path in `store/postgres.py`.
- `databricks-jobs`, `databricks-dabs`: `dbt_task` jobs and bundles for live runs.
- `databricks-docs`: anything the others do not cover.

Never pick a Databricks profile yourself; pass `--profile` and let the user choose.

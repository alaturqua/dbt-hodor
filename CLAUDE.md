@AGENTS.md

## Working in Claude Code

The main session coordinates: it plans, decides and answers. Delegate the rest to the
user-level agents, cheapest first, and run independent tasks in parallel:

- `Explore`: find files, symbols and usages.
- `runner`: pytest, ruff, mkdocs build; returns failures only.
- `implementer`: scoped edits once the approach is decided.
- `reviewer`: read-only review of the diff before a commit.
- `deep-debugger`: only after a cheaper attempt failed or the cause is unclear.

Give each agent the file paths and the check to run, so it does not re-explore.

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

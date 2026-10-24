# Development

Requires [uv](https://docs.astral.sh/uv/). Local development and tests run on DuckDB, with no Databricks access.

```sh
uv sync
uv run pytest
uv run ruff check
uv run ruff format
```

`dbt-hodor` is available on [PyPI](https://pypi.org/project/dbt-hodor/).
Build a wheel from the source checkout with `uv build`.

CI builds a wheel and an sdist on every push to `main` and keeps them for 14 days, so an
installable build is always at hand without cutting a release:

```sh
gh run download -n dist            # the latest successful run on main
uv pip install dist/dbt_hodor-*-py3-none-any.whl
```

## Changelog and releases

`CHANGELOG.md` is generated from the commit history with [git-cliff](https://git-cliff.org),
so commit messages follow [Conventional Commits](https://www.conventionalcommits.org). See
[CONTRIBUTING.md](https://github.com/alaturqua/dbt-hodor/blob/main/CONTRIBUTING.md) for the
prefixes.

```sh
uv run git-cliff --unreleased --strip all   # what the next release notes would say
uv run git-cliff --output CHANGELOG.md      # rewrite the changelog
```

A release is a `v*` tag. The release workflow refuses to build when the tag and the version
in `pyproject.toml` disagree, runs the checks on the tagged commit, generates the notes for
that tag and drafts a GitHub release with the wheel and the sdist. Its PyPI publishing job
is gated by the `PUBLISH_TO_PYPI` repository variable and PyPI trusted-publisher configuration.

The benchmarks are marked `benchmark` and skipped by default. Run them with `uv run pytest -m benchmark`.

## Docs

The site is published to <https://alaturqua.github.io/dbt-hodor/> by the `Docs site`
workflow on every push to `main` that touches `docs/` or `mkdocs.yml`.

```sh
uv run --group docs mkdocs serve
uv run --group docs mkdocs build --strict
```

The docs tools live in the `docs` dependency group, not in the package dependencies.

## Project layout

```text
src/dbt_hodor/
  __init__.py    cheap import; exposes `plugins` lazily
  plugin.py      initialize(): mode check, command gate, guard, install patches
  state_config.py  make dbt merge the `state:` config key (derived from upstream)
  patches.py     patched ModelRunner.execute, test execute, result lines (model, seed,
                 snapshot, test), run-end flush
  runtime.py     one run: settings, store, records, per-node decisions
  config.py      state: keys and hodor settings
  hashing.py     normalized SQL and config hashes (sqlglot)
  lineage.py     upstream tables per node
  metadata.py    data versions: Delta history, DuckDB fingerprint
  providers.py   per-adapter metadata providers
  engine.py      pure decision rules (no I/O)
  records.py     record schema and SQL
  store/         delta.py, duckdb.py, postgres.py
  explain.py     dbt-hodor explain: report and text output
  cli.py         the dbt-hodor command (ddl, explain, prune)
tests/           unit tests per module
tests/e2e/       a local dbt project run with dbt-duckdb
tests/integration/  the opt-in live tests on Databricks and Trino
```

## How the plugin loads

dbt imports every installed top-level module named `dbt_*` that has a `plugins` attribute. `dbt_hodor` defines `plugins` through a module-level `__getattr__`, so the dbt-facing code loads only when dbt asks for it. The plugin hooks run at parse time only, so changing how nodes run needs monkeypatches. `plugin.py` and `patches.py` install them for `run`, `build`, `test`, `seed` and `snapshot`.

## Tests

- The engine has table-driven unit tests, one case per rule.
- The end-to-end test in `tests/e2e/test_duckdb.py` runs dbt-duckdb in process: first run builds, second run reuses, new upstream data rebuilds, a changed seed file rebuilds the seed and its children, a code change rebuilds, a write outside dbt rebuilds, tests replay, `--full-refresh` builds. It also covers shadow and off modes, hooks on reuse, `--vars` and model versions.
- The live test in `tests/integration/test_databricks.py` runs the same lifecycle on a Databricks SQL warehouse, including `replace_where`, a seed and a snapshot. It runs once with `use_materialization_v2` off and once with it on, and covers a contract with a primary key, a row filter and column masks (UC functions it creates in the scratch schema), date-window and stamp models, and a table swapped in by `use_safer_relation_operations`. Column masks on tables are only asserted with v2, because dbt-databricks applies them only there. It is marked `databricks` and skipped by default. It needs these environment variables: `HODOR_DBX_HOST`, `HODOR_DBX_HTTP_PATH`, `HODOR_DBX_TOKEN`, `HODOR_DBX_CATALOG` and `HODOR_DBX_SCHEMA`. Pass a short-lived token for that one process only, for example from `databricks auth token --profile <profile>`, and never write it to a file. The test creates everything in `HODOR_DBX_SCHEMA` and drops that schema with `CASCADE` at the end, so point it at a schema that holds nothing else.
- The slim CI tests in `tests/e2e/test_slim_ci.py` run a base target and a CI target on two DuckDB databases, with CI schemas through `generate_schema_name`, `--defer` with and without `--favor-state`, base views read through defer, tables made by `dbt clone`, CI records in a state table per pull request, views changed in their SQL or through a macro, and deploys with changed, added and removed models. The live slim CI test covers tables made by `dbt clone`, reuse across pushes and rebuilds after new base data; `--favor-state` and deploys are covered on DuckDB only. That test, `tests/integration/test_databricks_slim_ci.py`, also needs `HODOR_DBX_CI_CATALOG`, a second catalog in the same metastore. It builds the base target in `HODOR_DBX_CATALOG.<HODOR_DBX_SCHEMA>_slim_ci` and the CI target in `HODOR_DBX_CI_CATALOG.pr_7_<HODOR_DBX_SCHEMA>_slim_ci`, so it never shares a schema with the other live test, and drops both schemas with `CASCADE` at the end.

    ```sh
    uv run pytest -m databricks tests/integration
    ```

    !!! warning "Not from Git Bash on Windows"

        Git Bash rewrites an environment value that looks like an absolute POSIX path when it starts a native program, so `HODOR_DBX_HTTP_PATH=/sql/1.0/warehouses/<id>` reaches Python as `C:/Program Files/Git/sql/1.0/warehouses/<id>` and the connection fails with a 404 on `OpenSession`. Use PowerShell or `cmd`, or set `MSYS_NO_PATHCONV=1`.

- The live test in `tests/integration/test_trino_iceberg.py` runs real dbt-trino builds on a Trino Iceberg catalog with the state in a local DuckDB file: first run builds, second run reuses, a new source row rebuilds the tables while the unchanged view stays reused, a rollback with `rollback_to_snapshot` rebuilds them, a changed view definition rebuilds the view, and `ALTER TABLE ... ADD COLUMN` on a source rebuilds a `select *` table. It covers the Iceberg and Delta Lake connectors as described in [Data versions](how-it-works/data-versions.md#trino-iceberg-and-delta-lake-connectors). It is marked `trino` and skipped by default, and skips itself when no server answers. `HODOR_TEST_TRINO_HOST` and `HODOR_TEST_TRINO_PORT` default to `localhost` and `8080`. The test creates one schema per run and drops it at the end.

    ```sh
    docker run -d --name hodor-trino -p 8080:8080 \
      -v "$PWD/tests/integration/trino_catalog:/etc/trino/catalog" \
      --tmpfs /data:uid=1000 trinodb/trino:476
    uv run pytest -m trino tests/integration
    ```

    The mounted directory holds the catalog file `tests/integration/trino_catalog/iceberg.properties`. Before running the test, check that the server answers: `docker exec hodor-trino trino --execute "select 1"`.

- The live test in `tests/e2e/test_postgres_state.py` runs real dbt-duckdb builds with the state in Postgres: first run records, second run reuses, new upstream data rebuilds the table while the unchanged view stays reused, a code change rebuilds. The warehouse stays DuckDB, so it tests the store and the decision path rather than a warehouse. It is marked `postgres` and skipped by default, and it works against Databricks Lakebase or any other Postgres. `HODOR_PG_SCHEMA` is dropped with `CASCADE` at the start and the end, so point it at a schema that holds nothing else.

    Lakebase, with workspace credentials for `databricks-sdk`:

    ```sh
    DATABRICKS_CONFIG_PROFILE=<profile> \
    HODOR_PG_ENDPOINT=projects/<project>/branches/<branch>/endpoints/<endpoint> \
    HODOR_PG_DATABASE=databricks_postgres HODOR_PG_SCHEMA=hodor_e2e \
    uv run pytest -m postgres tests/e2e/test_postgres_state.py
    ```

    Any other Postgres, for example one in Docker:

    ```sh
    docker run -d --name hodor-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
    HODOR_PG_HOST=localhost HODOR_PG_USER=postgres HODOR_PG_PASSWORD=postgres \
    HODOR_PG_DATABASE=postgres HODOR_PG_SCHEMA=hodor_e2e HODOR_PG_SSLMODE=disable \
    uv run pytest -m postgres tests/e2e/test_postgres_state.py
    ```

## Line endings

Every file uses LF line endings and UTF-8 (`.editorconfig`, `.gitattributes` with `eol=lf`). On Windows, check new files with `git ls-files --eol` and aim for `w/lf`.

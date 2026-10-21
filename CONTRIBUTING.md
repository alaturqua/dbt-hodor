# Contributing to dbt-hodor

Thanks for looking. hodor decides whether a dbt node is reused or rebuilt, so a wrong reuse
leaves someone with stale data they believe is fresh. That shapes everything below: changes
need a test, and anything uncertain must build rather than reuse.

dbt-hodor is in early development and the API is not stable yet. Opening an issue before a
large change saves you work.

## Before you start

- **Bugs**: open an issue with the dbt version, the adapter and version, the store, the
  reason code hodor logged, and what you expected instead. The output of
  `dbt-hodor explain -s <node>` for the affected node is the single most useful thing you can
  attach.
- **Features**: open an issue first and describe the case. Some things are deliberately out
  of scope, listed in section 3 of the [design spec](docs/design.md).
- **Security**: do not open a public issue. Use GitHub's "Report a vulnerability" button
  under the Security tab, which opens a private advisory.
- **A wrong reuse is a security-grade bug.** If you find an input where hodor reuses a node
  it should have rebuilt, report it privately first.

## Setting up

Requires [uv](https://docs.astral.sh/uv/). Everything runs locally on DuckDB, with no
Databricks or Postgres access needed.

```sh
uv sync
uv run pytest
uv run ruff check
uv run ruff format
```

The documentation is published at <https://alaturqua.github.io/dbt-hodor/>; `uv run --group docs mkdocs serve` builds it locally.
[docs/development.md](docs/development.md) has the project layout, how the plugin loads into
dbt, and how to run the opt-in live tests.

## Making a change

1. **Write the test first.** The engine has table-driven unit tests, one case per rule, and
   that is usually where a decision change belongs. A change in behaviour with no test will
   be asked for one.
2. **Keep decisions pure.** `engine.py` holds the rules and does no I/O. Hashing, lineage,
   metadata and the stores stay separate. Please do not move I/O into the engine.
3. **Fail open.** Every failure path must end in a build, never a reuse, and must never fail
   the dbt run. If you add a new way for hodor to fail, add the test that proves it builds.
4. **Run the checks.** `uv run pytest`, `uv run ruff check` and `uv run ruff format` all have
   to pass. Benchmarks (`-m benchmark`), the Databricks live test (`-m databricks`) and the
   Postgres live test (`-m postgres`) are skipped by default and need credentials, so they
   are not expected from an outside contributor. Say in the pull request which of them you
   ran, if any.
5. **Update the documentation in the same change.** If you add or change a setting, update
   [docs/configuration.md](docs/configuration.md), the table in `README.md` and the settings
   table in `docs/guide/index.html`. A new reason code also belongs in
   [docs/how-it-works/decision-rules.md](docs/how-it-works/decision-rules.md).
6. **LF line endings, UTF-8.** Enforced by `.editorconfig` and `.gitattributes`. On Windows,
   check with `git ls-files --eol` and aim for `w/lf`.

## Pull requests

- One topic per pull request. A small, focused change gets reviewed faster than a large one.
- Write the pull request body for a reader who has not seen the issue: what changed, why, and
  what you ran to check it.
- Sign off your commits with the [Developer Certificate of Origin](https://developercertificate.org/):
  `git commit -s`. That adds a `Signed-off-by` line and states that you have the right to
  submit the work under this project's licence.
- Describe what the change does, not the process that produced it.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org), because `CHANGELOG.md` and the
release notes are generated from the history with [git-cliff](https://git-cliff.org).

```text
feat(store): add a Postgres state store
fix(hashing): treat a changed CTE alias as a code change
fix(reuse): rebuild when an upstream table was dropped and recreated
docs(stores): document the prune command
```

| Prefix | Goes into |
| --- | --- |
| `feat` | Features |
| `fix` | Bug fixes |
| `fix(reuse)` or `fix(correctness)` | Correctness: reuse decisions, at the top of the release notes |
| `perf`, `refactor`, `docs`, `test`, `build`, `ci` | their own section |
| `feat!` or a `BREAKING CHANGE:` footer | marked as breaking |

Use `fix(reuse)` for anything that changes hodor from reusing to building in a case it got
wrong. Those entries are what a reader upgrading cares about most, so they get their own
heading.

A commit that does not follow the convention still lands in the changelog under "Other", so
nothing is lost. Preview what your commits will produce:

```sh
uv run git-cliff --unreleased --strip all
```

## Writing style for documentation

Plain sentences. No marketing language, no emoji. Prefer stating a limitation over claiming
completeness. A claim about what is tested should point at a test that actually runs.

## Licence

dbt-hodor is licensed under Apache-2.0, and contributions are accepted under the same
licence. Two files carry an upstream copyright header; if you change one, leave the header in
place. [NOTICE](NOTICE) says which files and why.

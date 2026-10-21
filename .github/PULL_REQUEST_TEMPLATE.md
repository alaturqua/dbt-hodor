<!-- Write for a reader who has not seen the issue. -->

## What this changes

<!-- One or two sentences. Link the issue if there is one: Closes #123 -->

## Why

<!-- The problem it solves. -->

## How it was checked

<!-- Name the tests you added or changed, and which suites you ran. -->

- [ ] `uv run pytest`
- [ ] `uv run ruff check` and `uv run ruff format --check`
- [ ] `uv run --group docs mkdocs build --strict` (if docs or settings changed)
- [ ] `uv run pytest -m postgres` (needs a Postgres or Lakebase)
- [ ] `uv run pytest -m databricks` (needs a Databricks workspace)

The last two need credentials and are not expected from an outside contributor. Say which of
them you ran, if any.

## Correctness

- [ ] Every new failure path ends in a **build**, never a reuse, and never fails the dbt run.
- [ ] If this changes when a node is reused, there is a test for the case that must still build.
- [ ] Decision logic stays in `engine.py` and does no I/O.

## Documentation

- [ ] A new or changed setting is in `docs/configuration.md`, the `README.md` table and the
      settings table in `docs/guide/index.html`.
- [ ] A new reason code is in `docs/how-it-works/decision-rules.md`.

## Sign-off

- [ ] Commits are signed off with `git commit -s` ([DCO](https://developercertificate.org/)).

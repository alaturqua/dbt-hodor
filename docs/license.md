# License

dbt-hodor is licensed under Apache-2.0. See `LICENSE` and `NOTICE` in the repository.

## Derived files

Parts of the dbt integration are derived from [dbt-state](https://github.com/dbt-labs/dbt-state) (commit `b0f3030`), Copyright 2026 Fivetran, Inc., licensed under Apache-2.0. Each derived file says so in its header and states that it was modified for hodor:

- `src/dbt_hodor/plugin.py` (from `clients/dbt_state/src/dbt_state/plugin.py`)
- `src/dbt_hodor/patches.py` (from `clients/dbt_state/src/dbt_state/runner.py` and `clients/dbt_state/src/dbt_state/run_cache.py`)

The engine, hashing, lineage, metadata, store and CLI modules are written new for hodor. hodor has no runtime or package dependency on dbt-state.

## Trademarks

dbt is a trademark of dbt Labs, Inc. dbt-hodor is not affiliated with or endorsed by dbt Labs, Inc. or Fivetran, Inc.

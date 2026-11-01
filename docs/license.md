# License

dbt-hodor is licensed under Apache-2.0. See `LICENSE` and `NOTICE` in the repository.

## Derived files

Parts of the dbt integration are derived from [dbt-state](https://github.com/dbt-labs/dbt-state) (commit `b0f3030`), Copyright 2026 Fivetran, Inc., licensed under Apache-2.0. Each derived file says so in its header and states that it was modified for hodor:

- `src/dbt_hodor/state_config.py` (from `clients/dbt_state/src/dbt_state/plugin.py`): the `state:` config merge. This is the only upstream-derived logic in hodor; it is kept in its own module so the rest of the plugin is unencumbered
- `src/dbt_hodor/plugin.py` (from `clients/dbt_state/src/dbt_state/plugin.py`): the plugin entry point
- `src/dbt_hodor/patches.py` (from `clients/dbt_state/src/dbt_state/runner.py`)

The engine, hashing, lineage, metadata, store and CLI modules are written new for hodor. hodor has no runtime or package dependency on dbt-state.

## Generated files

The Python files under `src/dbt_hodor/v2/proto/` are generated from the protocol buffer definitions in [dbt](https://github.com/dbt-labs/dbt) (tag `v2.0.8`), copyright dbt Labs, Inc., licensed under Apache-2.0. `scripts/gen_v2_stubs.py` regenerates them.

## Trademarks

dbt is a trademark of dbt Labs, Inc. dbt-hodor is not affiliated with or endorsed by dbt Labs, Inc. or Fivetran, Inc.

# Snowflake

hodor on `dbt-snowflake`, tested with `dbt-snowflake` 1.12.4. The live test passed on Snowflake 10.38.

## Install

=== "uv"

    ```sh
    uv pip install dbt-hodor dbt-snowflake
    # with the local DuckDB state store
    uv pip install "dbt-hodor[duckdb]"
    ```

=== "pip"

    ```sh
    pip install dbt-hodor dbt-snowflake
    # with the local DuckDB state store
    pip install "dbt-hodor[duckdb]"
    ```

## Profile

On the `snowflake` adapter the default store, `delta`, turns hodor off, so set `hodor_store` to `duckdb` or `postgres`. The `snowflake` adapter has no state store of its own yet. A `profiles.yml` entry with key-pair authentication and the state in a local DuckDB file, with placeholder values:

```yaml
hodor_demo:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: <account_identifier>
      user: <user>
      private_key_path: <path/to/private_key.p8>
      role: <role>
      warehouse: <warehouse>
      database: <database>
      schema: demo
      threads: 2
      hodor_store: duckdb
      hodor_path: .hodor/state.duckdb
```

## State store

The default store, `delta`, turns hodor off, so set `hodor_store` to [`duckdb`](../stores/duckdb.md) or [`postgres`](../stores/postgres.md). The `snowflake` adapter has no state store of its own yet. The `postgres` store uses its own connection and was not run together with Snowflake. See [State stores](../stores/index.md).

## Data version

hodor runs two queries per table. The first reads `created` and `last_ddl` from `<database>.information_schema.tables` (base tables only), as nanoseconds since the epoch. The second calls `SYSTEM$LAST_CHANGE_COMMIT_TIME` on the table, addressed by the name Snowflake stores. The token is `<commit>|<created>|<last_ddl>`. Relation keys are lowercase and Snowflake stores unquoted names in uppercase, so hodor finds the table with `lower()` and then quotes the stored name.

What moves each part, checked on Snowflake 10.38 by the live test `test_snowflake_token_moves_on_each_change`:

| Change | commit token | `created` | `last_ddl` |
| --- | --- | --- | --- |
| insert, update (even 0 rows), delete, truncate, merge | moves | no | no |
| `CREATE OR REPLACE TABLE`, CTAS, `SWAP WITH` | moves (a plain `CREATE OR REPLACE` of an empty table goes back to 0) | moves | moves |
| `ALTER TABLE ADD COLUMN`, `COMMENT` | no | no | moves |

A plain read moves nothing. The live lifecycle test also builds with dbt through insert, `ADD COLUMN` and `CREATE OR REPLACE` (see [Development](../development.md)). A `COMMENT` and a 0-row update move the token, so they cause an extra build, never a wrong reuse.

- **Unknown:** no matching base table, more than one match, a dynamic, hybrid or Iceberg table (the query leaves them out, because their change tokens were not tested; an account where `is_dynamic`, `is_hybrid` or `is_iceberg` is empty gets no match and one where a column is missing gets a query error, so every table builds), a null or non-integer value, or an error from `SYSTEM$LAST_CHANGE_COMMIT_TIME` (for example a missing table). Dependents build.
- **Views:** a view is hashed from its definition in `<database>.information_schema.views`. Two views whose names differ only in case give an unknown version, on every adapter.
- **Not supported:** stages and `@stage` reads are untraceable inputs, so the node builds. The `dynamic_table` materialization is unsupported, so those models always build.
- **State store:** on the `snowflake` adapter the state store can be `duckdb` or `postgres`. The default, `delta`, turns hodor off. The live tests use `duckdb`; the `postgres` store uses its own connection and was not run together with Snowflake.

## Limits

- A `COMMENT` and a 0-row update move the token, so they cause an extra build, never a wrong reuse.
- Dynamic, hybrid and Iceberg tables are left out of the query, because their change tokens were not tested. Their dependents build.
- Stages and `@stage` reads are untraceable inputs, so the node builds.
- The `dynamic_table` materialization is unsupported, so those models always build.
- Two views whose names differ only in case give an unknown version.

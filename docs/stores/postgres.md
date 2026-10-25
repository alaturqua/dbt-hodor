# postgres

Any Postgres, including Databricks Lakebase. Status: built, tested end to end against Lakebase. `lakebase` is accepted as an alias for `postgres`.

The point of this store is that state lives outside the warehouse: point reads and appends without SQL warehouse time, and a dbt project on any adapter can keep its state in a Postgres you already run.

- Table: `<hodor_schema>.<hodor_table>` in the database `hodor_database`. hodor creates the schema and the table on first use, so the connecting role needs `CREATE` on the database.
- It needs `psycopg` 3: `pip install dbt-hodor[postgres]`, or `dbt-hodor[lakebase]` which adds `databricks-sdk` for the token.
- Writes: one `executemany` per flush. Reads: one statement at run start.
- Concurrency: appends from concurrent jobs do not conflict.
- Connections are opened once per run and reopened once on a dropped connection or an expired token.

=== "Lakebase"

    `hodor_endpoint` is a Lakebase endpoint resource path. It supplies the host, the user and an OAuth token, so no password is stored anywhere. The token lasts an hour and is fetched again whenever hodor reconnects.

    ```yaml
    hodor_store: lakebase
    hodor_endpoint: projects/<project>/branches/<branch>/endpoints/<endpoint>
    hodor_database: databricks_postgres
    hodor_schema: ops
    ```

    Workspace credentials come from `databricks-sdk`, so `DATABRICKS_CONFIG_PROFILE`, or `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, select the workspace. Find the endpoint path with `databricks postgres list-endpoints projects/<project>/branches/<branch>`.

=== "Any other Postgres"

    ```yaml
    hodor_store: postgres
    hodor_host: db.example.com
    hodor_port: 5432
    hodor_user: hodor
    hodor_password: "{{ env_var('HODOR_PASSWORD') }}"
    hodor_database: hodor
    hodor_schema: ops
    hodor_sslmode: require
    ```

    Keep the password in an environment variable, not in `profiles.yml`. `HODOR_PASSWORD` sets it directly. Use `hodor_sslmode: disable` only for a local container.

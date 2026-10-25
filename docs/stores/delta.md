# delta

The default store on Databricks. Status: built, tested live on a Databricks workspace.

- Table: `<hodor_database>.<hodor_schema>.<hodor_table>`, for example `my_catalog.ops.hodor_state`. At run start hodor checks that the table exists (`SELECT * FROM <table> LIMIT 0`). Only when it is missing does hodor run `CREATE TABLE IF NOT EXISTS`.
- Layout: `CLUSTER BY (target_name, node_key)`, so the run-start read skips most files as history grows.
- The schema must exist. hodor never creates it.
- Job principals often hold `MODIFY` but not `CREATE TABLE` on the schema. Then an admin creates the table once, manually. Print the statement with:

    ```sh
    uv run dbt-hodor ddl --database my_catalog --schema ops
    ```

    If hodor cannot create the table, it turns itself off for the run with a warning that names this command.
- Writes: batched `INSERT INTO ... VALUES`, 500 rows per statement, through dbt's own adapter connection. hodor needs no other credentials or drivers.
- Concurrency: blind appends from concurrent jobs do not conflict in Delta.
- Grants: `MODIFY` (and `SELECT`) for the job service principals, `SELECT` for everyone else.

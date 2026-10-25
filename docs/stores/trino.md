# trino

The default store on the `trino` adapter. Status: built. Tested live on Trino 476 in Docker with the Iceberg connector. The Delta Lake connector is covered by unit tests only.

- Table: `<hodor_database>.<hodor_schema>.<hodor_table>`, for example `iceberg.ops.hodor_state`. `hodor_database` is a Trino catalog and defaults to the target's `database`. The catalog decides the table format: Iceberg or Delta Lake. hodor needs both `hodor_database` and `hodor_schema`, as for `delta`; without them it turns itself off for the run with a warning.
- At run start hodor checks that the table exists (`SELECT * FROM <table> LIMIT 0`). Only when it is missing does it run `CREATE TABLE IF NOT EXISTS`. The columns are `varchar` and `bigint`; hodor sets no partitioning or clustering.
- The schema must exist. hodor never creates it. The live test uses a file metastore, where it creates the schema with `location = 'local:///<name>'`. That is a property of that test setup, not a hodor requirement.
- Writes: batched `INSERT INTO ... VALUES`, 500 rows per statement, through dbt's own adapter connection. hodor needs no other credentials or drivers. String values are written as Trino literals: `'` is doubled and backslashes and newlines pass through unchanged.
- Each run's insert makes one Iceberg snapshot per batch. Iceberg keeps old snapshots and small files until `optimize` and `expire_snapshots` run. hodor does not run them. See [Operations](../operations.md#deleting-old-records).
- A failed insert, for example from an Iceberg commit conflict when two runs write at once, is caught: hodor warns that the records are lost and the next run builds those nodes (`no_history`). hodor does not retry. Concurrent runs were not tested.
- A Delta Lake catalog needs object storage. Trino 476 cannot write a Delta Lake table to local disk, so the Delta Lake path was not run live.
- `hodor_store: delta` on a `trino` target, or `trino` on another adapter, turns hodor off for the run with a warning.
- Grants: insert and select (and delete, for `prune`) on the table for the job users, select for everyone else.

Print the statement for a manual creation with:

```sh
uv run dbt-hodor ddl --store trino --database iceberg --schema ops
```

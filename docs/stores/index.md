# State stores

The state store holds one append-only table, `hodor_state`. The store loads the latest build records at run start and appends new records. It makes no decisions.

!!! danger "The state table decides what is skipped"

    Anyone who can write the table can force reuse. Grant `MODIFY` on it only to the job service principals. Everyone else gets `SELECT`.

| Store | Location | Created on first use |
| --- | --- | --- |
| `delta` | `<hodor_database>.<hodor_schema>.<hodor_table>` | the table only. The schema must exist, because creating one needs catalog-level grants |
| `trino` | `<hodor_database>.<hodor_schema>.<hodor_table>` in a Trino catalog | the table only. The schema must exist |
| `duckdb` | table `<hodor_schema>.<hodor_table>` in the file `hodor_path` | the file, the schema and the table |
| `postgres` (alias `lakebase`) | `<hodor_schema>.<hodor_table>` in the Postgres database `hodor_database` | the schema and the table |

Warehouse stores write through dbt's own adapter connection, so they need no extra package.

| Store | Works on adapters | Install |
| --- | --- | --- |
| [`delta`](delta.md) | `databricks` | no extra |
| [`trino`](trino.md) | `trino` | no extra |
| [`duckdb`](duckdb.md) | any supported adapter | `dbt-hodor[duckdb]` |
| [`postgres`](postgres.md) (alias `lakebase`) | any supported adapter | `dbt-hodor[postgres]`, or `dbt-hodor[lakebase]` for Lakebase |

The record columns and `dbt-hodor prune` are described in [Records and retention](records.md).

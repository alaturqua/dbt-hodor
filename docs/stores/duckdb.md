# duckdb

For local development and tests. Status: built and tested.

- One file, default `.hodor/state.duckdb`. It needs the `duckdb` package (`pip install dbt-hodor[duckdb]`).
- hodor creates the file, the schema (default `main`) and the table on first use.
- One process writes at a time, because DuckDB locks the file. A second dbt process that cannot open the file runs fail-open: everything builds, nothing is recorded, and hodor warns.
- Export to parquet: `COPY hodor_state TO 'x.parquet'`.

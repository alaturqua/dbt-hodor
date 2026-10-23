# Lineage

For each node, hodor needs the list of tables whose data the node reads.

## dbt parents

Parents come from `depends_on.nodes`. dbt also adds the `to` model or source of a foreign key constraint there, so a model with a foreign key builds when the referenced table gets new data, even if its SQL does not read that table.

- Tables and incremental models give their own data version.
- Views and ephemeral models are expanded through the manifest to their own upstream tables, recursively. hodor memoizes the result for the run.
- For any other type (for example metric views), the version is unknown.

## Sources

A source gives its Delta data version. A source that is not a Delta table (a foreign table, a view) is unknown.

## Deferred parents

With `--defer`, dbt may compile a parent as the relation of the `--state` manifest, for example the base table instead of the CI copy. hodor checks which relation the compiled SQL names and takes that node's data version. A deferred view is expanded with the `--state` manifest, to the tables it reads in that environment. See [Slim CI](../slim-ci.md).

## External inputs

Table references in the compiled SQL that match no parent or source:

- A Delta path reference such as ``delta.`<path>` `` gives the Delta data version of that path.
- `read_files(...)`, other table functions and any other reference are unknown.

!!! note

    A node that reads `read_files(...)` always builds (`unknown_upstream`). The DuckDB example `select * from range(3)` is also an untraceable input.

## Tests

The upstreams of a test are the nodes it tests. A `relationships` test has two: the model it is defined on and the `to` model, so new data on either side runs it again. A test key combines the test's unique id with a hash of the tested relation names.

# Security policy

## Supported versions

dbt-hodor is in early development. Only the latest release gets fixes. There is no
long-term support branch yet.

## Reporting a vulnerability

Do not open a public issue. Use the **Report a vulnerability** button under this
repository's Security tab, which opens a private advisory. Please include the dbt version,
the adapter, the store, and the smallest project or input that shows the problem.

You should get a first reply within a week. If a fix is needed, the advisory stays private
until a release is out.

## What counts as a vulnerability here

hodor decides whether a dbt node is reused or rebuilt, so the interesting failures are not
the usual ones:

- **A wrong reuse.** Any input where hodor reuses a node whose code or upstream data
  changed. This is the most serious class of bug in this project: a run reports success and
  leaves stale data behind that everyone downstream believes is fresh. Report it privately
  even if you think it needs unusual conditions.
- **A hash collision or normalization flaw** that makes two different models or configs look
  identical.
- **SQL injection through a setting.** Database, schema and table names come from
  `profiles.yml` and are quoted, and record values are bound as parameters where the driver
  allows it. An input that escapes either is a vulnerability.
- **Credential exposure.** A token, password or connection string that reaches a log line,
  an error message or the state table.

## What does not

- A build that was not needed. hodor is designed to build when anything is uncertain, so a
  missed reuse costs warehouse time but never correctness. Open a normal issue.
- Write access to the state table. Anyone who can write it can force a reuse, which is by
  design and documented in [docs/stores.md](docs/stores.md): grant `MODIFY` only to the job
  identities. A deployment that grants it widely is a configuration problem, not a flaw in
  hodor.

"""State in a Postgres table: Databricks Lakebase, or any other Postgres."""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from typing import Any

from dbt_hodor.config import IDENT_RE
from dbt_hodor.records import (
    COLUMN_NAMES,
    Record,
    add_column_sql,
    create_table_sql,
    invocation_sql,
    latest_builds_sql,
    latest_records_sql,
    lookup_index_name,
    lookup_index_sql,
    prune_sql,
)
from dbt_hodor.store import StoreError

# The password for one connection attempt. Called again for every reconnect, because a
# Lakebase OAuth token lasts an hour and a run can take longer.
Credential = Callable[[], str]

CONNECT_TIMEOUT_SECONDS = 10
# The first CREATE INDEX on a large table holds a lock that blocks appends; it gets this long
# (the statement timeout covers the wait for the lock too), then the open goes on without it.
INDEX_BUILD_TIMEOUT = "10s"


def lakebase_target(endpoint: str) -> tuple[str, str, Credential]:
    """Host, user and token provider for a Lakebase endpoint resource path.

    The endpoint path is projects/<project>/branches/<branch>/endpoints/<endpoint>. Workspace
    credentials come from the databricks-sdk, so DATABRICKS_HOST and DATABRICKS_TOKEN or
    DATABRICKS_CONFIG_PROFILE select the workspace.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise StoreError(
            "the lakebase store needs databricks-sdk: pip install 'dbt-hodor[lakebase]'"
        ) from exc
    try:
        client = WorkspaceClient()
        status = client.postgres.get_endpoint(endpoint).status
        host = status.hosts.host if status and status.hosts else None
        user = client.current_user.me().user_name
    except Exception as exc:  # noqa: BLE001 - any SDK failure means fail open
        raise StoreError(f"cannot read the Lakebase endpoint {endpoint!r}: {exc}") from exc
    if not host or not user:
        raise StoreError(f"the Lakebase endpoint {endpoint!r} reports no host")

    def token() -> str:
        try:
            credential = WorkspaceClient().postgres.generate_database_credential(endpoint)
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot get a Lakebase token for {endpoint!r}: {exc}") from exc
        if not credential.token:
            raise StoreError(f"the Lakebase token for {endpoint!r} is empty")
        return credential.token

    return host, user, token


class PostgresStore:
    def __init__(
        self,
        database: str,
        schema: str,
        table: str,
        host: str,
        port: int,
        user: str,
        credential: Credential,
        sslmode: str = "require",
        read_only: bool = False,
    ) -> None:
        for part in (database, schema, table):
            if not IDENT_RE.match(part):
                raise StoreError(f"invalid hodor name part {part!r}")
        try:
            import psycopg  # lazy: dbt imports dbt_hodor on every command
        except ImportError as exc:
            raise StoreError(
                "the postgres store needs psycopg: pip install 'dbt-hodor[postgres]'"
            ) from exc
        self._psycopg = psycopg
        self._lock = threading.Lock()
        self._table = f'"{schema}"."{table}"'
        self._credential = credential
        self._schema = schema
        self._name = table
        self._has_hashes = True
        self._conninfo: dict[str, Any] = {
            "dbname": database,
            "host": host,
            "port": port,
            "user": user,
            "sslmode": sslmode,
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
        }
        self._connect()
        if read_only:  # dbt-hodor explain: never create or alter the schema or the table
            try:
                self._has_hashes = self._columns_include_hashes()
            except Exception:  # noqa: BLE001 - the read that follows reports the problem
                pass
            return
        try:
            self._run(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            self._run(create_table_sql(self._table, "postgres"))
        except Exception as exc:  # noqa: BLE001
            raise StoreError(
                f"cannot create hodor state table {self._table} (needs CREATE on the database; "
                f"or create it once manually with `dbt-hodor ddl`): {exc}"
            ) from exc
        try:
            if not self._columns_include_hashes():
                self._run(add_column_sql(self._table, "postgres"))
        except Exception as exc:  # noqa: BLE001
            raise StoreError(
                f"cannot add the node_hashes column to {self._table} (needs ALTER on the table; "
                f"or run `ALTER TABLE {self._table} ADD COLUMN node_hashes TEXT` once): {exc}"
            ) from exc
        self._ensure_lookup_index()

    def _ensure_lookup_index(self) -> None:
        """Best effort: without the index reads only get slower, so any failure is ignored."""
        try:
            name = lookup_index_name(self._name)
            # look first: CREATE INDEX IF NOT EXISTS takes a lock on the table even when it exists
            if not self._run(
                "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s",
                [self._schema, name],
            ):
                with self._lock, self._con.transaction(), self._con.cursor() as cur:
                    cur.execute(f"SET LOCAL statement_timeout = '{INDEX_BUILD_TIMEOUT}'")
                    cur.execute(lookup_index_sql(self._schema, self._name))
        except Exception:  # noqa: BLE001 - for example the role does not own the table
            pass

    def _columns_include_hashes(self) -> bool:
        rows = self._run(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            [self._schema, self._name],
        )
        # no rows: the table is missing, and the read that follows reports it
        return not rows or any(row[0] == "node_hashes" for row in rows)

    def _connect(self) -> None:
        try:
            self._con = self._psycopg.connect(
                **self._conninfo, password=self._credential(), autocommit=True
            )
        except Exception as exc:  # noqa: BLE001 - connect or the credential call
            raise StoreError(
                f"cannot connect to hodor state database {self._conninfo['host']}: {exc}"
            ) from exc

    def _run(
        self,
        sql: str,
        params: Sequence[object] | None = None,
        many: Sequence[Sequence] | None = None,
    ) -> list[tuple] | None:
        """One statement, with one reconnect: tokens expire and idle connections are dropped."""
        with self._lock:
            try:
                return self._cursor(sql, params, many)
            except self._psycopg.OperationalError:
                self._connect()  # a fresh token and a new connection, then one more try
                return self._cursor(sql, params, many)

    def _cursor(
        self, sql: str, params: Sequence[object] | None, many: Sequence[Sequence] | None
    ) -> list[tuple] | None:
        with self._con.cursor() as cur:
            if many is not None:
                # all or nothing, so the retry after a dropped connection writes no row twice
                with self._con.transaction():
                    cur.executemany(sql, many)
                return None
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def load(
        self, project_name: str, target_name: str, since_ms: int, before_ms: int | None = None
    ) -> dict[str, Record]:
        sql = latest_builds_sql(
            self._table, "%s", "%s", "%s", None if before_ms is None else "%s", self._has_hashes
        )
        params: list[object] = [project_name, target_name, since_ms]
        records = self._read(sql, params if before_ms is None else [*params, before_ms])
        return {record.node_key: record for record in records}

    def load_latest(
        self,
        project_name: str,
        target_name: str,
        since_ms: int,
        any_status: bool = False,
        reused: bool = False,
    ) -> dict[str, Record]:
        sql = latest_records_sql(
            self._table, "%s", "%s", "%s", self._has_hashes, any_status, reused
        )
        records = self._read(sql, [project_name, target_name, since_ms])
        return {record.node_key: record for record in records}

    def invocation(
        self, project_name: str, target_name: str, invocation_id: str | None
    ) -> list[Record]:
        if invocation_id is None:
            sql = invocation_sql(self._table, "%s", "%s", None, self._has_hashes)
            return self._read(sql, [project_name, target_name] * 2)
        sql = invocation_sql(self._table, "%s", "%s", "%s", self._has_hashes)
        return self._read(sql, [project_name, target_name, invocation_id])

    def _read(self, sql: str, params: list[object]) -> list[Record]:
        try:
            rows = self._run(sql, params)
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot read hodor state: {exc}") from exc
        return [Record.from_row(row) for row in rows or []]

    def append(self, records: Sequence[Record]) -> None:
        if not records:
            return
        placeholders = ", ".join("%s" for _ in COLUMN_NAMES)
        sql = f"INSERT INTO {self._table} ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders})"
        try:
            self._run(sql, many=[record.to_row() for record in records])
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot write hodor state: {exc}") from exc

    def prune(self, project_name: str, target_name: str, before_ms: int, delete: bool) -> int:
        """Always counts first, so a dry run and the real run report the same number."""
        params: list[object] = [project_name, target_name, before_ms]
        try:
            rows = self._run(prune_sql(self._table, "%s", "%s", "%s", delete=False), params)
            count = int((rows or [(0,)])[0][0])
            if delete and count:
                self._run(prune_sql(self._table, "%s", "%s", "%s", delete=True), params)
            return count
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot prune hodor state: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            self._con.close()

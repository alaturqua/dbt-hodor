"""Local state in one DuckDB file."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from pathlib import Path

from dbt_hodor.config import IDENT_RE
from dbt_hodor.records import (
    COLUMN_NAMES,
    Record,
    add_column_sql,
    create_table_sql,
    invocation_sql,
    latest_builds_sql,
    latest_records_sql,
    prune_sql,
)
from dbt_hodor.store import StoreError


class DuckDBStore:
    def __init__(
        self, path: str, schema: str = "main", table: str = "hodor_state", read_only: bool = False
    ) -> None:
        for part in (schema, table):
            if not IDENT_RE.match(part):
                raise StoreError(f"invalid hodor name part {part!r}")
        try:
            import duckdb  # lazy: dbt imports dbt_hodor on every command
        except ImportError as exc:
            raise StoreError(
                "the duckdb store needs duckdb: pip install 'dbt-hodor[duckdb]'"
            ) from exc
        self._error = duckdb.Error
        self._lock = threading.Lock()
        self._table = f'"{schema}"."{table}"'
        self._has_hashes = True
        try:
            if read_only:  # dbt-hodor explain: never create the file, the schema or the table
                self._con = duckdb.connect(path, read_only=True)
                self._has_hashes = self._columns_include_hashes(schema, table)
                return
            if path != ":memory:":
                Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._con = duckdb.connect(path)
            self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            self._con.execute(create_table_sql(self._table, "duckdb"))
            if not self._columns_include_hashes(schema, table):
                self._con.execute(add_column_sql(self._table, "duckdb"))
        except (OSError, duckdb.Error) as exc:
            raise StoreError(f"cannot open hodor state file {path!r}: {exc}") from exc

    def _columns_include_hashes(self, schema: str, table: str) -> bool:
        rows = self._con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchall()
        # no rows: the table is missing, and the read that follows reports it
        return not rows or any(row[0] == "node_hashes" for row in rows)

    def load(
        self, project_name: str, target_name: str, since_ms: int, before_ms: int | None = None
    ) -> dict[str, Record]:
        sql = latest_builds_sql(
            self._table, "?", "?", "?", None if before_ms is None else "?", self._has_hashes
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
        sql = latest_records_sql(self._table, "?", "?", "?", self._has_hashes, any_status, reused)
        records = self._read(sql, [project_name, target_name, since_ms])
        return {record.node_key: record for record in records}

    def invocation(
        self, project_name: str, target_name: str, invocation_id: str | None
    ) -> list[Record]:
        if invocation_id is None:
            sql = invocation_sql(self._table, "?", "?", None, self._has_hashes)
            return self._read(sql, [project_name, target_name] * 2)
        sql = invocation_sql(self._table, "?", "?", "?", self._has_hashes)
        return self._read(sql, [project_name, target_name, invocation_id])

    def _read(self, sql: str, params: list[object]) -> list[Record]:
        try:
            with self._lock:
                rows = self._con.execute(sql, params).fetchall()
        except self._error as exc:
            raise StoreError(f"cannot read hodor state: {exc}") from exc
        return [Record.from_row(row) for row in rows]

    def append(self, records: Sequence[Record]) -> None:
        if not records:
            return
        placeholders = ", ".join("?" for _ in COLUMN_NAMES)
        sql = f"INSERT INTO {self._table} ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders})"
        try:
            with self._lock:
                self._con.executemany(sql, [record.to_row() for record in records])
        except self._error as exc:
            raise StoreError(f"cannot write hodor state: {exc}") from exc

    def prune(self, project_name: str, target_name: str, before_ms: int, delete: bool) -> int:
        """Always counts first, so a dry run and the real run report the same number."""
        params = [project_name, target_name, before_ms]
        try:
            with self._lock:
                count_sql = prune_sql(self._table, "?", "?", "?", delete=False)
                row = self._con.execute(count_sql, params).fetchone()
                count = int(row[0]) if row else 0
                if delete and count:
                    self._con.execute(prune_sql(self._table, "?", "?", "?", delete=True), params)
                return count
        except self._error as exc:
            raise StoreError(f"cannot prune hodor state: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            self._con.close()

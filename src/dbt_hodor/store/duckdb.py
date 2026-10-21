"""Local state in one DuckDB file."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from pathlib import Path

from dbt_hodor.config import IDENT_RE
from dbt_hodor.records import (
    COLUMN_NAMES,
    Record,
    create_table_sql,
    invocation_sql,
    latest_builds_sql,
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
        try:
            if read_only:  # dbt-hodor explain: never create the file, the schema or the table
                self._con = duckdb.connect(path, read_only=True)
                return
            if path != ":memory:":
                Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._con = duckdb.connect(path)
            self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            self._con.execute(create_table_sql(self._table, "duckdb"))
        except (OSError, duckdb.Error) as exc:
            raise StoreError(f"cannot open hodor state file {path!r}: {exc}") from exc

    def load(
        self, project_name: str, target_name: str, since_ms: int, before_ms: int | None = None
    ) -> dict[str, Record]:
        sql = latest_builds_sql(self._table, "?", "?", "?", None if before_ms is None else "?")
        params: list[object] = [project_name, target_name, since_ms]
        records = self._read(sql, params if before_ms is None else [*params, before_ms])
        return {record.node_key: record for record in records}

    def invocation(
        self, project_name: str, target_name: str, invocation_id: str | None
    ) -> list[Record]:
        if invocation_id is None:
            sql = invocation_sql(self._table, "?", "?", None)
            return self._read(sql, [project_name, target_name] * 2)
        sql = invocation_sql(self._table, "?", "?", "?")
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
                count = int(self._con.execute(count_sql, params).fetchone()[0])
                if delete and count:
                    self._con.execute(prune_sql(self._table, "?", "?", "?", delete=True), params)
                return count
        except self._error as exc:
            raise StoreError(f"cannot prune hodor state: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            self._con.close()

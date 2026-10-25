"""State in a Unity Catalog Delta table, through dbt's own adapter connection."""

from __future__ import annotations

from collections.abc import Callable, Sequence

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

# Runs one SQL statement and returns its rows as sequences in column order.
Execute = Callable[[str], Sequence[Sequence[object]]]


def quote_table(*parts: str, quote: str = "`") -> str:
    """`catalog`.`schema`.`table`; every part is validated, so quoting cannot be broken."""
    for part in parts:
        if not IDENT_RE.match(part):
            raise StoreError(f"invalid hodor name part {part!r}")
    return ".".join(f"{quote}{part}{quote}" for part in parts)


def sql_literal(value: object) -> str:
    """Databricks SQL literal. The store builds literal SQL because dbt's execute has no binds."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    raise TypeError(f"unsupported literal type {type(value).__name__}")


def trino_literal(value: object) -> str:
    """Trino SQL literal: `'` is doubled and nothing else is escaped, so a backslash or a
    newline passes through verbatim."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return sql_literal(value)


class DeltaStore:
    # The Trino store changes these three: the identifier quote, the literal and the DDL dialect.
    _quote = "`"
    _dialect = "delta"

    @staticmethod
    def _literal(value: object) -> str:
        return sql_literal(value)

    def __init__(
        self,
        database: str,
        schema: str,
        table: str,
        execute: Execute,
        batch_size: int = 500,
        read_only: bool = False,
    ) -> None:
        self._table = quote_table(database, schema, table, quote=self._quote)
        self._execute = execute
        self._batch_size = batch_size
        if read_only:  # dbt-hodor explain: never create the table
            return
        try:
            # Job principals often hold MODIFY but not CREATE TABLE, so only create when missing.
            self._execute(f"SELECT * FROM {self._table} LIMIT 0")
            return
        except Exception:  # noqa: BLE001 - most likely the table does not exist yet
            pass
        try:
            # The schema is not created: it needs catalog-level grants, so it must already exist.
            self._execute(create_table_sql(self._table, self._dialect))
        except Exception as exc:  # noqa: BLE001 - any adapter error means fail open
            raise StoreError(
                f"cannot create hodor state table {self._table} (needs CREATE TABLE on the "
                f"schema; or create it once manually with `dbt-hodor ddl`): {exc}"
            ) from exc

    def load(
        self, project_name: str, target_name: str, since_ms: int, before_ms: int | None = None
    ) -> dict[str, Record]:
        sql = latest_builds_sql(
            self._table,
            self._literal(project_name),
            self._literal(target_name),
            self._literal(since_ms),
            None if before_ms is None else self._literal(before_ms),
        )
        return {record.node_key: record for record in self._read(sql)}

    def invocation(
        self, project_name: str, target_name: str, invocation_id: str | None
    ) -> list[Record]:
        invocation = None if invocation_id is None else self._literal(invocation_id)
        sql = invocation_sql(
            self._table, self._literal(project_name), self._literal(target_name), invocation
        )
        return self._read(sql)

    def _read(self, sql: str) -> list[Record]:
        try:
            rows = self._execute(sql)
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot read hodor state: {exc}") from exc
        return [Record.from_row(tuple(row)) for row in rows]

    def append(self, records: Sequence[Record]) -> None:
        head = f"INSERT INTO {self._table} ({', '.join(COLUMN_NAMES)}) VALUES\n"
        for start in range(0, len(records), self._batch_size):
            batch = records[start : start + self._batch_size]
            values = ",\n".join(
                "(" + ", ".join(self._literal(v) for v in record.to_row()) + ")" for record in batch
            )
            try:
                self._execute(head + values)
            except Exception as exc:  # noqa: BLE001
                raise StoreError(f"cannot write hodor state: {exc}") from exc

    def prune(self, project_name: str, target_name: str, before_ms: int, delete: bool) -> int:
        """Always counts first, so a dry run and the real run report the same number.

        VACUUM is left to the operator: it has its own retention rules.
        """
        args = (self._literal(project_name), self._literal(target_name), self._literal(before_ms))
        try:
            rows = self._execute(prune_sql(self._table, *args, delete=False))
            count = int(rows[0][0]) if rows else 0
            if delete and count:
                self._execute(prune_sql(self._table, *args, delete=True))
            return count
        except Exception as exc:  # noqa: BLE001
            raise StoreError(f"cannot prune hodor state: {exc}") from exc

    def close(self) -> None:
        pass  # the connection belongs to dbt

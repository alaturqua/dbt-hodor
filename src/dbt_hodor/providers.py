"""Warehouse access for hodor's own queries: data versions and the clock."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from dbt_hodor.metadata import (
    UNKNOWN,
    DataVersion,
    _epoch_ms,
    delta_data_version,
    duckdb_fingerprint_sql,
    history_entry_from_row,
)

HISTORY_LIMIT = 20
# A view stores no data: its version is a hash of its definition in the warehouse.
VIEW_PREFIX = "view:"


@contextmanager
def thread_connection(adapter: Any) -> Iterator[None]:
    """Use the thread's open dbt connection, or open one named 'hodor' (main thread)."""
    if adapter.connections.get_if_exists() is not None:
        yield
    else:
        with adapter.connection_named("hodor"):
            yield


def fetch(adapter: Any, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Run SQL on the raw cursor: adapter.execute(fetch=True) infers agate types ('7' -> 7)."""
    with thread_connection(adapter):
        _, cursor = adapter.connections.add_query(sql, auto_begin=False)
        names = [d[0] for d in cursor.description or []]
        rows = [tuple(row) for row in cursor.fetchall()] if cursor.description else []
    return names, rows


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _definition(rows: Sequence[Sequence[Any]]) -> DataVersion:
    if not rows or rows[0][0] is None:
        raise LookupError("no view definition")
    return DataVersion(hashlib.sha256(str(rows[0][0]).encode("utf-8")).hexdigest())


def _quote(relation: str, quote: str) -> str:
    return ".".join(quote + part.replace(quote, quote * 2) + quote for part in relation.split("."))


class Provider:
    """Data version per relation, cached for the run. Any query failure gives UNKNOWN."""

    clock_sql = ""
    volatile_sql = ""  # current date first, then the session values

    def __init__(self, adapter: Any, debug: Callable[[str], None] = lambda _: None) -> None:
        self._adapter = adapter
        self._debug = debug
        self._cache: dict[str, DataVersion] = {}
        self._lock = threading.Lock()

    def _query(self, key: str) -> DataVersion:
        raise NotImplementedError

    def _safe(self, key: str) -> DataVersion:
        try:
            version = self._query(key)
        except Exception as exc:  # noqa: BLE001 - unknown version means the node builds
            self._debug(f"dbt-hodor: no data version for {key}: {exc}")
            version = UNKNOWN
        with self._lock:
            self._cache[key] = version
        return version

    def version(self, relation: str) -> DataVersion:
        with self._lock:
            cached = self._cache.get(relation)
        return cached if cached is not None else self._safe(relation)

    def refresh(self, relation: str) -> DataVersion:
        """Re-read after hodor built the relation, so downstream nodes see the new version."""
        return self._safe(relation)

    def prefetch(self, keys: Sequence[str], threads: int) -> None:
        """Read many versions in parallel; each worker keeps one connection for its share."""
        with self._lock:
            todo = [key for key in dict.fromkeys(keys) if key not in self._cache]
        if not todo:
            return
        workers = max(1, min(threads, len(todo)))

        def work(share: list[str]) -> None:
            with self._adapter.connection_named("hodor_metadata"):
                for key in share:
                    self._safe(key)

        with ThreadPoolExecutor(workers, thread_name_prefix="hodor") as pool:
            list(pool.map(work, [todo[i::workers] for i in range(workers)]))

    def path_version(self, path: str) -> DataVersion:
        return UNKNOWN

    def now_ms(self) -> int:
        _, rows = fetch(self._adapter, self.clock_sql)
        return int(rows[0][0])

    def volatile_values(self) -> dict[str, str]:
        """The values behind the `date` and `session` volatile classes, from one query."""
        _, rows = fetch(self._adapter, self.volatile_sql)
        today, *session = rows[0]
        return {"date": str(today), "session": "|".join(str(v) for v in session)}


class DuckDBProvider(Provider):
    """Content fingerprint: DuckDB has no table history."""

    clock_sql = "select epoch_ms(current_timestamp)"
    volatile_sql = (
        "select cast(current_date as varchar), current_user, current_database(),"
        " current_schema(), version()"
    )

    def _query(self, key: str) -> DataVersion:
        if key.startswith(VIEW_PREFIX):
            database, schema, name = key[len(VIEW_PREFIX) :].split(".")
            _, rows = fetch(
                self._adapter,
                "select sql from duckdb_views() where lower(database_name) = "
                f"{_literal(database)} and lower(schema_name) = {_literal(schema)} "
                f"and lower(view_name) = {_literal(name)}",
            )
            return _definition(rows)
        _, rows = fetch(self._adapter, duckdb_fingerprint_sql(_quote(key, '"')))
        return DataVersion(str(rows[0][0]))


class DeltaProvider(Provider):
    """Version of the latest data-change commit in the Delta history."""

    clock_sql = "select unix_millis(current_timestamp())"
    volatile_sql = (
        "select cast(current_date() as string), current_user(), current_catalog(),"
        " current_schema(), current_timezone(), to_json(current_version())"
    )

    def _history(self, target: str) -> DataVersion:
        names, rows = fetch(self._adapter, f"DESCRIBE HISTORY {target} LIMIT {HISTORY_LIMIT}")
        return delta_data_version(
            history_entry_from_row(dict(zip(names, row, strict=True))) for row in rows
        )

    def _query(self, key: str) -> DataVersion:
        if key.startswith(VIEW_PREFIX):
            catalog, schema, name = key[len(VIEW_PREFIX) :].split(".")
            _, rows = fetch(
                self._adapter,
                f"select view_definition from {_quote(catalog, '`')}.information_schema.views "
                f"where lower(table_schema) = {_literal(schema)} "
                f"and lower(table_name) = {_literal(name)}",
            )
            return _definition(rows)
        if key.startswith("delta:"):
            return self._history("delta." + _quote(key[len("delta:") :], "`"))
        return self._history(_quote(key, "`"))

    def path_version(self, path: str) -> DataVersion:
        return self.version(f"delta:{path}")


def _history_ms(value: Any) -> int:
    """Epoch milliseconds from a driver value: a datetime (naive means UTC) or a number."""
    if isinstance(value, bool):
        raise LookupError(f"unreadable history time {value!r}")
    if isinstance(value, datetime):
        ms = _epoch_ms(value)
        if ms is not None:
            return ms
    if isinstance(value, int | float):
        return int(value)
    raise LookupError(f"unreadable history time {value!r}")


class TrinoProvider(Provider):
    """Iceberg and Delta Lake connectors, through their hidden "$history" tables.

    Other connectors have no such table: the query fails and the node builds.
    """

    clock_sql = "select cast(to_unixtime(current_timestamp) * 1000 as bigint)"
    volatile_sql = (
        "select cast(current_date as varchar), current_user, current_catalog,"
        " current_schema, current_timezone(), version()"
    )

    @staticmethod
    def _system_table(key: str, suffix: str) -> str:
        *head, name = key.split(".")
        return _quote(".".join(head), '"') + '."' + name.replace('"', '""') + suffix + '"'

    def _iceberg(self, key: str, records: list[dict[str, Any]]) -> DataVersion:
        # A rollback adds no row: it clears is_current_ancestor on the rolled-back snapshots.
        ancestors = [r for r in records if r["is_current_ancestor"] is True]
        if not ancestors:
            raise LookupError("no current snapshot in the history")
        latest = max(_history_ms(r["made_current_at"]) for r in ancestors)
        current = {
            r["snapshot_id"] for r in ancestors if _history_ms(r["made_current_at"]) == latest
        }
        if len(current) != 1:
            raise LookupError("several current snapshots with the same time")
        # A schema change adds no snapshot but writes a new metadata file, which the log lists.
        names, rows = fetch(
            self._adapter, f"select * from {self._system_table(key, '$metadata_log_entries')}"
        )
        entries = [dict(zip(names, row, strict=True)) for row in rows]
        if not entries:
            raise LookupError("empty metadata log")
        logged = max(_history_ms(e["timestamp"]) for e in entries)
        files = {str(e["file"]) for e in entries if _history_ms(e["timestamp"]) == logged}
        if len(files) != 1:
            raise LookupError("several metadata files with the same time")
        file_id = hashlib.sha256(files.pop().encode("utf-8")).hexdigest()[:12]
        return DataVersion(f"{current.pop()}@{latest}+{logged}:{file_id}", max(latest, logged))

    def _history(self, key: str) -> DataVersion:
        names, rows = fetch(self._adapter, f"select * from {self._system_table(key, '$history')}")
        if not rows:
            raise LookupError("empty history")
        records = [dict(zip(names, row, strict=True)) for row in rows]
        if {"made_current_at", "snapshot_id", "is_current_ancestor"} <= set(names):
            return self._iceberg(key, records)
        if {"version", "timestamp", "operation"} <= set(names):
            return delta_data_version(history_entry_from_row(r) for r in records)
        raise LookupError(f"unknown history columns {names}")

    def _query(self, key: str) -> DataVersion:
        if key.startswith(VIEW_PREFIX):
            catalog, schema, name = key[len(VIEW_PREFIX) :].split(".")
            quoted = _quote(catalog, '"')
            _, rows = fetch(
                self._adapter,
                f"select view_definition from {quoted}.information_schema.views "
                f"where lower(table_schema) = {_literal(schema)} "
                f"and lower(table_name) = {_literal(name)}",
            )
            return _definition(rows)
        return self._history(key)


def store_executor(adapter: Any) -> Callable[[str], Sequence[Sequence[object]]]:
    """The Delta store's way to run SQL: dbt's own connection, raw rows."""

    def execute(sql: str) -> Sequence[Sequence[object]]:
        return fetch(adapter, sql)[1]

    return execute

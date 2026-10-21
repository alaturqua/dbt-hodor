"""Data versions: opaque tokens that change when a table's data changes."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class DataVersion:
    """Compared for equality only, never order: a recreated Delta table restarts at 0."""

    token: str | None
    changed_at_ms: int | None = None

    @property
    def known(self) -> bool:
        return self.token is not None


UNKNOWN = DataVersion(None)

# Delta operations that never change the data a table holds.
NON_DATA_OPERATIONS = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
        "ANALYZE",
        "COMPUTE STATS",
        "SET TBLPROPERTIES",
        "UNSET TBLPROPERTIES",
        "CHANGE COLUMN",
        "ADD CONSTRAINT",
        "DROP CONSTRAINT",
        "UPGRADE PROTOCOL",
        "SET TAGS",
        "UNSET TAGS",
        "CLUSTER BY",
        "REORG",
        "FSCK",
    }
)
ROW_METRICS = (
    "numOutputRows",
    "numTargetRowsInserted",
    "numTargetRowsUpdated",
    "numTargetRowsDeleted",
    "numDeletedRows",
    "numUpdatedRows",
    "numCopiedRows",
)


@dataclass(frozen=True)
class HistoryEntry:
    version: int
    timestamp_ms: int | None
    operation: str
    metrics: Mapping[str, object]


def _metrics(raw: object) -> Mapping[str, object]:
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    try:
        return dict(raw)  # type: ignore[call-overload]  # list of (key, value) pairs
    except (TypeError, ValueError):
        return {}


def _epoch_ms(value: object) -> int | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)  # Delta history times are UTC
        return int(value.timestamp() * 1000)
    return None


def history_entry_from_row(row: Mapping[str, object]) -> HistoryEntry:
    """One `DESCRIBE HISTORY` row, with operationMetrics as a dict, JSON text or pairs."""
    return HistoryEntry(
        version=int(row["version"]),  # type: ignore[call-overload]
        timestamp_ms=_epoch_ms(row.get("timestamp")),
        operation=str(row.get("operation") or ""),
        metrics=_metrics(row.get("operationMetrics")),
    )


def is_data_change(entry: HistoryEntry) -> bool:
    """Unknown operations and unreadable metrics count as changes: extra builds, no wrong reuse."""
    if entry.operation.upper() in NON_DATA_OPERATIONS:
        return False
    present = [entry.metrics[key] for key in ROW_METRICS if key in entry.metrics]
    if not present:
        return True
    for value in present:
        try:
            if int(str(value)) != 0:
                return True
        except ValueError:
            return True
    return False


def delta_data_version(history: Iterable[HistoryEntry]) -> DataVersion:
    """Token of the latest data-change commit; UNKNOWN if the window holds none.

    The token holds the commit time as well as the version: a table that is dropped and
    recreated, or swapped in from a staging table (use_safer_relation_operations), restarts
    its versions, so the version alone can repeat after a real change.
    """
    latest = max((e for e in history if is_data_change(e)), key=lambda e: e.version, default=None)
    if latest is None:
        return UNKNOWN
    return DataVersion(f"{latest.version}@{latest.timestamp_ms}", latest.timestamp_ms)


def duckdb_fingerprint_sql(relation: str) -> str:
    """One query per table: row count plus an order-independent sum of row hashes.

    relation must already be a quoted, trusted relation name from the manifest.
    """
    return (
        "select count(*)::varchar || ':' || coalesce(sum(hash(t))::varchar, '0') "
        f"from {relation} as t"
    )

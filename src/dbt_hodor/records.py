"""The append-only `hodor_state` record and its portable SQL."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

# (name, kind) in table order. kind is "text" or "bigint"; JSON is stored as text.
COLUMNS: tuple[tuple[str, str], ...] = (
    ("record_id", "text"),
    ("invocation_id", "text"),
    ("recorded_at_ms", "bigint"),
    ("project_name", "text"),
    ("target_name", "text"),
    ("node_key", "text"),
    ("unique_id", "text"),
    ("resource_type", "text"),
    ("mode", "text"),
    ("decision", "text"),
    ("reason", "text"),
    ("action", "text"),
    ("status", "text"),
    ("text_hash", "text"),
    ("code_hash", "text"),
    ("unrendered_hash", "text"),
    ("config_hash", "text"),
    ("hash_version", "text"),
    ("sql_facts", "text"),
    ("data_version", "text"),
    ("data_changed_at_ms", "bigint"),
    ("built_at_ms", "bigint"),
    ("upstreams", "text"),
    ("test_result", "text"),
    ("hodor_version", "text"),
)
COLUMN_NAMES = tuple(name for name, _ in COLUMNS)
TYPE_NAMES = {
    "delta": {"text": "STRING", "bigint": "BIGINT"},
    "duckdb": {"text": "VARCHAR", "bigint": "BIGINT"},
    "postgres": {"text": "TEXT", "bigint": "BIGINT"},
}
SUCCESS_STATUSES = ("success", "pass", "warn", "fail")
_JSON_FIELDS = ("sql_facts", "upstreams", "test_result")
_MS_FIELDS = ("recorded_at_ms", "data_changed_at_ms", "built_at_ms")


def _json(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Record:
    record_id: str
    invocation_id: str
    recorded_at_ms: int
    project_name: str
    target_name: str
    node_key: str
    unique_id: str
    resource_type: str
    mode: str
    decision: str
    reason: str
    action: str
    status: str
    text_hash: str | None = None
    code_hash: str | None = None
    unrendered_hash: str | None = None
    config_hash: str | None = None
    hash_version: str | None = None
    sql_facts: Mapping[str, object] | None = None
    data_version: str | None = None
    data_changed_at_ms: int | None = None
    built_at_ms: int | None = None
    upstreams: Mapping[str, str | None] = field(default_factory=dict)  # {table: data_version}
    test_result: Mapping[str, object] | None = None
    hodor_version: str | None = None

    def to_row(self) -> tuple[object, ...]:
        """Values in COLUMNS order, JSON fields serialized with sorted keys."""
        return tuple(
            _json(getattr(self, name)) if name in _JSON_FIELDS else getattr(self, name)
            for name in COLUMN_NAMES
        )

    @classmethod
    def from_row(cls, row: Sequence[object]) -> Record:
        values = dict(zip(COLUMN_NAMES, row, strict=True))
        for name in _JSON_FIELDS:
            text = values[name]
            values[name] = json.loads(text) if isinstance(text, str) and text else None
        values["upstreams"] = values["upstreams"] or {}
        for name in _MS_FIELDS:
            if values[name] is not None:
                values[name] = int(values[name])  # type: ignore[call-overload]
        return cls(**values)  # type: ignore[arg-type]


def create_table_sql(table: str, dialect: str) -> str:
    """Portable DDL. Delta clusters by the run-start lookup keys."""
    types = TYPE_NAMES[dialect]
    columns = ",\n  ".join(f"{name} {types[kind]}" for name, kind in COLUMNS)
    cluster = " CLUSTER BY (target_name, node_key)" if dialect == "delta" else ""
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {columns}\n){cluster}"


def latest_builds_sql(
    table: str, project: str, target: str, since: str, before: str | None = None
) -> str:
    """Latest successful build per node_key. ROW_NUMBER, not QUALIFY: Postgres has none.

    project, target, since and before are placeholders or literals the store has already made
    safe.
    """
    cols = ", ".join(COLUMN_NAMES)
    statuses = ", ".join(f"'{s}'" for s in SUCCESS_STATUSES)
    until = f" AND recorded_at_ms < {before}" if before is not None else ""
    return (
        f"SELECT {cols} FROM (SELECT {cols}, ROW_NUMBER() OVER ("
        "PARTITION BY node_key ORDER BY recorded_at_ms DESC, record_id DESC) AS hodor_rn "
        f"FROM {table} WHERE project_name = {project} AND target_name = {target} "
        f"AND recorded_at_ms >= {since}{until} AND action = 'build' AND status IN ({statuses})"
        ") latest WHERE hodor_rn = 1"
    )


def prune_sql(table: str, project: str, target: str, before: str, delete: bool) -> str:
    """Count or delete the records of one project and target older than `before`.

    project, target and before are placeholders or literals the store has already made safe.
    """
    head = f"DELETE FROM {table}" if delete else f"SELECT count(*) FROM {table}"
    return (
        f"{head} WHERE project_name = {project} AND target_name = {target} "
        f"AND recorded_at_ms < {before}"
    )


def invocation_sql(table: str, project: str, target: str, invocation: str | None) -> str:
    """All records of one invocation in record order; the latest invocation when None.

    project, target and invocation are placeholders or literals the store has already made safe.
    """
    where = f"project_name = {project} AND target_name = {target}"
    if invocation is None:
        invocation = (
            f"(SELECT invocation_id FROM {table} WHERE {where} "
            "ORDER BY recorded_at_ms DESC, record_id DESC LIMIT 1)"
        )
    return (
        f"SELECT {', '.join(COLUMN_NAMES)} FROM {table} WHERE {where} "
        f"AND invocation_id = {invocation} ORDER BY recorded_at_ms, record_id"
    )

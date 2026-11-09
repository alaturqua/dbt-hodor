"""The append-only `hodor_state` record and its portable SQL."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
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
    ("node_hashes", "text"),
)
COLUMN_NAMES = tuple(name for name, _ in COLUMNS)
TYPE_NAMES = {
    "delta": {"text": "STRING", "bigint": "BIGINT"},
    "duckdb": {"text": "VARCHAR", "bigint": "BIGINT"},
    "postgres": {"text": "TEXT", "bigint": "BIGINT"},
    "trino": {"text": "varchar", "bigint": "bigint"},
}
SUCCESS_STATUSES = ("success", "pass", "warn", "fail")
# status of a model's reuse record (dbt-core's RunStatus.Reused); state selection counts it
REUSED = "reused"
# action of a copy of the last build, written when a long-reused node nears the lookback limit
CARRY = "carry"
_JSON_FIELDS = ("sql_facts", "upstreams", "test_result", "node_hashes")
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
    # engine ("v1"/"v2"), exact relation name, and the state: selector hashes
    node_hashes: Mapping[str, object] | None = None

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


def newer(current: Record | None, record: Record) -> bool:
    """Whether `record` replaces `current` as the newest: later, or the same time and a
    record id that sorts after it."""
    return current is None or (current.recorded_at_ms, current.record_id) <= (
        record.recorded_at_ms,
        record.record_id,
    )


def by_unique_id(records: Iterable[Record]) -> dict[str, Record]:
    """The newest record per unique_id. Records are keyed by relation, which can change."""
    out: dict[str, Record] = {}
    for record in records:
        if newer(out.get(record.unique_id), record):
            out[record.unique_id] = record
    return out


def create_table_sql(table: str, dialect: str) -> str:
    """Portable DDL. Delta clusters by the run-start lookup keys."""
    types = TYPE_NAMES[dialect]
    columns = ",\n  ".join(f"{name} {types[kind]}" for name, kind in COLUMNS)
    cluster = " CLUSTER BY (target_name, node_key)" if dialect == "delta" else ""
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {columns}\n){cluster}"


def add_column_sql(table: str, dialect: str) -> str:
    """Add `node_hashes` to a table made before that column existed."""
    return f"ALTER TABLE {table} ADD COLUMN node_hashes {TYPE_NAMES[dialect]['text']}"


POSTGRES_NAME_LIMIT = 63  # bytes; Postgres cuts longer identifiers without a warning
_LOOKUP_SUFFIX = "_lookup"


def lookup_index_name(table_name: str) -> str:
    """Name of the Postgres lookup index, cut so a long table name still yields a name that
    fits the identifier limit and can be found by an exact match."""
    return table_name[: POSTGRES_NAME_LIMIT - len(_LOOKUP_SUFFIX)] + _LOOKUP_SUFFIX


def lookup_index_sql(schema: str, table_name: str) -> str:
    """Index on the columns every read filters by. Names are already validated."""
    return (
        f'CREATE INDEX IF NOT EXISTS "{lookup_index_name(table_name)}" '
        f'ON "{schema}"."{table_name}" (project_name, target_name, recorded_at_ms)'
    )


def select_columns(has_hashes: bool = True) -> str:
    """The column list of a read. A table without `node_hashes` (read-only open of an old
    table) yields NULL for it."""
    return ", ".join(
        name if has_hashes or name != "node_hashes" else "NULL AS node_hashes"
        for name in COLUMN_NAMES
    )


def _latest_sql(table: str, where: str, has_hashes: bool) -> str:
    cols = select_columns(has_hashes)
    return (
        f"SELECT {', '.join(COLUMN_NAMES)} FROM (SELECT {cols}, ROW_NUMBER() OVER ("
        "PARTITION BY node_key ORDER BY recorded_at_ms DESC, record_id DESC) AS hodor_rn "
        f"FROM {table} WHERE {where}"
        ") latest WHERE hodor_rn = 1"
    )


def latest_builds_sql(
    table: str,
    project: str,
    target: str,
    since: str,
    before: str | None = None,
    has_hashes: bool = True,
) -> str:
    """Latest successful build (or carried copy of one) per node_key. ROW_NUMBER, not
    QUALIFY: Postgres has none.

    project, target, since and before are placeholders or literals the store has already made
    safe.
    """
    statuses = ", ".join(f"'{s}'" for s in SUCCESS_STATUSES)
    until = f" AND recorded_at_ms < {before}" if before is not None else ""
    where = (
        f"project_name = {project} AND target_name = {target} "
        f"AND recorded_at_ms >= {since}{until} AND action IN ('build', '{CARRY}') "
        f"AND status IN ({statuses})"
    )
    return _latest_sql(table, where, has_hashes)


def latest_records_sql(
    table: str,
    project: str,
    target: str,
    since: str,
    has_hashes: bool = True,
    any_status: bool = False,
    reused: bool = False,
) -> str:
    """Latest successful record of any action (build, carry, clone, a test's reuse) per
    node_key; of any status too when `any_status`. `reused` adds the reuse records of models
    (status "reused"), which state selection compares with; defer and clone leave them out.

    project, target and since are placeholders or literals the store has already made safe.
    """
    statuses = ", ".join(f"'{s}'" for s in (*SUCCESS_STATUSES, *((REUSED,) if reused else ())))
    where = f"project_name = {project} AND target_name = {target} AND recorded_at_ms >= {since}" + (
        "" if any_status else f" AND status IN ({statuses})"
    )
    return _latest_sql(table, where, has_hashes)


def prune_sql(table: str, project: str, target: str, before: str, delete: bool) -> str:
    """Count or delete the records of one project and target older than `before`.

    project, target and before are placeholders or literals the store has already made safe.
    """
    head = f"DELETE FROM {table}" if delete else f"SELECT count(*) FROM {table}"
    return (
        f"{head} WHERE project_name = {project} AND target_name = {target} "
        f"AND recorded_at_ms < {before}"
    )


def invocation_sql(
    table: str, project: str, target: str, invocation: str | None, has_hashes: bool = True
) -> str:
    """All records of one invocation in record order; the latest invocation when None.

    project, target and invocation are placeholders or literals the store has already made safe.
    """
    where = f"project_name = {project} AND target_name = {target} AND action <> '{CARRY}'"
    if invocation is None:
        invocation = (
            f"(SELECT invocation_id FROM {table} WHERE {where} "
            "ORDER BY recorded_at_ms DESC, record_id DESC LIMIT 1)"
        )
    return (
        f"SELECT {select_columns(has_hashes)} FROM {table} WHERE {where} "
        f"AND invocation_id = {invocation} ORDER BY recorded_at_ms, record_id"
    )

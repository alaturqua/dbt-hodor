"""Stable hashes of a node's logic and config, and the SQL facts lineage needs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify

HASH_VERSION = "2"
INVOCATION_PLACEHOLDER = "__hodor_invocation_id__"
RUN_STARTED_PLACEHOLDER = "__hodor_run_started_at__"

# Config keys that do not change the data a node produces.
IGNORED_CONFIG_KEYS = frozenset(
    {
        "databricks_compute",
        "incremental_apply_config_changes",
        "tags",
        "meta",
        "docs",
        "state",
        "enabled",
        "persist_docs",
    }
)
# Column keys hashed, only when set, besides data_type and constraints: dbt-databricks
# column properties, which dbt keeps as extra column keys.
_OPTIONAL_COLUMN_KEYS = ("column_mask", "databricks_tags")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def normalize_runtime_values(
    sql: str, invocation_id: str | None = None, run_started_at: str | None = None
) -> str:
    """Replace values that change on every run with fixed placeholders."""
    if invocation_id:
        sql = sql.replace(invocation_id, INVOCATION_PLACEHOLDER)
    if run_started_at:
        sql = sql.replace(run_started_at, RUN_STARTED_PLACEHOLDER)
    return sql


def text_hash(normalized_sql: str) -> str:
    """Fast-path key. Includes HASH_VERSION so an algorithm change invalidates stored facts."""
    return _sha(f"{HASH_VERSION}\0{normalized_sql}")


@dataclass(frozen=True)
class SqlAnalysis:
    """What one sqlglot parse tells us about a node's compiled SQL."""

    code_hash: str
    select_star: bool
    tables: frozenset[str] = frozenset()  # lower-case catalog.schema.table, CTEs excluded
    delta_paths: frozenset[str] = frozenset()  # delta.`<path>` references, case kept
    opaque_inputs: bool = False  # table functions, partial names or unparsable SQL
    volatile: frozenset[str] = frozenset()  # VOLATILE_CLASSES that decide rows or values
    volatile_stamps: frozenset[str] = frozenset()  # VOLATILE_CLASSES only in stamp columns

    def to_json(self) -> dict[str, object]:
        return {
            "code_hash": self.code_hash,
            "select_star": self.select_star,
            "tables": sorted(self.tables),
            "delta_paths": sorted(self.delta_paths),
            "opaque_inputs": self.opaque_inputs,
            "volatile": sorted(self.volatile),
            "volatile_stamps": sorted(self.volatile_stamps),
        }

    @classmethod
    def from_json(cls, data: object) -> SqlAnalysis | None:
        """None for anything malformed, so the caller re-analyzes."""
        if not isinstance(data, Mapping):
            return None
        try:
            code, star, tables, paths, opaque, volatile, stamps = (
                data["code_hash"],
                data["select_star"],
                data["tables"],
                data["delta_paths"],
                data["opaque_inputs"],
                data["volatile"],
                data["volatile_stamps"],
            )
        except KeyError:
            return None
        lists = (tables, paths, volatile, stamps)
        if not (
            isinstance(code, str)
            and isinstance(star, bool)
            and isinstance(opaque, bool)
            and all(isinstance(x, list) for x in lists)
            and all(isinstance(t, str) for x in lists for t in x)
        ):
            return None
        return cls(
            code,
            star,
            frozenset(tables),
            frozenset(paths),
            opaque,
            frozenset(volatile),
            frozenset(stamps),
        )


# Volatile SQL functions, by the value that decides their result.
# always: changes on every call. date: the warehouse's current date.
# session: who runs the query and where (user, groups, catalog, schema, time zone, version).
VOLATILE_CLASSES = ("always", "date", "session")
_VOLATILE_TYPES: dict[str, tuple[type[exp.Expr], ...]] = {
    cls: tuple(t for t in (getattr(exp, name, None) for name in names) if isinstance(t, type))
    for cls, names in {
        "always": (
            "CurrentTimestamp",
            "CurrentTime",
            "CurrentDatetime",
            "Localtimestamp",
            "Rand",
            "Randn",
            "Uuid",
            "Shuffle",
        ),
        "date": ("CurrentDate",),
        "session": (
            "CurrentUser",
            "SessionUser",
            "CurrentCatalog",
            "CurrentDatabase",
            "CurrentSchema",
            "CurrentTimezone",
            "CurrentVersion",
        ),
    }.items()
}
# Functions sqlglot keeps as Anonymous in one of the two dialects.
_VOLATILE_NAMES = {
    "now": "always",
    "getdate": "always",
    "sysdate": "always",
    "localtimestamp": "always",
    "unix_timestamp": "always",  # only without arguments, see _volatile_classes
    "monotonically_increasing_id": "always",
    "random_uuid": "always",
    "uuid": "always",
    "today": "date",
    "curdate": "date",
    "user": "session",
    "is_member": "session",
    "is_account_group_member": "session",
    "current_metastore": "session",
}
_PLACEHOLDERS = (INVOCATION_PLACEHOLDER, RUN_STARTED_PLACEHOLDER)


def _volatile_class(node: exp.Expr) -> str | None:
    if isinstance(node, exp.StrToUnix):
        return "always" if node.this is None else None  # unix_timestamp() is now, (x) is not
    if isinstance(node, exp.Literal):
        # {{ invocation_id }} and {{ run_started_at }} change on every run, like now()
        return "always" if node.is_string and any(p in node.this for p in _PLACEHOLDERS) else None
    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        if name == "unix_timestamp" and node.expressions:
            return None
        return _VOLATILE_NAMES.get(name)
    for cls, types in _VOLATILE_TYPES.items():
        if isinstance(node, types):
            return cls
    return None


def _final_selects(query: exp.Expr) -> list[exp.Select]:
    if isinstance(query, exp.SetOperation):
        return _final_selects(query.this) + _final_selects(query.expression)
    if isinstance(query, exp.Subquery):
        return _final_selects(query.this)
    return [query] if isinstance(query, exp.Select) else []


def _stamp_ids(root: exp.Expr) -> set[int]:
    """Output columns of the final SELECT that read no column: same value on every row."""
    return {
        id(projection)
        for select in _final_selects(root)
        for projection in select.expressions
        if projection.find(exp.Column, exp.Star, exp.Select) is None
    }


def _volatile(root: exp.Expr) -> tuple[frozenset[str], frozenset[str]]:
    """(classes that decide rows or values, classes used only as stamps)."""
    stamps_at = _stamp_ids(root)
    logic: set[str] = set()
    stamps: set[str] = set()
    for node in root.walk():
        cls = _volatile_class(node)
        if cls is None:
            continue
        parent: exp.Expr | None = node
        while parent is not None and id(parent) not in stamps_at:
            parent = parent.parent
        (stamps if parent is not None else logic).add(cls)
    return frozenset(logic), frozenset(stamps)


def _text_fallback(sql: str) -> SqlAnalysis:
    volatile = frozenset({"always"}) if any(p in sql for p in _PLACEHOLDERS) else frozenset()
    return SqlAnalysis(
        f"text:{_sha(' '.join(sql.split()))}",
        select_star=True,
        opaque_inputs=True,
        volatile=volatile,
    )


def _projects_star(select: exp.Select) -> bool:
    return any(
        isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star))
        for p in select.expressions
    )


def _sources(select: exp.Select) -> list[exp.Expr]:
    sources = []
    from_ = select.args.get("from_")
    if from_ is not None:
        sources.append(from_.this)
    for join in select.args.get("joins") or []:
        sources.append(join.this)
    return sources


def _star_reaches_relation(
    query: exp.Expr, ctes: Mapping[str, exp.Expr], seen: frozenset[str]
) -> bool:
    if isinstance(query, exp.SetOperation):
        return _star_reaches_relation(query.this, ctes, seen) or _star_reaches_relation(
            query.expression, ctes, seen
        )
    if isinstance(query, exp.Subquery):
        return _star_reaches_relation(query.this, ctes, seen)
    if not isinstance(query, exp.Select):
        return True
    if not _projects_star(query):
        return False
    for source in _sources(query):
        if isinstance(source, exp.Subquery):
            if _star_reaches_relation(source.this, ctes, seen):
                return True
        elif (
            isinstance(source, exp.Table)
            and isinstance(source.this, exp.Identifier)
            and not source.db
            and source.name.lower() in ctes
        ):
            name = source.name.lower()
            if name not in seen and _star_reaches_relation(ctes[name], ctes, seen | {name}):
                return True
        else:
            return True
    return False


def _table_refs(root: exp.Expr, ctes: Mapping[str, exp.Expr]) -> tuple[set[str], set[str], bool]:
    tables: set[str] = set()
    paths: set[str] = set()
    opaque = False
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            opaque = True  # a table function such as read_files(...)
            continue
        catalog, db, name = table.catalog, table.db, table.name
        if not catalog and not db and name.lower() in ctes:
            continue
        if not catalog and db.lower() == "delta" and "/" in name:
            paths.add(name)
        elif catalog and db:
            tables.add(f"{catalog}.{db}.{name}".lower())
        else:
            opaque = True  # a partial name cannot be resolved safely
    return tables, paths, opaque


def _neutralize(root: exp.Expr, relations: Mapping[str, str]) -> None:
    """Replace each dbt relation with its node id, so the code hash does not depend on the
    catalog or schema a target writes to (a CI catalog, pr_<nr>_ schemas, --defer)."""
    for table in list(root.find_all(exp.Table)):
        if not isinstance(table.this, exp.Identifier) or not (table.catalog and table.db):
            continue
        node = relations.get(f"{table.catalog}.{table.db}.{table.name}".lower())
        if node is not None:
            table.set("catalog", None)
            table.set("db", None)
            table.set("this", exp.to_identifier(node, quoted=True))


def analyze_sql(
    normalized_sql: str, dialect: str = "databricks", relations: Mapping[str, str] | None = None
) -> SqlAnalysis:
    """Parse once; derive the code hash, the select-star flag and the table references.

    relations maps lower-case catalog.schema.table names of dbt nodes to their unique_id.
    Every fallback can only cause extra builds, never wrong reuses.
    """
    try:
        statements = [e for e in sqlglot.parse(normalized_sql, read=dialect) if e is not None]
    except SqlglotError:
        return _text_fallback(normalized_sql)
    if len(statements) != 1:
        return _text_fallback(normalized_sql)
    root = statements[0]
    ctes = {cte.alias_or_name.lower(): cte.this for cte in root.find_all(exp.CTE)}
    select_star = _star_reaches_relation(root, ctes, frozenset())
    tables, paths, opaque = _table_refs(root, ctes)
    volatile, stamps = _volatile(root)
    _neutralize(root, relations or {})
    try:
        # In place: the analysis above is done, so the tree needs no copy.
        canonical = qualify(
            root,
            dialect=dialect,
            validate_qualify_columns=False,
            expand_stars=False,
            identify=True,
            canonicalize_table_aliases=True,
        ).sql(dialect=dialect, comments=False)
        method = "qualify"
    except Exception:  # noqa: BLE001 - qualify may leave the tree half-changed; re-parse
        try:
            reparsed = sqlglot.parse_one(normalized_sql, read=dialect)
            _neutralize(reparsed, relations or {})
            canonical = reparsed.sql(dialect=dialect, comments=False)
            method = "parse"
        except Exception:  # noqa: BLE001
            return _text_fallback(normalized_sql)
    return SqlAnalysis(
        f"{method}:{_sha(canonical)}",
        select_star,
        frozenset(tables),
        frozenset(paths),
        opaque,
        volatile,
        stamps,
    )


def analyze_node_sql(
    compiled_sql: str,
    invocation_id: str | None = None,
    run_started_at: str | None = None,
    last_text_hash: str | None = None,
    last_sql_facts: object = None,
    dialect: str = "databricks",
    relations: Mapping[str, str] | None = None,
) -> tuple[str, SqlAnalysis]:
    """Return (text_hash, analysis); reuse the last build's facts when the SQL is unchanged."""
    sql = normalize_runtime_values(compiled_sql, invocation_id, run_started_at)
    key = text_hash(sql)
    if key == last_text_hash:
        cached = SqlAnalysis.from_json(last_sql_facts)
        if cached is not None:
            return key, cached
    return key, analyze_sql(sql, dialect, relations)


def unrendered_hash(
    raw_code: str, macro_sql: Mapping[str, str], unrendered_config: Mapping[str, object]
) -> str:
    return _sha(
        _canonical_json(
            {"raw_code": raw_code, "macros": dict(macro_sql), "config": dict(unrendered_config)}
        )
    )


def _normalize_type(value: object) -> object:
    """DECIMAL(10, 2) and decimal(10,2) are one type. Case and spaces only: sqlglot's type
    parser drops struct field types in some dialects, which would hide real changes."""
    return "".join(value.split()).lower() if isinstance(value, str) else value


def config_hash(
    config: Mapping[str, object],
    columns: Mapping[str, Mapping[str, object]] | None = None,
    constraints: Sequence[Mapping[str, object]] | None = None,
    macros: Mapping[str, str] | None = None,
    hooks: Sequence[str] | None = None,
) -> str:
    kept = {key: value for key, value in config.items() if key not in IGNORED_CONFIG_KEYS}
    cols = {
        name: {
            "data_type": _normalize_type(column.get("data_type")),
            "constraints": column.get("constraints"),
        }
        | {key: column[key] for key in _OPTIONAL_COLUMN_KEYS if column.get(key) is not None}
        for name, column in (columns or {}).items()
    }
    payload: dict[str, object] = {"config": kept, "columns": cols}
    # optional parts only when set, so hashes of nodes without them stay the same
    if constraints:
        payload["constraints"] = list(constraints)
    if macros:
        payload["macros"] = dict(macros)
    if hooks:
        payload["hooks"] = list(hooks)
    return _sha(_canonical_json(payload))

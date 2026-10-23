"""Pure decision rules: reuse or build. No I/O, no dbt."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from dbt_hodor.config import NodeStateConfig
from dbt_hodor.metadata import UNKNOWN, DataVersion
from dbt_hodor.records import Record

BUILD = "build"
REUSE = "reuse"
KINDS = ("table", "incremental", "seed", "snapshot", "view", "test", "unsupported")
_OWN_DATA_KINDS = ("table", "incremental", "seed", "snapshot")
# Upstream groups for volatile SQL values (volatile:date, volatile:session, volatile:always).
VOLATILE_PREFIX = "volatile:"
# Upstream groups for the code of the views a node reads through: {code:<unique_id>: code hash}.
CODE_PREFIX = "code:"


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str


@dataclass(frozen=True)
class NodeFacts:
    kind: str  # one of KINDS
    code_hash: str
    config_hash: str
    hash_version: str
    unrendered_hash: str | None = None
    full_refresh: bool = False
    relation_exists: bool = True
    target_data_version: DataVersion = UNKNOWN
    select_star_view: bool = False
    # One group per direct parent, external input or volatile value: {table: version},
    # or None when unknown.
    upstream_groups: Mapping[str, Mapping[str, DataVersion] | None] = field(default_factory=dict)


def _code_changed(facts: NodeFacts, last: Record, cfg: NodeStateConfig) -> bool:
    if facts.hash_version != last.hash_version:
        return True
    rendered = facts.code_hash != last.code_hash
    if not cfg.compare_unrendered_code:
        return rendered
    unrendered = facts.unrendered_hash is None or facts.unrendered_hash != last.unrendered_hash
    return rendered and unrendered


def _group_changed(group: Mapping[str, DataVersion], last: Record) -> bool:
    return any(version.token != last.upstreams.get(table) for table, version in group.items())


def decide(facts: NodeFacts, last: Record | None, cfg: NodeStateConfig, now_ms: int) -> Decision:
    """Rules 2-15 of the design, first match wins. Shadow mode (rule 1) is the caller's job."""
    if facts.full_refresh:
        return Decision(BUILD, "full_refresh")
    if facts.kind not in KINDS or facts.kind == "unsupported":
        return Decision(BUILD, "unsupported")
    if last is None or (facts.kind == "test" and last.test_result is None):
        return Decision(BUILD, "no_history")
    if not facts.relation_exists:
        return Decision(BUILD, "relation_missing")
    if _code_changed(facts, last, cfg):
        return Decision(BUILD, "code_changed")
    if facts.config_hash != last.config_hash:
        return Decision(BUILD, "config_changed")
    # a view's version is its definition: a run without hodor may have replaced it
    if (facts.kind in _OWN_DATA_KINDS or facts.kind == "view") and (
        not facts.target_data_version.known or facts.target_data_version.token != last.data_version
    ):
        return Decision(BUILD, "target_modified")
    if facts.kind == "view":
        if facts.select_star_view:
            return Decision(BUILD, "select_star_view")
        return Decision(REUSE, "view_unchanged")

    groups = facts.upstream_groups
    if any(g is None or not all(v.known for v in g.values()) for g in groups.values()):
        return Decision(BUILD, "unknown_upstream")
    # a view in between changed its SQL: the rows this node reads changed, whatever their data
    if any(_group_changed(g, last) for k, g in groups.items() if k.startswith(CODE_PREFIX)):  # type: ignore[arg-type]
        return Decision(BUILD, "upstream_code_changed")
    data = {k: g for k, g in groups.items() if not k.startswith(CODE_PREFIX)}
    changed = [_group_changed(g, last) for k, g in data.items() if not _volatile(k)]  # type: ignore[arg-type]
    if cfg.require_fresh_data_from == "all":
        fresh = bool(changed) and all(changed)
    else:
        fresh = any(changed)
    # a new date or user is new data on its own, even with require_fresh_data_from: all
    volatile = any(_group_changed(g, last) for k, g in data.items() if _volatile(k))  # type: ignore[arg-type]
    if not fresh and not volatile:
        return Decision(REUSE, "no_new_data")

    tolerance_ms = 0 if facts.kind == "test" else cfg.lag_tolerance_s * 1000
    if last.built_at_ms is not None and now_ms - last.built_at_ms < tolerance_ms:
        return Decision(REUSE, "within_lag_tolerance")
    return Decision(BUILD, "new_upstream_data" if fresh else "volatile_sql")


def _volatile(group: str) -> bool:
    return group.startswith(VOLATILE_PREFIX)


def upstream_versions(facts: NodeFacts) -> dict[str, str | None]:
    """The {table: token} map a build record stores for the next run's comparison."""
    return {
        table: version.token
        for group in facts.upstream_groups.values()
        if group is not None
        for table, version in group.items()
    }

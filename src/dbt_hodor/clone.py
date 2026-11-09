"""Decides whether a node that would build may be cloned from the base target. Pure: no I/O."""

from __future__ import annotations

from collections.abc import Mapping

from .config import BaseTarget
from .engine import BUILD, CODE_PREFIX, VOLATILE_PREFIX, Decision, NodeFacts
from .lineage import normalize_relation
from .records import CARRY, SUCCESS_STATUSES, Record

CLONE = "clone"  # action of a record of a table cloned from the base target
CLONED = "cloned_from_base"  # clone, nothing more to run
PRE_CLONED = "pre_cloned_from_base"  # clone, then run the model on top (pre_clone)
# builds that must stay builds: the run asked for a rebuild, or hodor failed
_NO_CLONE_REASONS = ("full_refresh", "hodor_error")
_CLONE_KINDS = ("table", "incremental")
# a clone record is no source: clone chains stay one deep, as in dbt
_SOURCE_ACTIONS = ("build", CARRY)


def relation_ids(*record_sets: Mapping[str, Record]) -> dict[str, str]:
    """{relation: unique_id} from the `relation` in each record's node_hashes, lower-cased.
    A name that two nodes claim is left out, so it maps to nothing."""
    ids: dict[str, str] = {}
    clashes: set[str] = set()
    for records in record_sets:
        for uid, record in records.items():
            hashes = record.node_hashes
            relation = hashes.get("relation") if isinstance(hashes, Mapping) else None
            if not isinstance(relation, str) or not relation:
                continue
            key = normalize_relation(relation)
            if ids.setdefault(key, uid) != uid:
                clashes.add(key)
    return {relation: uid for relation, uid in ids.items() if relation not in clashes}


def node_upstreams(
    upstreams: Mapping[str, str | None], ids: Mapping[str, str]
) -> dict[str, str | None] | None:
    """Re-key a record's {table: token} by unique_id. Keys in an upstreams map are table
    names, `code:<unique_id>` (a view's code hash; already an id, kept as is), `volatile:*`
    and `delta:<path>`. A table that maps to no node, and the other kinds, give None. So do
    two tables of one node (its relation in two targets) with different versions."""
    out: dict[str, str | None] = {}
    for key, token in upstreams.items():
        if key.startswith(CODE_PREFIX):
            out[key] = token
        elif key.startswith(VOLATILE_PREFIX) or key not in ids:
            return None
        elif out.setdefault(ids[key], token) != token:
            return None
    return out


def may_clone(
    facts: NodeFacts,
    decision: Decision,
    base: Record | None,
    base_cfg: BaseTarget,
    current_upstreams_by_id: Mapping[str, str | None] | None,
    base_upstreams_by_id: Mapping[str, str | None] | None,
    base_is_current: bool,
    volatile_stamps: frozenset[str] = frozenset(),
    volatile: frozenset[str] = frozenset(),
) -> Decision | None:
    """`Decision(BUILD, CLONED)` when the base target's table holds the rows a build would
    write now, else None (the node builds as decided). The action stays build: the caller
    clones. `Decision(BUILD, PRE_CLONED)` (pre_clone, code changed) means clone, then run
    the model on top; with `CLONED` the clone is the whole job.

    `volatile_stamps` are the classes of `SqlAnalysis.volatile_stamps`. A "session" stamp
    (current_user, current_schema) would hold the base target's value, so it blocks the
    clone. "date" and "always" stamps keep the base build's values, as on reuse.
    `volatile` is `SqlAnalysis.volatile`: with `evaluate_volatile_sql` off, a session
    function in a filter is only there, and blocks the clone the same way.

    The caller checks what this function cannot see: that the base table's current data
    version still equals `base.data_version` (v1 reads it; v2 sends it as
    `clone_required_last_modified_epoch`). Any unknown input gives None.

    `facts.code_hash` and `base.code_hash` must be computed with relations replaced by
    unique_ids, as schema names differ between targets. With `base_cfg.pre_clone`, an
    incremental node skips the code check: it is cloned, then runs on top of the clone.
    """
    if not base_cfg.allow_clones or base_is_current or decision.action != BUILD:
        return None
    if decision.reason in _NO_CLONE_REASONS or "session" in volatile_stamps | volatile:
        return None
    if facts.kind not in _CLONE_KINDS or base is None:
        return None
    if base.action not in _SOURCE_ACTIONS or base.status not in SUCCESS_STATUSES:
        return None
    if facts.config_hash != base.config_hash or facts.hash_version != base.hash_version:
        return None
    pre = base_cfg.pre_clone and facts.kind == "incremental"
    code_differs = facts.code_hash != base.code_hash
    if code_differs and not pre:
        return None
    if any(g is None or k.startswith(VOLATILE_PREFIX) for k, g in facts.upstream_groups.items()):
        return None
    if current_upstreams_by_id is None or base_upstreams_by_id is None:
        return None
    if dict(current_upstreams_by_id) != dict(base_upstreams_by_id):
        return None
    if any(token is None for token in current_upstreams_by_id.values()):
        return None  # two unknown versions are not equal versions
    return Decision(BUILD, PRE_CLONED if code_differs else CLONED)

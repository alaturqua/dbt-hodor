"""Finds the base target's relation for nodes a run does not select. Pure: no I/O."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from .lineage import normalize_relation
from .records import CARRY, SUCCESS_STATUSES, Record

# a clone record counts: the cloned table holds the rows of the build it copied
_BUILD_ACTIONS = ("build", CARRY, "clone")


def deferred_relations(
    unselected: Iterable[str],
    base: Mapping[str, Record],
    base_target: str,
    current_target: str,
    newest: Mapping[str, Record] | None = None,
) -> dict[str, str]:
    """Map each unselected unique_id to the exact relation its base-target build wrote.

    A node is left out when it has no successful build record or the record holds no relation,
    so dbt resolves it as it would without defer. Deferring a target to itself returns nothing.
    `newest` holds the newest record of any status per unique_id: when it names another
    relation (a renamed node whose build under the new name failed), the successful build's
    relation may be an orphan nobody updates, so the node is left out too.
    """
    if base_target == current_target:
        return {}
    out: dict[str, str] = {}
    for unique_id in unselected:
        record = base.get(unique_id)
        if record is None or record.action not in _BUILD_ACTIONS:
            continue
        if record.status not in SUCCESS_STATUSES:
            continue
        relation = _relation(record)
        if relation is None:
            continue
        newer = (newest or {}).get(unique_id)
        if newer is not None and newer.record_id != record.record_id:
            other = _relation(newer)
            if other is None or normalize_relation(other) != normalize_relation(relation):
                continue
        out[unique_id] = relation
    return out


def _relation(record: Record) -> str | None:
    hashes = record.node_hashes
    relation = hashes.get("relation") if isinstance(hashes, Mapping) else None
    return relation if isinstance(relation, str) and relation else None


def split_relation(relation: str, quote: str) -> list[str] | None:
    """The dot-separated parts of a rendered relation, quotes removed. None when the text is
    not a plain sequence of quoted or unquoted parts, so the node is not deferred."""
    parts: list[str] = []
    i, n = 0, len(relation)
    while True:
        if relation.startswith(quote, i):
            end = relation.find(quote, i + 1)
            if end < 0:
                return None
            part, i = relation[i + 1 : end], end + 1
        else:
            end = relation.find(".", i)
            end = n if end < 0 else end
            part, i = relation[i:end], end
            if quote in part:
                return None
        if not part:
            return None
        parts.append(part)
        if i == n:
            return parts
        if relation[i] != ".":
            return None
        i += 1

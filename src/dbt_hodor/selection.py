"""Answers dbt `state:` selectors from node hashes and the base target's records. Pure: no I/O."""

from __future__ import annotations

from collections.abc import Mapping

from .records import Record

NodeHashes = Mapping[str, object]

# criterion -> key in `node_hashes`
SUB_CRITERIA = {
    "modified.body": "body",
    "modified.configs": "configs",
    "modified.contract": "contract",
    "modified.macros": "macros",
    "modified.relation": "relation_hash",
    "modified.persisted_descriptions": "persisted_descriptions",
}
CRITERIA = ("new", "modified", "unmodified", "old", *SUB_CRITERIA)


class SelectionError(Exception):
    """The criterion is not one hodor can answer."""


def _differs(key: str, now: NodeHashes, record: Record | None, engine: str) -> bool:
    """True when the node is new, the record is unusable, or the hash at `key` changed."""
    if record is None:
        return True
    stored = record.node_hashes
    if stored is None or stored.get("engine") != engine:
        return True
    return now.get(key) != stored.get(key)


def select(
    criterion: str,
    current: Mapping[str, NodeHashes],
    base: Mapping[str, Record],
    engine: str,
) -> frozenset[str]:
    """Return the ids in `current` that match `criterion`, judged against `base`."""
    if criterion not in CRITERIA:
        raise SelectionError(f"unknown state selector: {criterion}")
    if criterion in ("new", "old"):
        is_new = criterion == "new"
        return frozenset(uid for uid in current if (uid not in base) == is_new)
    keys = (
        tuple(SUB_CRITERIA.values())
        if criterion in ("modified", "unmodified")
        else (SUB_CRITERIA[criterion],)
    )
    want_modified = criterion != "unmodified"
    return frozenset(
        uid
        for uid, now in current.items()
        if any(_differs(k, now, base.get(uid), engine) for k in keys) == want_modified
    )

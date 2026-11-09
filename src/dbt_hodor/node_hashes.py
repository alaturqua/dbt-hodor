"""What dbt-core's `state:modified` compares for a node, hashed for a record, and the hash
of a seed file."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from typing import Any

from dbt.artifacts.resources import TestConfig
from dbt_common.contracts.config.base import CompareBehavior


def seed_hash(seed: Any) -> str:
    """sha256 of the seed file. dbt's own checksum is only the path for large seeds."""
    with open(os.path.join(seed.root_path, seed.original_file_path), "rb") as f:
        data = f.read().replace(b"\r\n", b"\n")  # same hash on Windows and Linux checkouts
    return hashlib.sha256(data).hexdigest()


# the config keys dbt's TestConfig.same_contents compares; every other test config is ignored
_TEST_COMPARED_KEYS = (
    "severity",
    "where",
    "limit",
    "fail_calc",
    "warn_if",
    "error_if",
    "store_failures",
    "store_failures_as",
    "sql_header",
)


def _digest(value: object) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _compared_config(node: Any) -> dict[str, object]:
    """The unrendered config as dbt's `same_config` compares it: without the keys its config
    class excludes (alias, schema, database, tags, group); a test compares only modifiers."""
    unrendered = dict(getattr(node, "unrendered_config", None) or {})
    config = getattr(node, "config", None)
    if isinstance(config, TestConfig):
        return {k: unrendered[k] for k in _TEST_COMPARED_KEYS if k in unrendered}
    get_fields = getattr(type(config), "_get_fields", None)
    if get_fields is None:
        return unrendered
    excluded = {name for fld, name in get_fields() if not CompareBehavior.should_include(fld)}
    return {k: v for k, v in unrendered.items() if k not in excluded}


def _parse_macros(node: Any) -> tuple[str, ...]:
    return tuple(getattr(getattr(node, "depends_on", None), "macros", None) or ())


def _macro_closure(
    node: Any, manifest: Any, macros: Sequence[str] | None = None
) -> dict[str, str | None]:
    """macro_sql of every macro the node calls, directly or through other macros. `macros`:
    the node's direct macros at parse time, when the node may have been compiled since."""
    out: dict[str, str | None] = {}
    stack = list(_parse_macros(node) if macros is None else macros)
    while stack:
        macro_id = stack.pop()
        if str(macro_id) in out:
            continue
        macro = manifest.macros.get(macro_id) if macro_id else None
        out[str(macro_id)] = macro.macro_sql if macro is not None else None
        if macro is not None:
            stack.extend(macro.depends_on.macros)
    return out


def v1_node_hashes(
    node: Any, manifest: Any, macros: Sequence[str] | None = None
) -> dict[str, object]:
    """`node_hashes` of a record: what dbt-core's `state:modified` sub-selectors compare,
    and the relation name as written. State selection hashes parsed nodes; a record hashes
    a compiled one, so it passes `macros`, the node's macros at parse time: compiling a
    generic test adds macros to its `depends_on`."""
    unrendered = getattr(node, "unrendered_config", None) or {}
    checksum = getattr(node, "checksum", None)
    contract = getattr(node, "contract", None)
    persist = getattr(getattr(node, "config", None), "persist_docs", None) or {}
    persisted: dict[str, object] = {}
    if persist.get("relation"):
        persisted["relation"] = getattr(node, "description", None)
    if persist.get("columns"):
        columns = getattr(node, "columns", None) or {}
        persisted["columns"] = {name: col.description for name, col in columns.items()}
    return {
        "engine": "v1",
        "relation": getattr(node, "relation_name", None) or None,
        "body": checksum.checksum if checksum is not None else None,
        # dbt's same_fqn and same_ref_representation have no key of their own: they count
        # as configs, so modified.configs selects them too
        "configs": _digest(
            [_compared_config(node)]
            + [getattr(node, k, None) for k in ("fqn", "latest_version", "access")]
            + [getattr(node, "deprecation_date", None)]
        ),
        "contract": (contract.checksum or "enforced")
        if contract is not None and contract.enforced
        else None,
        "macros": _digest(_macro_closure(node, manifest, macros)),
        "relation_hash": _digest({k: unrendered.get(k) for k in ("database", "schema", "alias")}),
        "persisted_descriptions": _digest(persisted) if persisted else None,
    }

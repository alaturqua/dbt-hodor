"""The base target (`defer_to_target`): `state:` selection and `--defer` answered from its
records. Reading the store is the only I/O."""

from __future__ import annotations

import dataclasses
import os
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dbt.adapters.factory import get_adapter
from dbt.artifacts.resources import DeferRelation
from dbt.node_types import REFABLE_NODE_TYPES
from dbt_common.exceptions import DbtRuntimeError

from dbt_hodor.config import DAY_MS, WAREHOUSE_STORES, Settings, load_settings, node_state_config
from dbt_hodor.defer import deferred_relations, split_relation
from dbt_hodor.node_hashes import v1_node_hashes
from dbt_hodor.records import Record
from dbt_hodor.records import by_unique_id as _by_unique_id
from dbt_hodor.selection import select
from dbt_hodor.session import _profile_settings, debug, open_store, warn
from dbt_hodor.store import StoreError


def _record_relation(record: Record) -> str | None:
    hashes = record.node_hashes
    relation = hashes.get("relation") if isinstance(hashes, Mapping) else None
    return relation if isinstance(relation, str) and relation else None


def _node_base(node: Any) -> str | None:
    """The node's own `state: defer_to_target`, which overrides the target's for that node."""
    config = getattr(node, "config", None)
    raw = config.get("state") if config is not None and hasattr(config, "get") else None
    return node_state_config(raw if isinstance(raw, Mapping) else None, debug).defer_to_target


def _check_base(unique_id: str, node: Any, base: str) -> None:
    """State selection compares every node with one base target: a node that names another
    one cannot be answered."""
    other = _node_base(node)
    if other is not None and other != base:
        raise StoreError(
            f"node {unique_id} sets defer_to_target {other}; state selection uses one base target"
        )


def _base_settings(config: Any) -> Settings:
    return load_settings(
        _profile_settings(config),
        os.environ,
        lambda m: warn(f"dbt-hodor: {m}"),
        adapter_type=config.credentials.type,
    )


def _read_base(
    settings: Settings,
    config: Any,
    adapter: Any,
    base: str,
    connect: bool = True,
    any_status: Sequence[bool] = (False,),
    reused: bool = False,
) -> list[dict[str, Record]]:
    """The base target's newest record per unique_id, of any action: one map per entry of
    `any_status`, successful records only for False. `reused` counts reuse records as
    successful (state selection). `connect` False when the caller already holds a dbt
    connection."""
    store = open_store(settings, config, adapter, read_only=True)
    try:
        since = int(time.time() * 1000) - settings.lookback_days * DAY_MS

        def read() -> list[dict[str, Record]]:
            maps = []
            for flag in any_status:
                extra: dict[str, bool] = {"any_status": True} if flag else {}
                if reused:  # the default read stays as it was
                    extra["reused"] = True
                records = store.load_latest(config.project_name, base, since, **extra)
                maps.append(_by_unique_id(records.values()))
            return maps

        if connect and settings.store in WAREHOUSE_STORES:  # these read through dbt's connection
            with adapter.connection_named("hodor"):
                return read()
        return read()
    finally:
        store.close()


def state_selection(
    config: Any, manifest: Any, criterion: str, nodes: Iterable[tuple[str, Any]]
) -> frozenset[str] | None:
    """The ids among `nodes` that match a `state:` criterion against the base target's latest
    records. None without a base target: dbt-core's own method answers. Anything else that
    stops the comparison fails the run, as dbt-core does without a comparison manifest."""
    try:
        settings = _base_settings(config)
    except Exception as exc:  # noqa: BLE001 - no known base target: dbt-core answers
        warn(f"dbt-hodor: cannot read the base target for state selection: {exc}")
        return None
    base = settings.defer_to_target
    if base is None:
        return None
    try:
        if settings.mode == "off":
            raise StoreError("dbt-hodor is off")
        current: dict[str, dict[str, object]] = {}
        for uid, node in nodes:
            _check_base(uid, node, base)
            current[uid] = v1_node_hashes(node, manifest)
        # a node only reused in the base target still counts, with the hashes it ran with
        (latest,) = _read_base(settings, config, get_adapter(config), base, reused=True)
        return select(criterion, current, latest, "v1")
    except Exception as exc:
        raise DbtRuntimeError(f"dbt-hodor cannot compare with {base}: {exc}") from exc


def _defer_relation(config: Any, adapter: Any, node: Any, relation: str) -> DeferRelation | None:
    """The DeferRelation dbt's merge_from_artifact would build, from the relation a base
    build wrote. None when the text does not render back to the same relation. A record holds
    only the relation: config, description, meta and tags come from the current node, not
    from the base target's, and there is no compiled code."""
    relation_cls = adapter.Relation
    parts = split_relation(relation, relation_cls.quote_character)
    if parts is None or len(parts) not in (2, 3):
        return None
    # two parts: the adapter does not render the database, so the node's own one is kept
    database, schema, alias = parts if len(parts) == 3 else [node.database, *parts]
    deferred = DeferRelation(
        database=database,
        schema=schema,
        alias=alias,
        relation_name=relation,
        resource_type=node.resource_type,
        name=node.name,
        description=node.description,
        compiled_code=None,  # a record holds no code
        meta=node.meta,
        tags=node.tags,
        config=node.config,
    )
    if str(relation_cls.create_from(config, deferred)) != relation:
        return None
    return deferred


def defer_target(config: Any) -> str | None:
    """The base target name for a warning, or None when it cannot be read."""
    try:
        return _base_settings(config).defer_to_target
    except Exception:  # noqa: BLE001 - only names the target in a message
        return None


def defer_to_base(config: Any, manifest: Any, selected: Iterable[str]) -> bool:
    """`--defer` without --state: point unselected nodes at the relations the base target's
    builds wrote. False when hodor does not defer, so dbt-core answers as without hodor. A
    node is deferred only from a successful base build whose relation renders back exactly."""
    try:
        settings = _base_settings(config)
    except Exception as exc:  # noqa: BLE001 - no known base target: dbt-core answers
        warn(f"dbt-hodor: cannot read the base target for defer: {exc}")
        return False
    base = settings.defer_to_target
    if base is None or base == config.target_name:
        return False
    try:
        if settings.mode == "off":
            raise StoreError("dbt-hodor is off")
        chosen = set(selected)
        nodes = {
            uid: node
            for uid, node in manifest.nodes.items()
            if uid not in chosen
            and node.resource_type in REFABLE_NODE_TYPES
            and not node.is_ephemeral
            and _node_base(node) in (None, base)  # a node naming another base is not deferred
        }
        adapter = get_adapter(config)
        # dbt's before_run holds a connection; dbt-hodor's run start does not
        held = adapter.connections.get_if_exists() is not None
        latest, newest = _read_base(
            settings, config, adapter, base, connect=not held, any_status=(False, True)
        )
        found = deferred_relations(nodes, latest, base, config.target_name, newest)
        deferred: dict[str, Any] = {}
        for uid, relation in found.items():
            target = _defer_relation(config, adapter, nodes[uid], relation)
            if target is not None:
                deferred[uid] = dataclasses.replace(nodes[uid], defer_relation=target)
    except Exception as exc:  # noqa: BLE001 - no hodor defer: dbt-core answers
        warn(f"dbt-hodor could not defer to {base}: {exc}; dbt fails the run without --state")
        return False
    manifest.nodes.update(deferred)  # all or nothing
    if not latest:
        warn(f"dbt-hodor found no records for {base} to defer to")
    elif nodes and not deferred:
        warn(f"dbt-hodor deferred none of {len(nodes)} unselected nodes to {base}")
    debug(f"dbt-hodor: deferred {len(deferred)} of {len(nodes)} unselected nodes to {base}")
    return True

"""dbt v2 state-protocol requests to decision facts, and decisions back to responses. Pure:
no I/O. Anything the request leaves unknown maps to a fact that makes the node build."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..clone import CLONED
from ..engine import CODE_PREFIX, REUSE, VOLATILE_PREFIX, Decision, NodeFacts
from ..hashing import HASH_VERSION, SqlAnalysis, analyze_node_sql, analyze_sql
from ..lineage import normalize_relation
from ..metadata import UNKNOWN, DataVersion
from ..records import Record
from .proto.query_cache_protobuf.query_cache import shared_pb2
from .proto.query_cache_protobuf.query_cache.services import (
    clone_service_pb2,
    selector_service_pb2,
    sql_service_pb2,
)

# v1 and v2 hash different inputs, so their records never match.
V2_HASH_VERSION = "v2-" + HASH_VERSION
_T = shared_pb2.ModelExecutionType
_KINDS = {
    _T.FULL: "table",
    _T.APPEND: "incremental",
    _T.MERGE: "incremental",
    _T.INSERT_OVERWRITE: "incremental",
    _T.DELETE_INSERT: "incremental",
    _T.SNAPSHOT: "snapshot",
}
# The adapters dbt v2 starts its state client for; sqlglot reads each of these dialects.
_DIALECTS = frozenset({"databricks", "spark", "snowflake", "bigquery", "redshift"})
# A table, path or function the SQL reads that the request does not list.
_OPAQUE_GROUP = "<opaque inputs>"
# node_hashes key -> field of DbtNodeState and DbtNodeData
_HASH_FIELDS = {
    "body": "node_body_hash",
    "configs": "node_configs_hash",
    "contract": "node_contract_hash",
    "macros": "node_macros_hash",
    # no `relation_hash`: dbt 2.0.8 sends node_database_representation only with a state
    # selection, as the session target's relation, never with the builds the base target
    # recorded. Compared, it would mark every node modified
    "persisted_descriptions": "node_persisted_descriptions_hash",
}
_SC = selector_service_pb2
# SelectorCriteria -> criterion of `selection.select`
_CRITERIA = {
    _SC.MODIFIED: "modified",
    _SC.NEW: "new",
    _SC.UNMODIFIED: "unmodified",
    _SC.OLD: "old",
    _SC.BODY: "modified.body",
    _SC.CONFIGS: "modified.configs",
    _SC.RELATION: "modified.relation",
    _SC.PERSISTED_DESCRIPTIONS: "modified.persisted_descriptions",
    _SC.MACROS: "modified.macros",
    _SC.CONTRACT: "modified.contract",
}
_CLONE_SQL = {
    "databricks": "CREATE OR REPLACE TABLE {t} SHALLOW CLONE {s}",
    "spark": "CREATE OR REPLACE TABLE {t} SHALLOW CLONE {s}",
    "snowflake": "CREATE OR REPLACE TABLE {t} CLONE {s}",
    "bigquery": "CREATE OR REPLACE TABLE {t} CLONE {s}",
}


@dataclass(frozen=True)
class CloneAnswer:
    """The fields of a `ReadyToCloneResponse` that the clone decision fills."""

    clone_sqls: tuple[str, ...]
    clone_source: str
    clone_target: str
    # the base record's data_version: dbt builds instead when the base table differs
    clone_required_last_modified_epoch: int | None


def normalize(name: str) -> str:
    """`Cat`.`Sch`.`T` -> cat.sch.t, the form record upstreams and sqlglot table names use."""
    return normalize_relation(name)


def _optional(msg: Any, name: str) -> str | None:
    return getattr(msg, name) if msg.HasField(name) else None


def _config_hash(state: Any, semantic_extras: Mapping[str, str], execution_type: int) -> str:
    configs = _optional(state, "node_configs_hash") if state is not None else None
    contract = _optional(state, "node_contract_hash") if state is not None else None
    text = json.dumps(
        {
            "configs": configs,
            "contract": contract,
            "semantic_extras": dict(semantic_extras),
            "execution_type": execution_type,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _version(info: Any) -> DataVersion:
    if not info.HasField("last_modified_epoch"):
        return UNKNOWN
    epoch = info.last_modified_epoch
    return DataVersion(str(epoch), changed_at_ms=epoch)


def node_facts(
    req: Any, last: Record | None, relations: Mapping[str, str]
) -> tuple[NodeFacts, SqlAnalysis]:
    """Facts for a `SubmitEnrichedSQLRequest`, and the analysis of its SQL.

    `relations` maps lower-case relation names of dbt nodes to their unique_id. Volatile
    groups carry unknown versions (the server cannot read the warehouse's date or user), so
    a node with volatile SQL builds; the caller drops them when `evaluate_volatile_sql` is
    false, and adds `volatile_stamps` from the analysis when it is true.
    """
    dialect = req.dialect.lower()
    state = req.dbt_node_state if req.HasField("dbt_node_state") else None
    kind = _KINDS.get(req.execution_type, "unsupported")
    if (
        dialect not in _DIALECTS
        or not req.HasField("target_table")
        or state is None
        or not state.HasField("node_configs_hash")
    ):
        kind = "unsupported"
    if dialect in _DIALECTS:
        # a record of another hash version holds that version's code hash
        current = last if last is not None and last.hash_version == V2_HASH_VERSION else None
        _, analysis = analyze_node_sql(
            req.sql,
            last_text_hash=current.text_hash if current else None,
            last_sql_facts=current.sql_facts if current else None,
            dialect=dialect,
            relations=relations,
        )
    else:
        analysis = SqlAnalysis("", False, opaque_inputs=True)

    target = normalize(req.target_table) if req.HasField("target_table") else None
    target_version: DataVersion | None = None
    groups: dict[str, Mapping[str, DataVersion] | None] = {}
    for info in req.tables:
        name, version = normalize(info.name), _version(info)
        if name == target:
            same = target_version in (None, version)
            target_version = version if same else UNKNOWN
            continue
        seen = groups.get(name)
        if seen is not None and seen[name] != version:
            version = UNKNOWN  # two epochs for one table: neither is trusted
        groups[name] = {name: version}

    # a view in between is read live: its volatile values and tables count as the node's own
    reads = [analysis]
    views: set[str] = set()
    for dep in req.query_dependencies:
        name = normalize(dep.name)
        views.add(name)
        key = f"{CODE_PREFIX}{relations.get(name, name)}"
        view = (
            analyze_sql(dep.query, dialect, relations)
            if dialect in _DIALECTS
            else SqlAnalysis("", False, opaque_inputs=True)
        )
        reads.append(view)
        groups[key] = {key: DataVersion(view.code_hash or None)}

    listed = set(groups) | views | ({target} if target else set())
    if any(a.opaque_inputs or a.delta_paths or a.tables - listed for a in reads):
        groups[_OPAQUE_GROUP] = None
    for cls in sorted(set().union(*(a.volatile for a in reads))):
        key = f"{VOLATILE_PREFIX}{cls}"
        groups[key] = {key: UNKNOWN}

    facts = NodeFacts(
        kind=kind,
        code_hash=analysis.code_hash,
        config_hash=_config_hash(state, req.semantic_extras, req.execution_type),
        hash_version=V2_HASH_VERSION,
        relation_exists=target_version is not None,
        target_data_version=target_version or UNKNOWN,
        upstream_groups=groups,
        epoch_versions=True,
    )
    return facts, analysis


def seed_facts(req: Any, last: Record | None) -> NodeFacts:
    """Facts for a `SubmitValuesRequest`. v2 sends no timestamp for the seed's own table, so
    existence and version come from the last build; a later outside write is caught when a
    model's request lists the seed table with another epoch."""
    state = req.dbt_node_state if req.HasField("dbt_node_state") else None
    version = last.data_version if last is not None else None
    return NodeFacts(
        kind="seed" if req.values_hash else "unsupported",
        code_hash=req.values_hash,
        config_hash=_config_hash(state, req.semantic_extras, shared_pb2.VALUES),
        hash_version=V2_HASH_VERSION,
        relation_exists=last is not None,
        target_data_version=DataVersion(version) if version is not None else UNKNOWN,
        epoch_versions=True,
    )


def v2_node_hashes(state: Any, relation: str) -> dict[str, object]:
    """`node_hashes` of a record from a `DbtNodeState` or `DbtNodeData`; relation as written."""
    hashes: dict[str, object] = {"engine": "v2", "relation": relation}
    hashes.update({key: _optional(state, name) for key, name in _HASH_FIELDS.items()})
    return hashes


def _explained(result: int, reason: str) -> Any:
    return shared_pb2.ExplainedDecision(decision=result, decision_description=reason)


def answer(decision: Decision, request_id: str, clone: CloneAnswer | None, out_cls: Any) -> Any:
    """The response for a decision. Reuse skips; a plain clone with its SQL clones; anything
    else executes (a pre_clone must still run the model after the clone)."""
    if decision.action == REUSE:
        skip = sql_service_pb2.SkipExecutionResponse(
            explained_decision=_explained(shared_pb2.SKIP_EXECUTION, decision.reason),
            execution_decision_id=request_id,
        )
        return out_cls(skip_execution=skip)
    # the epoch is dbt's only check that the base table is unchanged since its build
    if (
        clone is not None
        and decision.reason == CLONED
        and clone.clone_sqls
        and clone.clone_required_last_modified_epoch is not None
    ):
        ready = clone_service_pb2.ReadyToCloneResponse(
            request_id=request_id,
            clone_sqls=list(clone.clone_sqls),
            clone_source=clone.clone_source,
            clone_target=clone.clone_target,
            clone_required_last_modified_epoch=clone.clone_required_last_modified_epoch,
            explained_decision=_explained(shared_pb2.READY_TO_CLONE, decision.reason),
            execution_decision_id=request_id,
        )
        return out_cls(ready_to_clone=ready)
    execute = sql_service_pb2.ReadyToExecuteResponse(
        request_id=request_id,
        explained_decision=_explained(shared_pb2.READY_TO_EXECUTE, decision.reason),
        execution_decision_id=request_id,
    )
    return out_cls(ready_to_execute=execute)


def clone_sql(dialect: str, target: str, source: str) -> str | None:
    """The statement that clones `source` into `target`; None where dbt v2 cannot clone."""
    template = _CLONE_SQL.get(dialect.lower())
    return template.format(t=target, s=source) if template else None


def criterion(selector_criteria: int) -> str | None:
    """The `selection.select` criterion of a `SelectorCriteria`; None when unspecified."""
    return _CRITERIA.get(selector_criteria)


def clone_epoch(data_version: str | None) -> int | None:
    """A record's data_version as the epoch dbt checks before a clone; None when it is not
    an epoch (dbt could not check the base table, so the node builds)."""
    if data_version is None or not (data_version.isascii() and data_version.isdigit()):
        return None
    return int(data_version)

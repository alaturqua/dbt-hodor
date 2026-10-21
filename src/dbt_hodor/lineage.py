"""Upstream tables of a node: dbt parents, sources and external inputs."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass

from dbt_hodor.hashing import SqlAnalysis

# Materializations that store their own data and have their own data version.
OWN_DATA = frozenset({"table", "incremental", "snapshot", "seed"})
# Materializations that store no data: expand them to their own upstream tables.
PASS_THROUGH = frozenset({"view", "ephemeral"})

_IN_PROGRESS = object()


@dataclass(frozen=True)
class GraphNode:
    unique_id: str
    resource_type: str
    materialized: str | None = None
    relation: str | None = None  # normalized catalog.schema.table
    parents: tuple[str, ...] = ()


def normalize_relation(name: str) -> str:
    """`Cat`.`Sch`.`T` -> cat.sch.t"""
    return ".".join(part.strip('`"').lower() for part in name.split("."))


class Lineage:
    """Base tables per node, memoized for the run (views are expanded once, not per child)."""

    def __init__(self, graph: Mapping[str, GraphNode]) -> None:
        self._graph = graph
        self._memo: dict[str, object] = {}

    def base_tables(self, unique_id: str) -> frozenset[str] | None:
        """Tables whose data the node exposes; None when any of them is unknown."""
        cached = self._memo.get(unique_id)
        if cached is _IN_PROGRESS:
            return None  # a cycle: the manifest is a DAG, so treat as unknown
        if unique_id in self._memo:
            return cached  # type: ignore[return-value]
        self._memo[unique_id] = _IN_PROGRESS
        result = self._compute(unique_id)
        self._memo[unique_id] = result
        return result

    def _compute(self, unique_id: str) -> frozenset[str] | None:
        node = self._graph.get(unique_id)
        if node is None:
            return None
        if node.resource_type == "source" or (node.materialized or "view") in OWN_DATA:
            return frozenset({node.relation}) if node.relation else None
        if (node.materialized or "view") not in PASS_THROUGH:
            return None
        tables: set[str] = set()
        for parent in node.parents:
            parent_tables = self.base_tables(parent)
            if parent_tables is None:
                return None
            tables |= parent_tables
        return frozenset(tables)

    def upstream_groups(self, unique_id: str) -> dict[str, frozenset[str] | None]:
        """Map each direct parent to the base tables it reads, or None when unknown."""
        return {parent: self.base_tables(parent) for parent in self._graph[unique_id].parents}


@dataclass(frozen=True)
class ExternalInputs:
    tables: frozenset[str] = frozenset()
    delta_paths: frozenset[str] = frozenset()
    unknown: bool = False


def external_inputs(analysis: SqlAnalysis, known_relations: AbstractSet[str]) -> ExternalInputs:
    """Inputs the SQL reads that the lineage does not cover.

    known_relations: the node's own relation, its parents' relations and their base tables.
    """
    return ExternalInputs(
        analysis.tables - known_relations, analysis.delta_paths, analysis.opaque_inputs
    )

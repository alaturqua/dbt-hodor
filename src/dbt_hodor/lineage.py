"""Upstream tables of a node: dbt parents, sources and external inputs."""

from __future__ import annotations

import threading
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
    """Base tables per node, memoized for the run (views are expanded once, not per child).

    deferred: nodes that also exist under another name in the --defer state. A view or
    ephemeral model may read either one, so expanding it through such a parent gives unknown,
    unless the run told us what the view's own SQL reads (set_reads).
    """

    def __init__(
        self, graph: Mapping[str, GraphNode], deferred: AbstractSet[str] = frozenset()
    ) -> None:
        self._graph = graph
        self._deferred = deferred
        self._reads: dict[str, frozenset[str] | None] = {}
        self._memo: dict[str, object] = {}
        self._lock = threading.RLock()  # dbt's threads share the memo
        self._children: dict[str, list[str]] = {}
        for node in graph.values():
            for parent in node.parents:
                self._children.setdefault(parent, []).append(node.unique_id)

    def set_reads(self, unique_id: str, tables: frozenset[str] | None) -> None:
        """The base tables a view reads, from its compiled SQL in this run."""
        with self._lock:
            self._reads[unique_id] = tables
            # forget the views and ephemerals expanded through this one; a table stops the walk
            stack, seen = [unique_id], set()
            while stack:
                uid = stack.pop()
                node = self._graph.get(uid)
                if uid in seen or node is None or (node.materialized or "view") not in PASS_THROUGH:
                    continue
                seen.add(uid)
                self._memo.pop(uid, None)
                stack.extend(self._children.get(uid, ()))

    def base_tables(self, unique_id: str) -> frozenset[str] | None:
        """Tables whose data the node exposes; None when any of them is unknown."""
        with self._lock:
            return self._base_tables(unique_id)

    def _base_tables(self, unique_id: str) -> frozenset[str] | None:
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
        if unique_id in self._reads:
            return self._reads[unique_id]
        tables: set[str] = set()
        for parent in node.parents:
            if parent in self._deferred:
                return None
            parent_tables = self.base_tables(parent)
            if parent_tables is None:
                return None
            tables |= parent_tables
        return frozenset(tables)

    def views(self, unique_id: str) -> set[str]:
        """The views a read of this node goes through, the node itself included. An ephemeral
        model is inlined, so its own SQL is in the reader's code hash."""
        found: set[str] = set()
        stack = [unique_id]
        while stack:
            node = self._graph.get(stack.pop())
            if node is None or node.unique_id in found or node.resource_type == "source":
                continue
            materialized = node.materialized or "view"
            if materialized not in PASS_THROUGH:
                continue
            if materialized == "view":
                found.add(node.unique_id)
            stack.extend(node.parents)
        return found


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

"""dbt-hodor explain: why each node of one invocation was reused or built."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dbt_hodor.records import CARRY, Record

_OK_STATUSES = ("success", "pass", "reused")


def matches(record: Record, selector: str) -> bool:
    """The unique_id, the relation, or the node name (which matches every model version)."""
    return selector in (record.unique_id, record.node_key) or (
        record.unique_id.split(".")[2:3] == [selector]
    )


def report(
    records: Sequence[Record], last_builds: Mapping[str, Record], select: Sequence[str] = ()
) -> dict[str, Any]:
    """Each record of one invocation next to the build its decision was compared with.

    records are in record order; last_builds holds the latest successful build per node_key
    from before the invocation, so a reused node shows the build it kept.
    """
    nodes = []
    for r in records:
        if select and not any(matches(r, s) for s in select):
            continue
        last = last_builds.get(r.node_key)
        before = last.upstreams if last else {}
        nodes.append(
            {
                "unique_id": r.unique_id,
                "node_key": r.node_key,
                "resource_type": r.resource_type,
                "mode": r.mode,
                "decision": r.decision,
                "reason": r.reason,
                "action": r.action,
                "status": r.status,
                "recorded_at_ms": r.recorded_at_ms,
                "code_hash": r.code_hash,
                "config_hash": r.config_hash,
                "last_build": None
                if last is None
                else {
                    "invocation_id": last.invocation_id,
                    # a carried copy is recorded later than the build it copies
                    "recorded_at_ms": last.built_at_ms
                    if last.action == CARRY and last.built_at_ms is not None
                    else last.recorded_at_ms,
                    "code_hash": last.code_hash,
                    "config_hash": last.config_hash,
                },
                "upstreams": [
                    {"table": t, "last": before.get(t), "now": r.upstreams.get(t)}
                    for t in sorted({*before, *r.upstreams})
                ],
            }
        )
    first = records[0]
    return {
        "project": first.project_name,
        "target": first.target_name,
        "invocation_id": first.invocation_id,
        "started_at_ms": first.recorded_at_ms,
        "nodes": nodes,
    }


# Reasons whose build came from upstream changes; their changed upstreams are the triggers.
_UPSTREAM_REASONS = ("new_upstream_data", "upstream_code_changed", "volatile_sql")
_TOP_TRIGGERS = 10


def summarize(rep: Mapping[str, Any]) -> dict[str, Any]:
    """Counts per decision and reason, and which changed upstreams caused the builds.

    Counted by decision, not action, so a shadow run shows what `on` would have done. An
    upstream that was itself built in the run marks a rebuild that spread down the DAG.
    """
    nodes = rep["nodes"]
    decisions: dict[str, int] = {}
    reasons: dict[tuple[str, str], int] = {}
    for n in nodes:
        decisions[n["decision"]] = decisions.get(n["decision"], 0) + 1
        key = (n["decision"], n["reason"])
        reasons[key] = reasons.get(key, 0) + 1
    built = {n["node_key"] for n in nodes if n["decision"] == "build"}
    triggers: dict[str, int] = {}
    for n in nodes:
        if n["decision"] != "build" or n["reason"] not in _UPSTREAM_REASONS:
            continue
        for u in n["upstreams"]:
            if u["last"] != u["now"]:
                triggers[u["table"]] = triggers.get(u["table"], 0) + 1
    return {
        **{k: rep[k] for k in ("project", "target", "invocation_id", "started_at_ms")},
        "nodes": len(nodes),
        "decisions": dict(sorted(decisions.items())),
        "reasons": [
            {"decision": d, "reason": r, "count": c}
            for (d, r), c in sorted(reasons.items(), key=lambda kv: (kv[0][0], -kv[1], kv[0][1]))
        ],
        "triggers": [
            {"table": t, "built_in_run": t in built, "nodes": c}
            for t, c in sorted(triggers.items(), key=lambda kv: (-kv[1], kv[0]))
        ],
    }


def render_summary(summary: Mapping[str, Any]) -> str:
    counts = ", ".join(f"{c} {d}" for d, c in summary["decisions"].items())
    lines = [
        f"dbt-hodor explain: {summary['project']}/{summary['target']}, invocation "
        f"{summary['invocation_id']} ({_when(summary['started_at_ms'])}), "
        f"{summary['nodes']} node{'' if summary['nodes'] == 1 else 's'}: {counts}"
    ]
    width = max((len(r["reason"]) for r in summary["reasons"]), default=0)
    lines += [
        f"  {r['decision']:<5}  {r['reason']:<{width}}  {r['count']}" for r in summary["reasons"]
    ]
    triggers = summary["triggers"][:_TOP_TRIGGERS]
    if triggers:
        lines.append("changed upstreams behind the builds:")
        for t in triggers:
            name = t["table"] + (" (built this run)" if t["built_in_run"] else "")
            lines.append(f"  {name:<26}  {t['nodes']} node{'' if t['nodes'] == 1 else 's'}")
    return "\n".join(lines)


def _when(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


HEX = set("0123456789abcdef")


def _short(value: object, width: int) -> str:
    text = "none" if value is None else str(value)
    return text if len(text) <= width else text[: width - 3] + "..."


def _digest(value: object) -> str:
    """qualify:<sha256>:<reads> as qualify:<first 8 hex digits>:<first 8 hex digits>."""
    if value is None:
        return "none"
    parts = str(value).split(":")
    return ":".join(p[:8] if len(p) > 8 and all(c in HEX for c in p) else p for p in parts)


def _compare(last: object, now: object, show: Callable[[object], str]) -> str:
    if last == now:
        return f"same ({show(now)})"
    return f"{show(last)} -> {show(now)}"


def _token(value: object) -> str:
    return _short(value, 40)


def render(rep: Mapping[str, Any]) -> str:
    nodes = rep["nodes"]
    lines = [
        f"dbt-hodor explain: {rep['project']}/{rep['target']}, invocation "
        f"{rep['invocation_id']} ({_when(rep['started_at_ms'])}), "
        f"{len(nodes)} node{'' if len(nodes) == 1 else 's'}"
    ]
    for n in nodes:
        lines.append(f"{n['unique_id']}: {n['action']} ({n['reason']})")
        if n["decision"] != n["action"]:
            lines.append(f"  mode {n['mode']}: hodor decided {n['decision']}")
        if n["status"] not in _OK_STATUSES:
            lines.append(f"  status       {n['status']}")
        last = n["last_build"]
        if last is None:
            lines.append("  last build   none in the lookback window")
            continue
        lines.append(
            f"  last build   {_when(last['recorded_at_ms'])}, invocation {last['invocation_id']}"
        )
        if n["code_hash"] is None and n["config_hash"] is None:  # hodor_error: no facts
            lines.append("  hashes       not computed for this run")
            continue
        lines += [
            f"  code hash    {_compare(last['code_hash'], n['code_hash'], _digest)}",
            f"  config hash  {_compare(last['config_hash'], n['config_hash'], _digest)}",
        ]
        lines += [
            f"  upstream     {u['table']}: {_compare(u['last'], u['now'], _token)}"
            for u in n["upstreams"]
        ]
    return "\n".join(lines)

import json

from dbt_hodor.explain import matches, render, report

T0 = 1_792_465_200_000  # 2026-10-20 03:00 UTC


def test_matches_unique_id_relation_and_name(record):
    r = record(unique_id="model.p.customers.v2", node_key="c.s.customers_v2")
    assert matches(r, "model.p.customers.v2")
    assert matches(r, "c.s.customers_v2")
    assert matches(r, "customers")  # a name matches every version
    assert not matches(r, "cust")
    assert not matches(r, "p")


def test_report_pairs_each_record_with_its_last_build(record):
    last = record(invocation_id="inv-0", recorded_at_ms=T0, upstreams={"c.raw.orders": "3@1"})
    now = record(
        invocation_id="inv-1",
        recorded_at_ms=T0 + 86_400_000,
        decision="reuse",
        reason="no_new_data",
        action="reuse",
        status="reused",
        upstreams={"c.raw.orders": "3@1", "volatile:date": "2026-10-21"},
    )
    new = record(node_key="c.s.new", unique_id="model.p.new", reason="no_history")
    rep = report([now, new], {"c.s.fct": last})
    assert rep["invocation_id"] == "inv-1"
    fct, fresh = rep["nodes"]
    assert fct["last_build"]["invocation_id"] == "inv-0"
    assert fct["upstreams"] == [
        {"table": "c.raw.orders", "last": "3@1", "now": "3@1"},
        {"table": "volatile:date", "last": None, "now": "2026-10-21"},
    ]
    assert fresh["last_build"] is None
    json.dumps(rep)  # --json prints it as is


def test_report_filters_by_selector_but_keeps_the_invocation_header(record):
    a = record(unique_id="model.p.a", node_key="c.s.a", recorded_at_ms=T0)
    b = record(unique_id="model.p.b", node_key="c.s.b", recorded_at_ms=T0 + 5)
    rep = report([a, b], {}, ["b"])
    assert [n["unique_id"] for n in rep["nodes"]] == ["model.p.b"]
    assert rep["started_at_ms"] == T0


def test_render_shows_reason_hashes_and_upstream_changes(record):
    last = record(invocation_id="inv-0", recorded_at_ms=T0, upstreams={"c.raw.orders": "3@1"})
    now = record(
        invocation_id="inv-1",
        recorded_at_ms=T0 + 60_000,
        config_hash="cfg2",
        reason="config_changed",
        upstreams={"c.raw.orders": "4@2"},
    )
    text = render(report([now], {"c.s.fct": last}))
    assert text.splitlines() == [
        "dbt-hodor explain: proj/prod, invocation inv-1 (2026-10-20 03:01 UTC), 1 node",
        "model.p.fct: build (config_changed)",
        "  last build   2026-10-20 03:00 UTC, invocation inv-0",
        "  code hash    same (qualify:code)",
        "  config hash  cfg -> cfg2",
        "  upstream     c.raw.orders: 3@1 -> 4@2",
    ]


def test_render_a_failed_decision_compares_nothing(record):
    failed = record(reason="hodor_error", code_hash=None, config_hash=None, upstreams={})
    text = render(report([failed], {"c.s.fct": record(invocation_id="inv-0", recorded_at_ms=T0)}))
    assert text.splitlines()[2:] == [
        "  last build   2026-10-20 03:00 UTC, invocation inv-0",
        "  hashes       not computed for this run",
    ]


def test_render_shadow_mode_errors_and_missing_history(record):
    shadow = record(mode="shadow", decision="reuse", reason="no_new_data", status="error")
    text = render(report([shadow], {}))
    assert text.splitlines()[1:] == [
        "model.p.fct: build (no_new_data)",
        "  mode shadow: hodor decided reuse",
        "  status       error",
        "  last build   none in the lookback window",
    ]

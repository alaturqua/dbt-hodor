from dataclasses import replace

import pytest

from dbt_hodor.core.config import NodeStateConfig
from dbt_hodor.core.engine import BUILD, REUSE, Decision, NodeFacts, decide, upstream_versions
from dbt_hodor.core.metadata import UNKNOWN, DataVersion

NOW = 1_000_000 + 3_600_000  # one hour after the last build
CFG = NodeStateConfig()  # lag_tolerance 45m

UNCHANGED = {
    "model.p.stg": {"c.raw.orders": DataVersion("3")},
    "model.p.dim": {"c.s.dim": DataVersion("12")},
}


def facts(**overrides):
    base = NodeFacts(
        kind="incremental",
        code_hash="qualify:code",
        config_hash="cfg",
        hash_version="1",
        target_data_version=DataVersion("7"),
        upstream_groups=UNCHANGED,
    )
    return replace(base, **overrides)


def new_orders():
    return {**UNCHANGED, "model.p.stg": {"c.raw.orders": DataVersion("4")}}


@pytest.mark.parametrize(
    ("node", "last", "cfg", "now", "expected"),
    [
        ("unchanged", {}, CFG, NOW, (REUSE, "no_new_data")),
        ("full_refresh", {}, CFG, NOW, (BUILD, "full_refresh")),
        ("unsupported", {}, CFG, NOW, (BUILD, "unsupported")),
        ("bad_kind", {}, CFG, NOW, (BUILD, "unsupported")),
        ("unchanged", None, CFG, NOW, (BUILD, "no_history")),
        ("missing", {}, CFG, NOW, (BUILD, "relation_missing")),
        ("code", {}, CFG, NOW, (BUILD, "code_changed")),
        ("unchanged", {"hash_version": "0"}, CFG, NOW, (BUILD, "code_changed")),
        ("config", {}, CFG, NOW, (BUILD, "config_changed")),
        ("target_written", {}, CFG, NOW, (BUILD, "target_modified")),
        ("target_unknown", {}, CFG, NOW, (BUILD, "target_modified")),
        ("star_view", {}, CFG, NOW, (BUILD, "select_star_view")),
        ("view_new_data", {}, CFG, NOW, (REUSE, "view_unchanged")),
        ("unknown_group", {}, CFG, NOW, (BUILD, "unknown_upstream")),
        ("unknown_table", {}, CFG, NOW, (BUILD, "unknown_upstream")),
        ("new_data", {}, CFG, NOW, (BUILD, "new_upstream_data")),
        ("new_data", {}, CFG, 1_000_000 + 60_000, (REUSE, "within_lag_tolerance")),
        ("new_data", {"built_at_ms": None}, CFG, NOW, (BUILD, "new_upstream_data")),
        ("new_table", {}, CFG, NOW, (BUILD, "new_upstream_data")),
    ],
)
def test_decision_rules(record, node, last, cfg, now, expected):
    nodes = {
        "unchanged": facts(),
        "full_refresh": facts(full_refresh=True, code_hash="other"),
        "unsupported": facts(kind="unsupported"),
        "bad_kind": facts(kind="materialized_view"),
        "missing": facts(relation_exists=False),
        "code": facts(code_hash="qualify:new"),
        "config": facts(config_hash="cfg2"),
        "target_written": facts(target_data_version=DataVersion("8")),
        "target_unknown": facts(target_data_version=UNKNOWN),
        "star_view": facts(kind="view", select_star_view=True),
        "view_new_data": facts(kind="view", upstream_groups={"x": None}),
        "unknown_group": facts(upstream_groups={**UNCHANGED, "ext": None}),
        "unknown_table": facts(upstream_groups={**UNCHANGED, "ext": {"/a": UNKNOWN}}),
        "new_data": facts(upstream_groups=new_orders()),
        "new_table": facts(upstream_groups={**UNCHANGED, "ext": {"c.ext.rates": DataVersion("1")}}),
    }
    last_record = None if last is None else record(**last)
    assert decide(nodes[node], last_record, cfg, now) == Decision(*expected)


def test_seeds_have_no_upstream_but_guard_their_target(record):
    seed = facts(kind="seed", upstream_groups={})
    assert decide(seed, record(), CFG, NOW) == Decision(REUSE, "no_new_data")
    written = replace(seed, target_data_version=DataVersion("8"))
    assert decide(written, record(), CFG, NOW) == Decision(BUILD, "target_modified")
    assert decide(replace(seed, code_hash="csv2"), record(), CFG, NOW).reason == "code_changed"


def test_snapshots_follow_the_table_rules(record):
    snapshot = facts(kind="snapshot")
    assert decide(snapshot, record(), CFG, NOW) == Decision(REUSE, "no_new_data")
    new_data = replace(snapshot, upstream_groups=new_orders())
    assert decide(new_data, record(), CFG, NOW) == Decision(BUILD, "new_upstream_data")
    written = replace(snapshot, target_data_version=DataVersion("8"))
    assert decide(written, record(), CFG, NOW) == Decision(BUILD, "target_modified")


def test_require_fresh_data_from_all(record):
    cfg = NodeStateConfig(require_fresh_data_from="all", lag_tolerance_s=0)
    one_changed = facts(upstream_groups=new_orders())
    assert decide(one_changed, record(), cfg, NOW) == Decision(REUSE, "no_new_data")
    both = {
        "model.p.stg": {"c.raw.orders": DataVersion("4")},
        "model.p.dim": {"c.s.dim": DataVersion("13")},
    }
    assert decide(facts(upstream_groups=both), record(), cfg, NOW) == Decision(
        BUILD, "new_upstream_data"
    )
    assert decide(facts(upstream_groups={}), record(), cfg, NOW) == Decision(REUSE, "no_new_data")


def test_compare_unrendered_code_needs_both_hashes_changed(record):
    cfg = NodeStateConfig(compare_unrendered_code=True)
    last = record(unrendered_hash="u1")
    only_rendered = facts(code_hash="qualify:new", unrendered_hash="u1")
    assert decide(only_rendered, last, cfg, NOW).reason == "no_new_data"
    both = facts(code_hash="qualify:new", unrendered_hash="u2")
    assert decide(both, last, cfg, NOW).reason == "code_changed"
    no_unrendered = facts(code_hash="qualify:new")
    assert decide(no_unrendered, last, cfg, NOW).reason == "code_changed"


def test_tests_ignore_lag_tolerance_and_need_a_result(record):
    test = facts(kind="test", target_data_version=UNKNOWN, upstream_groups=new_orders())
    with_result = record(test_result={"status": "pass", "failures": 0})
    assert decide(test, with_result, CFG, 1_000_000 + 1).reason == "new_upstream_data"
    assert decide(test, record(), CFG, NOW).reason == "no_history"
    unchanged = facts(kind="test", target_data_version=UNKNOWN)
    assert decide(unchanged, with_result, CFG, NOW) == Decision(REUSE, "no_new_data")


def test_upstream_versions_flattens_known_groups():
    groups = {**UNCHANGED, "ext": None, "path": {"/a": UNKNOWN}}
    assert upstream_versions(facts(upstream_groups=groups)) == {
        "c.raw.orders": "3",
        "c.s.dim": "12",
        "/a": None,
    }


TODAY = {"c.raw.orders": "3", "c.s.dim": "12", "volatile:date": "2026-10-21"}


def volatile(day):
    return facts(
        upstream_groups={**UNCHANGED, "volatile:date": {"volatile:date": DataVersion(day)}}
    )


def test_volatile_values_are_upstreams(record):
    last = record(upstreams=TODAY)
    assert decide(volatile("2026-10-21"), last, CFG, NOW) == Decision(REUSE, "no_new_data")
    assert decide(volatile("2026-10-22"), last, CFG, NOW) == Decision(BUILD, "volatile_sql")
    soon = 1_000_000 + 60_000  # inside lag_tolerance
    assert decide(volatile("2026-10-22"), last, CFG, soon) == Decision(
        REUSE, "within_lag_tolerance"
    )
    both = replace(
        volatile("2026-10-22"),
        upstream_groups={**volatile("2026-10-22").upstream_groups, **new_orders()},
    )
    assert decide(both, last, CFG, NOW) == Decision(BUILD, "new_upstream_data")
    unknown = facts(
        upstream_groups={**UNCHANGED, "volatile:session": {"volatile:session": UNKNOWN}}
    )
    assert decide(unknown, last, CFG, NOW) == Decision(BUILD, "unknown_upstream")


def test_a_new_date_builds_even_with_require_fresh_data_from_all(record):
    cfg = NodeStateConfig(require_fresh_data_from="all", lag_tolerance_s=0)
    assert decide(volatile("2026-10-22"), record(upstreams=TODAY), cfg, NOW) == Decision(
        BUILD, "volatile_sql"
    )


def test_a_changed_view_in_between_builds_whatever_the_data_and_tolerance(record):
    view = {"code:model.p.int": {"code:model.p.int": DataVersion("sum1")}}
    rec = record(upstreams={"c.raw.orders": "3", "c.s.dim": "12", "code:model.p.int": "sum1"})
    same = facts(upstream_groups={**UNCHANGED, **view})
    assert decide(same, rec, CFG, NOW) == Decision(REUSE, "no_new_data")
    edited = {"code:model.p.int": {"code:model.p.int": DataVersion("sum2")}}
    changed = facts(upstream_groups={**UNCHANGED, **edited})
    lenient = NodeStateConfig(require_fresh_data_from="all", lag_tolerance_s=10**9)
    assert decide(changed, rec, lenient, NOW) == Decision(BUILD, "upstream_code_changed")


EPOCH_LAST = {
    "built_at_ms": 1793483538600,
    "data_version": "1793483539000",
    "upstreams": {"c.raw.orders": "1793483538500", "c.s.dim": "12"},
}


def epoch_facts(orders, **overrides):
    base = {
        "epoch_versions": True,
        "target_data_version": DataVersion("1793483539000"),
        "upstream_groups": {
            "model.p.stg": {"c.raw.orders": DataVersion(orders)},
            "model.p.dim": {"c.s.dim": DataVersion("12")},
        },
    }
    return facts(**{**base, **overrides})


# (last build's start, last build's version, orders token then, orders token now, now, expected)
# epochs are whole seconds: an upstream token unchanged since the last build, written in the
# same second the build started or later, may have been written again after the build read it
B = 1_793_483_530_000  # a build that started at B and wrote its table at B + 9 s
LAG = NodeStateConfig(lag_tolerance_s=2700)


@pytest.mark.parametrize(
    ("started", "version", "then", "now_token", "now", "expected"),
    [
        # beside the build's own version: the old rule
        (B + 8_600, B + 9_000, B + 8_500, B + 8_500, NOW, (BUILD, "same_second_read")),
        # written during a long build, after it started
        (B, B + 9_000, B + 5_000, B + 5_000, NOW, (BUILD, "same_second_read")),
        # one second before the start: may still be in the same second
        (B, B + 9_000, B - 1_000, B - 1_000, NOW, (BUILD, "same_second_read")),
        # more than a second before the start: read before any later write
        (B, B + 9_000, B - 1_001, B - 1_001, NOW, (REUSE, "no_new_data")),
        # no start time: only the build's own version counts
        (None, B + 9_000, B + 5_000, B + 5_000, NOW, (REUSE, "no_new_data")),
        # a changed token is new data, and the lag tolerance still applies to it
        (B, B + 9_000, B + 5_000, B + 6_000, B + 60_000, (REUSE, "within_lag_tolerance")),
    ],
)
def test_same_second_read_against_the_build_start(
    record, started, version, then, now_token, now, expected
):
    last = record(
        built_at_ms=started,
        data_version=str(version),
        upstreams={"c.raw.orders": str(then), "c.s.dim": "12"},
    )
    node = epoch_facts(str(now_token), target_data_version=DataVersion(str(version)))
    assert decide(node, last, LAG, now) == Decision(*expected)


def test_same_second_read_builds_when_an_upstream_landed_beside_the_last_build(record):
    last = record(**EPOCH_LAST)
    node = epoch_facts("1793483538500")
    assert decide(node, last, CFG, NOW) == Decision(BUILD, "same_second_read")


def test_same_second_read_is_off_for_v1_versions(record):
    # same tokens, epoch_versions False: Delta versions are not times
    last = record(**EPOCH_LAST)
    node = replace(epoch_facts("1793483538500"), epoch_versions=False)
    assert decide(node, last, CFG, NOW) == Decision(REUSE, "no_new_data")


def test_an_upstream_two_seconds_before_the_build_reuses(record):
    last = record(
        built_at_ms=1793483538100,
        data_version="1793483539000",
        upstreams={"c.raw.orders": "1793483537000", "c.s.dim": "12"},
    )
    node = epoch_facts("1793483537000")
    assert decide(node, last, CFG, NOW) == Decision(REUSE, "no_new_data")


def test_an_upstream_exactly_one_second_away_does_not_count_as_the_same_second(record):
    last = record(
        built_at_ms=1793483539100,  # started in the second its table was written
        data_version="1793483539000",
        upstreams={"c.raw.orders": "1793483538000", "c.s.dim": "12"},
    )
    node = epoch_facts("1793483538000")
    assert decide(node, last, CFG, NOW) == Decision(REUSE, "no_new_data")


def test_epoch_versions_with_a_non_integer_token_builds(record):
    last = record(**EPOCH_LAST)
    node = epoch_facts("not-a-number")
    assert decide(node, last, CFG, NOW) == Decision(BUILD, "unknown_upstream")


def test_epoch_versions_with_a_non_integer_last_data_version_builds(record):
    last = record(**{**EPOCH_LAST, "data_version": "abc"})
    node = epoch_facts("1793483538500", target_data_version=DataVersion("abc"))
    assert decide(node, last, CFG, NOW) == Decision(BUILD, "unknown_upstream")

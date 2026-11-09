from __future__ import annotations

from dataclasses import replace

import pytest

from dbt_hodor.core.clone import PRE_CLONED, may_clone, node_upstreams, relation_ids
from dbt_hodor.core.config import BaseTarget
from dbt_hodor.core.engine import BUILD, REUSE, Decision, NodeFacts
from dbt_hodor.core.metadata import DataVersion
from dbt_hodor.core.records import Record


def _record(uid="model.p.a", node_hashes=None, **kw):
    fields = dict(
        record_id="r",
        invocation_id="i",
        recorded_at_ms=1,
        project_name="p",
        target_name="prod",
        node_key=uid,
        unique_id=uid,
        resource_type="model",
        mode="v1",
        decision="build",
        reason="x",
        action="build",
        status="success",
        code_hash="c",
        config_hash="cfg",
        hash_version="v",
        node_hashes=node_hashes,
    )
    fields.update(kw)
    return Record(**fields)


FACTS = NodeFacts(
    kind="table",
    code_hash="c",
    config_hash="cfg",
    hash_version="v",
    upstream_groups={"db.raw.t": {"db.raw.t": DataVersion("1")}},
)
BUILD_DECISION = Decision(BUILD, "new_upstream_data")
BASE_CFG = BaseTarget("prod", True, False)
UP = {"source.p.raw.t": "1"}


def _clone(**over):
    args = dict(
        facts=FACTS,
        decision=BUILD_DECISION,
        base=_record(),
        base_cfg=BASE_CFG,
        current_upstreams_by_id=UP,
        base_upstreams_by_id=UP,
        base_is_current=False,
    )
    args.update(over)
    return may_clone(**args)


def test_all_checks_pass():
    assert _clone() == Decision(BUILD, "cloned_from_base")


@pytest.mark.parametrize(
    "over",
    [
        {"base_cfg": BaseTarget("prod", False, False)},
        {"facts": replace(FACTS, kind="view")},
        {"facts": replace(FACTS, kind="seed")},
        {"facts": replace(FACTS, kind="snapshot")},
        {"facts": replace(FACTS, kind="test")},
        {"decision": Decision(REUSE, "no_new_data")},
        {"base": None},
        {"base": _record(status="error")},
        {"base": _record(action="clone")},
        {"base": _record(config_hash="other")},
        {"base": _record(hash_version="v0")},
        {"base": _record(code_hash="other")},
        {"current_upstreams_by_id": None},
        {"base_upstreams_by_id": None},
        {"base_upstreams_by_id": {"source.p.raw.t": "2"}},
        {"base_upstreams_by_id": {}},
        {"current_upstreams_by_id": {**UP, "model.p.b": "1"}},
        {"base_is_current": True},
        {"decision": Decision(BUILD, "full_refresh")},
        {"decision": Decision(BUILD, "hodor_error")},
        {"volatile_stamps": frozenset({"session"})},
        {"volatile": frozenset({"session"})},
        {"volatile_stamps": frozenset({"date", "session"})},
        {
            "current_upstreams_by_id": {"source.p.raw.t": None},
            "base_upstreams_by_id": {"source.p.raw.t": None},
        },
        {
            "facts": replace(
                FACTS,
                upstream_groups={"volatile:date": {"volatile:date": DataVersion("d")}},
            )
        },
        {"facts": replace(FACTS, upstream_groups={"opaque": None})},
    ],
    ids=lambda o: ",".join(o),
)
def test_no_clone(over):
    assert _clone(**over) is None


def test_no_clone_from_own_target():
    assert _clone(base_is_current=True) is None


@pytest.mark.parametrize("stamps", [frozenset({"date"}), frozenset({"always"})])
def test_date_and_always_stamps_allow_clone(stamps):
    assert _clone(volatile_stamps=stamps) == Decision(BUILD, "cloned_from_base")


def test_carry_record_is_usable():
    assert _clone(base=_record(action="carry")) is not None


def test_dev_built_parent_blocks_clone():
    # a parent built in dev has its own table and version; prod's differs
    assert (
        _clone(current_upstreams_by_id={"model.p.b": "9"}, base_upstreams_by_id={"model.p.b": "3"})
        is None
    )


def test_pre_clone_ignores_code_for_incremental():
    facts = replace(FACTS, kind="incremental", code_hash="new")
    cfg = BaseTarget("prod", True, True)
    assert _clone(facts=facts, base_cfg=cfg) == Decision(BUILD, PRE_CLONED)
    assert PRE_CLONED == "pre_cloned_from_base"
    assert _clone(facts=facts) is None  # pre_clone off


def test_pre_clone_with_same_code_is_plain_clone():
    facts = replace(FACTS, kind="incremental")
    cfg = BaseTarget("prod", True, True)
    assert _clone(facts=facts, base_cfg=cfg) == Decision(BUILD, "cloned_from_base")


def test_pre_clone_still_checks_upstreams_and_config():
    facts = replace(FACTS, kind="incremental", code_hash="new")
    cfg = BaseTarget("prod", True, True)
    assert _clone(facts=facts, base_cfg=cfg, base_upstreams_by_id={"source.p.raw.t": "2"}) is None
    assert _clone(facts=facts, base_cfg=cfg, base=_record(config_hash="o")) is None


def test_pre_clone_never_for_table():
    # a table with changed code is rebuilt in full: a clone would replace the build
    cfg = BaseTarget("prod", True, True)
    assert _clone(facts=replace(FACTS, code_hash="new"), base_cfg=cfg) is None


def test_relation_ids_lowercases_and_drops_clashes():
    a = {"model.p.a": _record("model.p.a", {"relation": "`Db`.`S`.`A`"})}
    b = {
        "model.p.b": _record("model.p.b", {"relation": "db.s.b"}),
        "model.p.c": _record("model.p.c", {"relation": "db.s.b"}),
        "model.p.d": _record("model.p.d", None),
    }
    assert relation_ids(a, b) == {"db.s.a": "model.p.a"}


def test_relation_ids_same_node_in_both_sets_is_kept():
    r = {"model.p.a": _record("model.p.a", {"relation": "db.s.a"})}
    assert relation_ids(r, r) == {"db.s.a": "model.p.a"}


def test_node_upstreams_maps_tables_and_keeps_code_keys():
    ids = {"db.s.a": "model.p.a"}
    ups = {"db.s.a": "1", "code:model.p.v": "h"}
    assert node_upstreams(ups, ids) == {"model.p.a": "1", "code:model.p.v": "h"}


@pytest.mark.parametrize("key", ["db.s.unknown", "volatile:date", "delta:/x"])
def test_node_upstreams_unmapped_is_none(key):
    assert node_upstreams({key: "1"}, {"db.s.a": "model.p.a"}) is None


def test_node_upstreams_two_tables_of_one_node_with_other_versions_is_none():
    ids = {"db.dev.a": "model.p.a", "db.prod.a": "model.p.a"}
    assert node_upstreams({"db.dev.a": "1", "db.prod.a": "2"}, ids) is None
    assert node_upstreams({"db.dev.a": "1", "db.prod.a": "1"}, ids) == {"model.p.a": "1"}

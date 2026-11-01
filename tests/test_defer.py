from __future__ import annotations

from dataclasses import replace

import pytest

from dbt_hodor.defer import deferred_relations, split_relation
from dbt_hodor.records import Record


def _record(uid="model.p.a", node_hashes=None, action="build", status="success"):
    return Record(
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
        action=action,
        status=status,
        node_hashes=node_hashes,
    )


def _defer(base, unselected=("model.p.a",), base_target="prod", current_target="dev"):
    return deferred_relations(unselected, base, base_target, current_target)


def test_built_node_returns_its_relation():
    base = {"model.p.a": _record(node_hashes={"relation": "db.s.a"})}
    assert _defer(base) == {"model.p.a": "db.s.a"}


def test_case_is_kept_verbatim():
    base = {"model.p.a": _record(node_hashes={"relation": "`Cat`.`Sch`.`T`"})}
    assert _defer(base) == {"model.p.a": "`Cat`.`Sch`.`T`"}


def test_no_record_is_absent():
    assert _defer({}) == {}


@pytest.mark.parametrize("hashes", [None, "x", {}, {"relation": ""}, {"relation": 3}])
def test_record_without_usable_relation_is_absent(hashes):
    assert _defer({"model.p.a": _record(node_hashes=hashes)}) == {}


@pytest.mark.parametrize(
    ("action", "status", "kept"),
    [
        ("build", "error", False),
        ("build", "skipped", False),
        ("reuse", "success", False),
        ("carry", "success", True),
        ("clone", "success", True),
        ("build", "warn", True),
    ],
)
def test_only_successful_builds_count(action, status, kept):
    base = {"model.p.a": _record(node_hashes={"relation": "db.s.a"}, action=action, status=status)}
    assert _defer(base) == ({"model.p.a": "db.s.a"} if kept else {})


def test_defer_to_self_returns_nothing():
    base = {"model.p.a": _record(node_hashes={"relation": "db.s.a"})}
    assert _defer(base, base_target="prod", current_target="prod") == {}


def test_only_unselected_ids_are_returned():
    base = {
        "model.p.a": _record(node_hashes={"relation": "db.s.a"}),
        "model.p.b": _record("model.p.b", {"relation": "db.s.b"}),
    }
    assert _defer(base, unselected=["model.p.b"]) == {"model.p.b": "db.s.b"}


@pytest.mark.parametrize(
    ("relation", "quote", "parts"),
    [
        ('"db"."s"."a"', '"', ["db", "s", "a"]),
        ('"Db"."my.schema"."A b"', '"', ["Db", "my.schema", "A b"]),
        ("db.s.a", '"', ["db", "s", "a"]),
        ('"s".a', '"', ["s", "a"]),
        ("`cat`.`sch`.`t`", "`", ["cat", "sch", "t"]),
        ("a", '"', ["a"]),
        ('"db"."s', '"', None),
        ('"db""s"', '"', None),
        ('db."s".a"', '"', None),
        ("db..a", '"', None),
        ("db.s.", '"', None),
        ('"".s.a', '"', None),
        ("", '"', None),
    ],
)
def test_split_relation(relation, quote, parts):
    assert split_relation(relation, quote) == parts


def _newest(relation, record_id="n", action="build", status="error"):
    rec = _record(node_hashes={"relation": relation}, action=action, status=status)
    return {"model.p.a": replace(rec, record_id=record_id)}


@pytest.mark.parametrize(
    ("newest", "kept"),
    [
        (_newest('"db"."s"."a_new"'), False),  # failed first build under a new name
        (_newest('"DB"."S"."A"'), True),  # failed rebuild of the same table
        (_newest('"db"."s"."a"', action="reuse", status="reused"), True),
        (_newest(None), False),  # the newest record does not say where it wrote
        ({}, True),
    ],
)
def test_a_newer_record_with_another_relation_is_not_deferred(newest, kept):
    base = {"model.p.a": _record(node_hashes={"relation": '"db"."s"."a"'})}
    out = deferred_relations(("model.p.a",), base, "prod", "dev", newest)
    assert out == ({"model.p.a": '"db"."s"."a"'} if kept else {})


def test_the_success_itself_as_the_newest_record_is_deferred():
    base = {"model.p.a": _record(node_hashes={"relation": '"db"."s"."a"'})}
    out = deferred_relations(("model.p.a",), base, "prod", "dev", dict(base))
    assert out == {"model.p.a": '"db"."s"."a"'}

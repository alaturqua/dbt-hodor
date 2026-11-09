from __future__ import annotations

import pytest

from dbt_hodor.core.records import Record
from dbt_hodor.core.selection import CRITERIA, SelectionError, select

HASH_KEYS = ("body", "configs", "contract", "macros", "relation_hash", "persisted_descriptions")
SUB = {
    "modified.body": "body",
    "modified.configs": "configs",
    "modified.contract": "contract",
    "modified.macros": "macros",
    "modified.relation": "relation_hash",
    "modified.persisted_descriptions": "persisted_descriptions",
}


def _hashes(**over):
    h = {k: f"{k}-1" for k in HASH_KEYS}
    h["persisted_descriptions"] = None
    h.update(over)
    return h


def _record(node_hashes, uid="model.p.a"):
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
        action="build",
        status="success",
        node_hashes=node_hashes,
    )


def _base(**stored):
    return {"model.p.a": _record({"engine": "v1", "relation": "db.s.a", **_hashes(), **stored})}


ALL = {"new", "modified", "unmodified", "old", *SUB}
UID = "model.p.a"


@pytest.mark.parametrize(
    ("current", "base", "expected"),
    [
        pytest.param(_hashes(), {}, {"new", "modified", *SUB}, id="no record"),
        pytest.param(_hashes(), _base(), {"unmodified", "old"}, id="same hashes"),
        pytest.param(
            _hashes(body="changed"),
            _base(),
            {"modified", "modified.body", "old"},
            id="body differs",
        ),
        pytest.param(
            _hashes(),
            {UID: _record(None)},
            {"modified", "old", *SUB},
            id="no node_hashes",
        ),
        pytest.param(
            _hashes(),
            {UID: _record({"engine": "v2", "relation": "r", **_hashes()})},
            {"modified", "old", *SUB},
            id="other engine",
        ),
        pytest.param(
            _hashes(persisted_descriptions=None),
            _base(persisted_descriptions=None),
            {"unmodified", "old"},
            id="None equals None",
        ),
        pytest.param(
            _hashes(persisted_descriptions="d"),
            _base(persisted_descriptions=None),
            {"modified", "modified.persisted_descriptions", "old"},
            id="None vs value",
        ),
    ],
)
def test_selection_rules(current, base, expected):
    for criterion in CRITERIA:
        got = select(criterion, {UID: current}, base, "v1")
        assert got == (frozenset({UID}) if criterion in expected else frozenset()), criterion


def test_result_only_has_current_ids():
    base = {**_base(), "model.p.gone": _record(_hashes(), "model.p.gone")}
    assert select("old", {UID: _hashes()}, base, "v1") == {UID}


def test_unknown_criterion_raises():
    with pytest.raises(SelectionError):
        select("modified.nope", {}, {}, "v1")

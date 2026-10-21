from dataclasses import replace

import pytest

from dbt_hodor.records import Record


def make_record(**overrides) -> Record:
    base = Record(
        record_id="r-1",
        invocation_id="inv-1",
        recorded_at_ms=1_000_000,
        project_name="proj",
        target_name="prod",
        node_key="c.s.fct",
        unique_id="model.p.fct",
        resource_type="model",
        mode="on",
        decision="build",
        reason="new_upstream_data",
        action="build",
        status="success",
        text_hash="th",
        code_hash="qualify:code",
        config_hash="cfg",
        hash_version="1",
        sql_facts={"code_hash": "qualify:code", "select_star": False},
        data_version="7",
        data_changed_at_ms=990_000,
        built_at_ms=1_000_000,
        upstreams={"c.raw.orders": "3", "c.s.dim": "12"},
        hodor_version="0.1.0",
    )
    return replace(base, **overrides)


@pytest.fixture
def record():
    return make_record

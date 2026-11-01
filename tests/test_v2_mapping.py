import importlib
import json
from pathlib import Path

import pytest

pytest.importorskip("grpc")
pytest.importorskip("google.protobuf")

from google.protobuf import json_format  # noqa: E402

import dbt_hodor.v2 as v2  # noqa: E402
from dbt_hodor.clone import CLONED, PRE_CLONED  # noqa: E402
from dbt_hodor.config import NodeStateConfig  # noqa: E402
from dbt_hodor.engine import BUILD, REUSE, Decision, decide  # noqa: E402
from dbt_hodor.hashing import HASH_VERSION, SqlAnalysis, analyze_sql, text_hash  # noqa: E402
from dbt_hodor.metadata import UNKNOWN, DataVersion  # noqa: E402
from dbt_hodor.v2 import mapping  # noqa: E402
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache import shared_pb2  # noqa: E402
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    selector_service_pb2,
    sql_service_pb2,
)

SERVICES = [
    "client_telemetry_service",
    "client_validation_service",
    "clone_service",
    "execution_service",
    "explain_service",
    "health_service",
    "selector_service",
    "sql_service",
]
BASE = "dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services"


def test_version_constants():
    assert v2.SUPPORTED_CLIENT_VERSIONS == frozenset({"2.0.8"})
    assert v2.PROTO_TAG == "v2.0.8"


def test_stubs_import_and_have_every_service():
    for name in SERVICES:
        importlib.import_module(f"{BASE}.{name}_pb2")
        importlib.import_module(f"{BASE}.{name}_pb2_grpc")
    grpc_mod = importlib.import_module(f"{BASE}.sql_service_pb2_grpc")
    assert hasattr(grpc_mod.SQLServicer, "SubmitEnrichedSQL")


# Requests a dbt-oss 2.0.8 run sent, with names replaced (catalog cat, schema sch).
FIXTURE = Path(__file__).parent / "fixtures" / "v2_requests.jsonl"
CASES = {
    row["case"]: row for row in map(json.loads, FIXTURE.read_text(encoding="utf-8").splitlines())
}
TARGET_EPOCH = "1793483505000"  # stg_orders, the target of the target_present request
RAW_EPOCH = "1793483539000"  # raw_orders, its upstream
# a last build that started more than a second after raw_orders was written
STARTED_MS = int(RAW_EPOCH) + 2000
Response = sql_service_pb2.SubmitSQLResponse


def _sql_req(case="target_present", **changes):
    req = json_format.ParseDict(CASES[case]["request"], sql_service_pb2.SubmitEnrichedSQLRequest())
    for name, value in changes.items():
        if name in ("tables", "query_dependencies"):
            req.ClearField(name)
            getattr(req, name).extend(value)
        elif name == "semantic_extras":
            req.semantic_extras.clear()
            req.semantic_extras.update(value)
        else:
            setattr(req, name, value)
    return req


def _seed_req():
    return json_format.ParseDict(CASES["seed"]["request"], sql_service_pb2.SubmitValuesRequest())


def _facts(req=None, last=None, relations=None):
    facts, _ = mapping.node_facts(req or _sql_req(), last, relations or {})
    return facts


def _table(name, epoch=None):
    if epoch is None:
        return shared_pb2.TableModifiedInfo(name=name)
    return shared_pb2.TableModifiedInfo(name=name, last_modified_epoch=epoch)


def test_normalize():
    assert mapping.normalize('`Cat`.`Sch`."T"') == "cat.sch.t"


@pytest.mark.parametrize(
    ("execution_type", "kind"),
    [
        ("FULL", "table"),
        ("MERGE", "incremental"),
        ("APPEND", "incremental"),
        ("INSERT_OVERWRITE", "incremental"),
        ("DELETE_INSERT", "incremental"),
        ("SNAPSHOT", "snapshot"),
        ("MICROBATCH", "unsupported"),
        ("VIEW", "unsupported"),
        ("DBT_DATA_TEST", "unsupported"),
        ("DBT_CUSTOM", "unsupported"),
        ("UNSPECIFIED", "unsupported"),
    ],
)
def test_kind_from_execution_type(execution_type, kind):
    req = _sql_req(execution_type=shared_pb2.ModelExecutionType.Value(execution_type))
    assert _facts(req).kind == kind


def test_target_in_tables_exists_with_its_epoch():
    facts = _facts()
    assert facts.relation_exists
    assert facts.target_data_version.token == TARGET_EPOCH
    assert facts.epoch_versions


def test_target_absent_does_not_exist():
    facts = _facts(_sql_req("target_absent"))
    assert not facts.relation_exists
    assert not facts.target_data_version.known


def test_target_without_epoch_is_unknown():
    facts = _facts(_sql_req(tables=[_table("`cat`.`sch`.`stg_orders`")]))
    assert facts.relation_exists
    assert facts.target_data_version == UNKNOWN


def test_upstream_epochs_as_tokens_target_excluded():
    groups = _facts().upstream_groups
    assert set(groups) == {"cat.sch.raw_orders"}
    assert groups["cat.sch.raw_orders"]["cat.sch.raw_orders"].token == RAW_EPOCH


def test_upstream_without_epoch_is_unknown():
    tables = [_table("`cat`.`sch`.`raw_orders`"), _table("`cat`.`sch`.`stg_orders`", 1)]
    facts = _facts(_sql_req(tables=tables))
    assert facts.upstream_groups["cat.sch.raw_orders"] == {"cat.sch.raw_orders": UNKNOWN}


def test_backticked_names_match_records_and_keep_case():
    req = _sql_req(
        target_table="`Cat`.`Sch`.`Stg_Orders`",
        sql="select id from `Cat`.`Sch`.`Raw_Orders`",
        tables=[_table("`Cat`.`Sch`.`Raw_Orders`", 7), _table("`Cat`.`Sch`.`Stg_Orders`", 9)],
    )
    facts = _facts(req)
    assert set(facts.upstream_groups) == {"cat.sch.raw_orders"}
    assert facts.target_data_version.token == "9"
    hashes = mapping.v2_node_hashes(req.dbt_node_state, req.target_table)
    assert hashes["relation"] == "`Cat`.`Sch`.`Stg_Orders`"


def test_reuse_when_nothing_changed(record):
    facts = _facts()
    last = record(
        code_hash=facts.code_hash,
        config_hash=facts.config_hash,
        hash_version=facts.hash_version,
        data_version=TARGET_EPOCH,
        built_at_ms=STARTED_MS,
        upstreams={"cat.sch.raw_orders": RAW_EPOCH},
    )
    assert decide(facts, last, NodeStateConfig(), now_ms=0) == Decision(REUSE, "no_new_data")


def test_query_dependency_is_a_code_group():
    view_sql = "select id, amount from `cat`.`sch`.`raw_orders`"
    dep = shared_pb2.QueryDependency(name="`cat`.`sch`.`v_orders`", query=view_sql)
    sql = "select id, amount * 2 as amount from `cat`.`sch`.`v_orders`"
    facts = _facts(_sql_req(sql=sql, query_dependencies=[dep]))
    key = "code:cat.sch.v_orders"
    assert facts.upstream_groups[key] == {key: DataVersion(analyze_sql(view_sql).code_hash)}
    # the view a node reads through is a known input, not an unlisted one
    assert None not in facts.upstream_groups.values()


def test_query_dependency_keyed_by_unique_id_when_known():
    dep = shared_pb2.QueryDependency(name="`cat`.`sch`.`v_orders`", query="select 1")
    relations = {"cat.sch.v_orders": "model.spike.v_orders"}
    facts = _facts(_sql_req(query_dependencies=[dep]), relations=relations)
    assert "code:model.spike.v_orders" in facts.upstream_groups


def test_table_read_but_not_listed_is_unknown():
    sql = "select * from `cat`.`sch`.`raw_orders` join `cat`.`sch`.`other` using (id)"
    assert None in _facts(_sql_req(sql=sql)).upstream_groups.values()


def test_volatile_sql_is_unknown():
    sql = "select id from `cat`.`sch`.`raw_orders` where d = current_date()"
    facts = _facts(_sql_req(sql=sql))
    assert facts.upstream_groups["volatile:date"] == {"volatile:date": UNKNOWN}


def test_config_hash_changes_with_semantic_extras():
    base = _facts().config_hash
    assert _facts(_sql_req(semantic_extras={"lookback": "2"})).config_hash != base
    req = _sql_req()
    req.dbt_node_state.node_contract_hash = "contract-2"
    assert _facts(req).config_hash != base
    req = _sql_req()
    req.dbt_node_state.node_configs_hash = "configs-2"
    assert _facts(req).config_hash != base


def test_missing_configs_hash_is_unsupported():
    req = _sql_req()
    req.dbt_node_state.ClearField("node_configs_hash")
    assert _facts(req).kind == "unsupported"
    req = _sql_req()
    req.ClearField("dbt_node_state")
    assert _facts(req).kind == "unsupported"


def test_missing_target_or_unknown_dialect_is_unsupported():
    req = _sql_req()
    req.ClearField("target_table")
    assert _facts(req).kind == "unsupported"
    assert _facts(_sql_req(dialect="nosuchdialect")).kind == "unsupported"


def test_hash_version_is_v2():
    assert _facts().hash_version.startswith("v2-")
    assert mapping.V2_HASH_VERSION == "v2-" + HASH_VERSION


def test_analysis_cached_only_from_a_v2_record(record):
    req = _sql_req()
    facts, analysis = mapping.node_facts(req, None, {})
    assert analysis.code_hash == facts.code_hash
    assert analysis.tables == frozenset({"cat.sch.raw_orders"})
    cached = SqlAnalysis("cached", False, frozenset({"cat.sch.raw_orders"}))
    last = record(
        hash_version=mapping.V2_HASH_VERSION,
        text_hash=text_hash(req.sql),
        sql_facts=cached.to_json(),
    )
    assert _facts(req, last).code_hash == "cached"
    assert (
        _facts(
            req,
            record(
                hash_version=HASH_VERSION, text_hash=text_hash(req.sql), sql_facts=cached.to_json()
            ),
        ).code_hash
        == facts.code_hash
    )


def test_seed_facts(record):
    last = record(data_version="42", hash_version=mapping.V2_HASH_VERSION)
    facts = mapping.seed_facts(_seed_req(), last)
    assert facts.kind == "seed"
    assert facts.code_hash == "values-raw-orders"
    assert facts.hash_version == "v2-" + HASH_VERSION
    assert facts.relation_exists
    assert facts.target_data_version.token == "42"
    assert not facts.upstream_groups
    new = mapping.seed_facts(_seed_req(), None)
    assert not new.relation_exists
    assert not new.target_data_version.known


def test_seed_config_hash_changes_with_semantic_extras():
    req = _seed_req()
    base = mapping.seed_facts(req, None).config_hash
    req.semantic_extras["delimiter"] = ";"
    assert mapping.seed_facts(req, None).config_hash != base


def test_seed_without_values_hash_is_unsupported():
    req = _seed_req()
    req.values_hash = ""
    assert mapping.seed_facts(req, None).kind == "unsupported"


def test_v2_node_hashes_from_state_and_data():
    hashes = mapping.v2_node_hashes(_sql_req().dbt_node_state, "`cat`.`sch`.`stg_orders`")
    assert hashes == {
        "engine": "v2",
        "relation": "`cat`.`sch`.`stg_orders`",
        "body": "body-stg-orders",
        "configs": "configs-1",
        "contract": "contract-1",
        "macros": None,
        "persisted_descriptions": None,
    }
    data = selector_service_pb2.DbtNodeData(
        node_unique_id="model.spike.stg_orders", node_body_hash="b", node_macros_hash="m"
    )
    hashes = mapping.v2_node_hashes(data, "r")
    assert (hashes["body"], hashes["macros"], hashes["configs"]) == ("b", "m", None)


def test_answer_reuse_sets_skip_and_reason():
    out = mapping.answer(Decision(REUSE, "no_new_data"), "rid", None, Response)
    assert out.WhichOneof("response") == "skip_execution"
    explained = out.skip_execution.explained_decision
    assert explained.decision_description == "no_new_data"
    assert explained.decision == shared_pb2.SKIP_EXECUTION
    assert out.skip_execution.execution_decision_id == "rid"


def test_answer_build_sets_ready_to_execute():
    out = mapping.answer(Decision(BUILD, "code_changed"), "rid", None, Response)
    assert out.WhichOneof("response") == "ready_to_execute"
    assert out.ready_to_execute.request_id == "rid"
    assert out.ready_to_execute.explained_decision.decision_description == "code_changed"
    assert out.ready_to_execute.explained_decision.decision == shared_pb2.READY_TO_EXECUTE


CLONE = mapping.CloneAnswer(
    clone_sqls=("CREATE OR REPLACE TABLE t SHALLOW CLONE s",),
    clone_source="s",
    clone_target="t",
    clone_required_last_modified_epoch=1793483505000,
)


def test_answer_clone_sets_ready_to_clone():
    out = mapping.answer(Decision(BUILD, CLONED), "rid", CLONE, Response)
    assert out.WhichOneof("response") == "ready_to_clone"
    clone = out.ready_to_clone
    assert clone.request_id == "rid"
    assert list(clone.clone_sqls) == ["CREATE OR REPLACE TABLE t SHALLOW CLONE s"]
    assert (clone.clone_source, clone.clone_target) == ("s", "t")
    assert clone.clone_required_last_modified_epoch == 1793483505000
    assert clone.explained_decision.decision_description == CLONED
    assert clone.explained_decision.decision == shared_pb2.READY_TO_CLONE


@pytest.mark.parametrize("decision", [Decision(BUILD, "code_changed"), Decision(BUILD, PRE_CLONED)])
def test_answer_clone_only_for_a_plain_clone(decision):
    # pre_clone runs the model on top of the clone; ready_to_clone alone would skip that run
    out = mapping.answer(decision, "rid", CLONE, Response)
    assert out.WhichOneof("response") == "ready_to_execute"


def test_answer_clone_reason_without_clone_builds():
    out = mapping.answer(Decision(BUILD, CLONED), "rid", None, Response)
    assert out.WhichOneof("response") == "ready_to_execute"


@pytest.mark.parametrize(
    ("dialect", "sql"),
    [
        ("databricks", "CREATE OR REPLACE TABLE t SHALLOW CLONE s"),
        ("spark", "CREATE OR REPLACE TABLE t SHALLOW CLONE s"),
        ("snowflake", "CREATE OR REPLACE TABLE t CLONE s"),
        ("bigquery", "CREATE OR REPLACE TABLE t CLONE s"),
        ("redshift", None),
        ("postgres", None),
    ],
)
def test_clone_sql(dialect, sql):
    assert mapping.clone_sql(dialect, "t", "s") == sql


VIEW_READ = "select id, amount * 2 as amount from `cat`.`sch`.`v_orders`"


def _view_facts(view_sql):
    dep = shared_pb2.QueryDependency(name="`cat`.`sch`.`v_orders`", query=view_sql)
    return _facts(_sql_req(sql=VIEW_READ, query_dependencies=[dep]))


def _decide_against_own_record(facts, record):
    last = record(
        code_hash=facts.code_hash,
        config_hash=facts.config_hash,
        hash_version=facts.hash_version,
        data_version=TARGET_EPOCH,
        built_at_ms=STARTED_MS,
        upstreams={k: v.token for g in facts.upstream_groups.values() if g for k, v in g.items()},
    )
    return decide(facts, last, NodeStateConfig(), now_ms=0)


def test_view_with_volatile_sql_builds(record):
    facts = _view_facts("select * from `cat`.`sch`.`raw_orders` where d = current_date()")
    assert facts.upstream_groups["volatile:date"] == {"volatile:date": UNKNOWN}
    assert _decide_against_own_record(facts, record).action == BUILD


def test_view_reading_an_unlisted_table_builds(record):
    facts = _view_facts(
        "select * from `cat`.`sch`.`raw_orders` join `cat`.`sch`.`hidden` using (id)"
    )
    assert None in facts.upstream_groups.values()
    assert _decide_against_own_record(facts, record).action == BUILD


def test_view_reading_listed_tables_reuses(record):
    facts = _view_facts("select id, amount from `cat`.`sch`.`raw_orders`")
    assert _decide_against_own_record(facts, record) == Decision(REUSE, "no_new_data")


def test_config_hash_changes_with_execution_type():
    full = _facts(_sql_req(execution_type=shared_pb2.FULL)).config_hash
    assert _facts(_sql_req(execution_type=shared_pb2.MERGE)).config_hash != full


@pytest.mark.parametrize(
    "clone",
    [
        mapping.CloneAnswer(("sql",), "s", "t", None),
        mapping.CloneAnswer((), "s", "t", 1793483505000),
    ],
)
def test_answer_clone_without_epoch_or_sql_builds(clone):
    out = mapping.answer(Decision(BUILD, CLONED), "rid", clone, Response)
    assert out.WhichOneof("response") == "ready_to_execute"


@pytest.mark.parametrize(
    ("data_version", "epoch"),
    [
        ("1793483505000", 1793483505000),
        ("0", 0),
        ("abc", None),
        ("12.5", None),
        ("-1", None),
        ("²", None),
        ("", None),
        (None, None),
    ],
)
def test_clone_epoch_takes_digit_strings_only(data_version, epoch):
    assert mapping.clone_epoch(data_version) == epoch


def test_every_selector_criteria_maps_to_a_selection_criterion():
    from dbt_hodor.selection import CRITERIA

    values = [v for _, v in selector_service_pb2.SelectorCriteria.items() if v]
    assert sorted(mapping.criterion(v) for v in values) == sorted(CRITERIA)
    assert mapping.criterion(selector_service_pb2.SC_UNSPECIFIED) is None

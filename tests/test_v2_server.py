import dataclasses
import sys
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("grpc")
pytest.importorskip("google.protobuf")

import grpc  # noqa: E402

from dbt_hodor.cli import main  # noqa: E402
from dbt_hodor.store.duckdb import DuckDBStore  # noqa: E402
from dbt_hodor.v2 import server as server_mod  # noqa: E402
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache import (  # noqa: E402
    shared_pb2,
    struct_pb2,
)
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    client_telemetry_service_pb2,
    client_telemetry_service_pb2_grpc,
    clone_service_pb2,
    clone_service_pb2_grpc,
    execution_service_pb2,
    execution_service_pb2_grpc,
    explain_service_pb2,
    explain_service_pb2_grpc,
    selector_service_pb2,
    selector_service_pb2_grpc,
    sql_service_pb2,
    sql_service_pb2_grpc,
)
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    client_validation_service_pb2 as validation_pb2,
)
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    client_validation_service_pb2_grpc as validation_grpc,
)
from dbt_hodor.v2.server import StateServer  # noqa: E402

TARGET = "`cat`.`sch`.`stg_orders`"
RAW = "`cat`.`sch`.`raw_orders`"
SEED = "`cat`.`sch`.`seed_codes`"
RAW_EPOCH = 1_793_483_000_000
TARGET_EPOCH = 1_793_483_505_000
SEED_EPOCH = 1_793_480_000_000


class Store(DuckDBStore):
    """In-memory DuckDB that also keeps what it was asked to append."""

    def __init__(self, path=":memory:"):
        super().__init__(path)
        self.appended = []

    def append(self, records):
        super().append(records)
        self.appended.extend(records)


def _state(uid, resource_type="model", target_name="dev", body=None):
    return shared_pb2.DbtNodeState(
        node_unique_id=uid,
        target_name=target_name,
        node_body_hash=body,
        project_name="p",
        resource_type=resource_type,
        node_hash="h",
        node_configs_hash="c1",
        node_contract_hash="k1",
        profile_name="p",
    )


def _table(name, epoch):
    return shared_pb2.TableModifiedInfo(name=name, last_modified_epoch=epoch)


def sql_req(
    target=TARGET,
    raw_epoch=RAW_EPOCH,
    target_epoch=TARGET_EPOCH,
    extra=(),
    tolerance=0,
    target_name="dev",
    body=None,
    sql="select id from cat.sch.raw_orders",
):
    tables = [_table(RAW, raw_epoch), *extra]
    if target_epoch is not None:
        tables.append(_table(target, target_epoch))
    name = target.strip("`").split("`.`")[-1]
    return sql_service_pb2.SubmitEnrichedSQLRequest(
        target_table=target,
        dialect="databricks",
        default_catalog="cat",
        execution_type=shared_pb2.FULL,
        sql=sql,
        freshness_tolerance_seconds=tolerance,
        tables=tables,
        dbt_node_state=_state(f"model.p.{name}", target_name=target_name, body=body),
    )


def seed_req(values_hash="v1"):
    return sql_service_pb2.SubmitValuesRequest(
        target_table=SEED,
        dialect="databricks",
        default_catalog="cat",
        values_hash=values_hash,
        dbt_node_state=_state("seed.p.seed_codes", "seed"),
    )


def confirm(epoch):
    return lambda rid: execution_service_pb2.ConfirmExecutionRequest(
        request_id=rid, last_modified_epoch=epoch
    )


@pytest.fixture
def serve():
    running = []

    def start(factory=Store, **kwargs):
        srv = StateServer(factory, "p", kwargs.pop("project_id", None), 0, **kwargs)
        channel = grpc.insecure_channel(f"127.0.0.1:{srv.start()}")
        running.append((srv, channel))
        client = SimpleNamespace(
            sql=sql_service_pb2_grpc.SQLStub(channel),
            execution=execution_service_pb2_grpc.ExecutionStub(channel),
            validation=validation_grpc.ClientValidationStub(channel),
            explain=explain_service_pb2_grpc.ExplainStub(channel),
            selector=selector_service_pb2_grpc.SelectorServiceStub(channel),
            telemetry=client_telemetry_service_pb2_grpc.ClientTelemetryStub(channel),
            clone=clone_service_pb2_grpc.CloneStub(channel),
        )
        return srv, client

    yield start
    for srv, channel in running:
        channel.close()
        srv.stop()
    assert not [t.name for t in threading.enumerate() if t.name.startswith("hodor-")]


def _store_factory():
    store = Store()
    return store, lambda: store


def _kind(resp):
    return resp.WhichOneof("response")


def _decided(resp):
    return getattr(resp, _kind(resp)).explained_decision.decision_description


def test_execute_confirm_then_same_epochs_skip(serve):
    store, factory = _store_factory()
    srv, c = serve(factory)
    first = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
    assert _kind(first) == "ready_to_execute"
    assert _decided(first) == "no_history"
    c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(first.ready_to_execute.request_id))
    second = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
    assert _kind(second) == "skip_execution"
    assert _decided(second) == "no_new_data"
    srv.stop()
    assert [(r.action, r.data_version) for r in store.appended] == [
        ("build", str(TARGET_EPOCH)),
        ("reuse", None),
    ]
    build, reuse = store.appended
    assert build.node_key == "cat.sch.stg_orders" and build.unique_id == "model.p.stg_orders"
    assert build.upstreams == {"cat.sch.raw_orders": str(RAW_EPOCH)}
    assert reuse.node_hashes["engine"] == "v2" and reuse.node_hashes["relation"] == TARGET


def _later(monkeypatch):
    """Move the server clock past the default lag_tolerance (2700 s) of the last build."""
    now = server_mod.now_ms
    monkeypatch.setattr(server_mod, "now_ms", lambda: now() + 3 * 3600 * 1000)


@pytest.mark.parametrize(
    ("tolerance", "action", "reason"),
    [
        (0, "ready_to_execute", "new_upstream_data"),
        (-5, "ready_to_execute", "new_upstream_data"),
        (2700, "skip_execution", "within_lag_tolerance"),
    ],
)
def test_lag_tolerance_is_dbts_freshness_tolerance(serve, tolerance, action, reason):
    _, factory = _store_factory()
    _, c = serve(factory)
    first = c.sql.SubmitEnrichedSQL(sql_req(tolerance=tolerance), timeout=5)
    c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(first.ready_to_execute.request_id))
    # a newer upstream one minute later: inside a 2700 s window
    second = c.sql.SubmitEnrichedSQL(
        sql_req(raw_epoch=RAW_EPOCH + 60_000, tolerance=tolerance), timeout=5
    )
    assert (_kind(second), _decided(second)) == (action, reason)


def test_newer_upstream_epoch_executes_after_the_tolerance(serve, monkeypatch):
    _, factory = _store_factory()
    _, c = serve(factory)
    first = c.sql.SubmitEnrichedSQL(sql_req(tolerance=2700), timeout=5)
    c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(first.ready_to_execute.request_id))
    _later(monkeypatch)
    second = c.sql.SubmitEnrichedSQL(
        sql_req(raw_epoch=RAW_EPOCH + 60_000, tolerance=2700), timeout=5
    )
    assert _kind(second) == "ready_to_execute"
    assert _decided(second) == "new_upstream_data"


def test_seed_with_the_same_values_hash_skips(serve):
    _, factory = _store_factory()
    _, c = serve(factory)
    first = c.sql.SubmitValues(seed_req(), timeout=5)
    assert _kind(first) == "ready_to_execute"
    c.execution.ConfirmExecution(confirm(SEED_EPOCH)(first.ready_to_execute.request_id))
    assert _kind(c.sql.SubmitValues(seed_req(), timeout=5)) == "skip_execution"
    changed = c.sql.SubmitValues(seed_req("v2"), timeout=5)
    assert _decided(changed) == "code_changed"


def test_outside_write_to_a_seed_builds_it_next_run(serve, tmp_path):
    path = str(tmp_path / "state.duckdb")
    srv, c = serve(lambda: Store(path))
    first = c.sql.SubmitValues(seed_req(), timeout=5)
    c.execution.ConfirmExecution(confirm(SEED_EPOCH)(first.ready_to_execute.request_id))
    same = sql_req(extra=[_table(SEED, SEED_EPOCH)])
    c.sql.SubmitEnrichedSQL(same, timeout=5)  # the seed's own epoch: nothing marked
    assert _kind(c.sql.SubmitValues(seed_req(), timeout=5)) == "skip_execution"
    c.sql.SubmitEnrichedSQL(sql_req(extra=[_table(SEED, SEED_EPOCH + 5_000)]), timeout=5)
    srv.stop()

    srv, c = serve(lambda: Store(path))  # a new server reads the stale record from the store
    again = c.sql.SubmitValues(seed_req(), timeout=5)
    assert _kind(again) == "ready_to_execute"
    assert _decided(again) == "target_modified"
    srv.stop()
    stale = [r for r in Store(path).load_latest("p", "dev", 0).values() if r.action == "stale"]
    assert [(r.node_key, r.status, r.reason) for r in stale] == [
        ("cat.sch.seed_codes", "success", "target_modified")
    ]


@pytest.mark.parametrize(("version", "supported"), [("2.0.8", True), ("2.0.9", False)])
def test_unsupported_client_version_answers_unsupported(serve, version, supported):
    _, c = serve()
    req = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version=version)
    assert c.validation.ValidateClientVersion(req, timeout=5).is_supported is supported


def test_version_gate_answers_while_the_store_hangs(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.1)
    release = threading.Event()

    def hanging():
        release.wait(10)
        return Store()

    _, c = serve(hanging)
    try:
        req = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version="2.0.8")
        assert c.validation.ValidateClientVersion(req, timeout=1).is_supported
        resp = c.sql.SubmitEnrichedSQL(sql_req(), timeout=2)
        assert _kind(resp) == "ready_to_execute"
        assert _decided(resp) == "store_slow"
    finally:
        release.set()


def test_decisions_build_when_the_store_cannot_open(serve):
    def broken():
        raise OSError("no such store")

    _, c = serve(broken)
    for _ in range(2):
        resp = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
        assert _kind(resp) == "ready_to_execute"
        assert _decided(resp) == "hodor_error"
        c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(resp.ready_to_execute.request_id))
    assert _kind(c.sql.SubmitValues(seed_req(), timeout=5)) == "ready_to_execute"


def test_parallel_submits_and_confirms_write_one_record_each(serve):
    store, factory = _store_factory()
    srv, c = serve(factory)
    errors = []

    def run(i):
        try:
            target = f"`cat`.`sch`.`m{i}`"
            resp = c.sql.SubmitEnrichedSQL(sql_req(target=target), timeout=10)
            c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(resp.ready_to_execute.request_id))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    srv.stop()
    assert not errors
    assert sorted(r.node_key for r in store.appended) == sorted(f"cat.sch.m{i}" for i in range(20))
    assert {r.action for r in store.appended} == {"build"}


def test_confirm_for_unknown_request_writes_nothing(serve):
    store, factory = _store_factory()
    srv, c = serve(factory)
    resp = c.execution.ConfirmExecution(confirm(TARGET_EPOCH)("no-such-id"), timeout=5)
    assert resp.success and resp.request_id == "no-such-id"
    srv.stop()
    assert store.appended == []


def test_speculative_submit_is_undecided(serve):
    _, c = serve()
    resp = c.sql.SubmitEnrichedSQLSpeculative(sql_req(target_epoch=None), timeout=5)
    assert resp.WhichOneof("response") == "undecided"


def test_explain_returns_the_reason_and_inputs(serve, monkeypatch):
    _, c = serve()
    first = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
    rid = first.ready_to_execute.request_id
    assert first.ready_to_execute.execution_decision_id == rid
    c.execution.ConfirmExecution(confirm(TARGET_EPOCH)(rid))
    _later(monkeypatch)
    second = c.sql.SubmitEnrichedSQL(sql_req(raw_epoch=RAW_EPOCH + 60_000), timeout=5)
    ids = [rid, second.ready_to_execute.execution_decision_id, "unknown"]
    req = explain_service_pb2.GetExplainMessagesRequest(execution_decision_ids=ids)
    messages = c.explain.GetExplainMessages(req, timeout=5).messages
    assert [m.decision_description for m in messages] == ["no_history", "new_upstream_data"]
    assert messages[1].decision == shared_pb2.READY_TO_EXECUTE
    texts = [line.text for line in messages[1].explain_lines]
    assert f"cat.sch.raw_orders: {RAW_EPOCH + 60_000} (last build: {RAW_EPOCH})" in texts


def test_record_executions_writes_build_records(serve):
    store, factory = _store_factory()
    srv, c = serve(factory)
    req = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version="2.0.8")
    assert c.validation.ValidateClientVersion(req, timeout=5).is_supported
    sql = sql_req()
    record = _execution(sql, TARGET_EPOCH, runtime_ms=3000)
    req = execution_service_pb2.RecordExecutionsRequest(records=[record])
    assert c.execution.RecordExecutions(req, timeout=5).records_stored == 1
    # the recorded build counts: same epochs reuse
    assert _kind(c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)) == "skip_execution"
    srv.stop()
    assert [(r.action, r.reason) for r in store.appended][0] == ("build", "full_refresh")


def _execution(sql, target_epoch, runtime_ms=None):
    outcome = execution_service_pb2.ExecutionOutcome(last_modified_epoch=target_epoch)
    if runtime_ms is not None:
        outcome.execution_runtime_ms = runtime_ms
    return execution_service_pb2.ExecutionRecord(
        outcome=outcome,
        enriched_sql=execution_service_pb2.SQLExecution(
            target_table=sql.target_table,
            dialect=sql.dialect,
            execution_type=sql.execution_type,
            sql=sql.sql,
            tables=sql.tables,
            dbt_node_state=sql.dbt_node_state,
        ),
    )


STARTED_MS = 1_793_490_000_000  # when the dbt invocation started
REPORTED_MS = STARTED_MS + 160_000  # when dbt reports the execution, possibly in a batch


@pytest.mark.parametrize("validated", [True, False], ids=["invocation-start", "no-start-known"])
def test_a_reported_execution_starts_with_the_invocation(serve, monkeypatch, validated):
    """dbt may report the execution long after it ran, so its start is the start of the
    invocation, or 0 (always build) when none was seen. An upstream rewritten after the
    node read it, within the invocation, must make the next run build."""
    now = [STARTED_MS]
    monkeypatch.setattr(server_mod, "now_ms", lambda: now[0])
    store, factory = _store_factory()
    srv, c = serve(factory)
    if validated:
        req = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version="2.0.8")
        assert c.validation.ValidateClientVersion(req, timeout=5).is_supported
    now[0] = REPORTED_MS
    raw = STARTED_MS + 100_500  # rewritten after the node read it
    target = REPORTED_MS - 100
    sql = sql_req(raw_epoch=raw, target_epoch=target)
    req = execution_service_pb2.RecordExecutionsRequest(
        records=[_execution(sql, target, runtime_ms=3000)]
    )
    assert c.execution.RecordExecutions(req, timeout=5).records_stored == 1
    resp = c.sql.SubmitEnrichedSQL(sql_req(raw_epoch=raw, target_epoch=target), timeout=5)
    assert (_kind(resp), _decided(resp)) == ("ready_to_execute", "same_second_read")
    srv.stop()
    assert store.appended[0].built_at_ms == (STARTED_MS if validated else 0)


def test_each_invocation_reports_its_own_start(serve, monkeypatch):
    """A later invocation's validation does not move the start of an earlier one, and an
    id that never validated has no start."""
    now = [STARTED_MS]
    monkeypatch.setattr(server_mod, "now_ms", lambda: now[0])
    store, factory = _store_factory()
    srv, c = serve(factory)
    vreq = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version="2.0.8")
    first, second, unseen = ((("x-dbt-invocation-id", i),) for i in ("inv-a", "inv-b", "inv-c"))
    c.validation.ValidateClientVersion(vreq, metadata=first, timeout=5)
    now[0] = STARTED_MS + 50_000
    c.validation.ValidateClientVersion(vreq, metadata=second, timeout=5)
    sql = sql_req()
    req = execution_service_pb2.RecordExecutionsRequest(
        records=[_execution(sql, TARGET_EPOCH, runtime_ms=3000)]
    )
    for metadata in (first, second, unseen):
        assert c.execution.RecordExecutions(req, metadata=metadata, timeout=5).records_stored == 1
    srv.stop()
    assert [r.built_at_ms for r in store.appended] == [STARTED_MS, STARTED_MS + 50_000, 0]


def test_record_executions_keeps_the_good_entries(serve, monkeypatch):
    store, factory = _store_factory()
    srv, c = serve(factory)

    def boom(*args):
        raise RuntimeError("boom")

    monkeypatch.setattr(server_mod, "seed_facts", boom)
    sql, seed = sql_req(), seed_req()
    outcome = execution_service_pb2.ExecutionOutcome(last_modified_epoch=TARGET_EPOCH)
    bad = execution_service_pb2.ExecutionRecord(
        outcome=outcome,
        values=execution_service_pb2.ValuesExecution(
            target_table=seed.target_table,
            dialect=seed.dialect,
            values_hash=seed.values_hash,
            dbt_node_state=seed.dbt_node_state,
        ),
    )
    good = execution_service_pb2.ExecutionRecord(
        outcome=outcome,
        enriched_sql=execution_service_pb2.SQLExecution(
            target_table=sql.target_table,
            dialect=sql.dialect,
            execution_type=sql.execution_type,
            sql=sql.sql,
            tables=sql.tables,
            dbt_node_state=sql.dbt_node_state,
        ),
    )
    req = execution_service_pb2.RecordExecutionsRequest(records=[bad, good])
    assert c.execution.RecordExecutions(req, timeout=5).records_stored == 1
    srv.stop()
    assert [r.node_key for r in store.appended] == ["cat.sch.stg_orders"]


def test_failed_loads_do_not_pile_up_threads(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.5)

    class Failing(Store):
        def load(self, *args, **kwargs):
            raise OSError("read failed")

    srv, c = serve(Failing)
    for _ in range(10):
        resp = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
        assert _decided(resp) == "hodor_error"
    # the opener, the writer and at most the latest loader
    assert len(srv._threads) <= 3


def test_a_failing_handler_answers_build(serve, monkeypatch):
    _, c = serve()

    def boom(*args):
        raise RuntimeError("boom")

    monkeypatch.setattr(server_mod, "node_facts", boom)
    resp = c.sql.SubmitEnrichedSQL(sql_req(), timeout=5)
    assert _kind(resp) == "ready_to_execute"
    assert _decided(resp) == "hodor_error"


def _project(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(
        "name: shop\ndbt-cloud:\n  project-id: '42'\n", encoding="utf-8"
    )
    return str(tmp_path)


def test_cli_serve_prints_the_environment(tmp_path, monkeypatch, capsys):
    import dbt_hodor.cli as cli

    seen = {}

    def wait(srv):
        seen["project"] = (srv.project_name, srv.project_id)

    monkeypatch.setattr(cli, "_wait_for_interrupt", wait)
    start = time.monotonic()
    assert main(["serve", "--project-dir", _project(tmp_path), "--port", "0"]) == 0
    out = capsys.readouterr().out
    assert "DBT_ENGINE_MANAGE_STATE=true" in out
    port = out.split("RUN_CACHE_API_URL=127.0.0.1:")[1].split()[0]
    assert int(port) > 0
    assert seen["project"] == ("shop", "42")
    assert time.monotonic() - start < 15


@pytest.mark.parametrize(
    ("args", "env", "allowed", "warned"),
    [
        ([], None, False, False),
        (["--allow-clones"], None, True, False),
        ([], "true", True, False),
        ([], "false", False, False),
        ([], "maybe", False, True),  # an invalid value keeps clones off
    ],
)
def test_cli_serve_turns_clones_on_only_when_asked(
    tmp_path, monkeypatch, capsys, args, env, allowed, warned
):
    import dbt_hodor.cli as cli

    seen = {}
    monkeypatch.setattr(
        cli, "_wait_for_interrupt", lambda srv: seen.setdefault("on", srv.allow_clones)
    )
    if env is None:
        monkeypatch.delenv("HODOR_ALLOW_CLONES", raising=False)
    else:
        monkeypatch.setenv("HODOR_ALLOW_CLONES", env)
    assert main(["serve", "--project-dir", _project(tmp_path), "--port", "0", *args]) == 0
    assert seen["on"] is allowed
    assert ("HODOR_ALLOW_CLONES" in capsys.readouterr().err) is warned


def test_cli_serve_without_grpc_prints_the_install_hint(tmp_path, monkeypatch, capsys):
    import importlib.util

    find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a: None if name == "grpc" else find_spec(name, *a),
    )
    assert main(["serve", "--project-dir", _project(tmp_path), "--port", "0"]) != 0
    assert "pip install 'dbt-hodor[v2]'" in capsys.readouterr().err


def test_cli_serve_lets_other_import_errors_raise(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "dbt_hodor.v2.server", None)
    with pytest.raises(ImportError):
        main(["serve", "--project-dir", _project(tmp_path), "--port", "0"])


# -- state selection, defer and clone ----------------------------------------------------

INV = (("x-dbt-invocation-id", "inv-1"),)
DEV = "`cat`.`dev`.`stg_orders`"
CLONE_SQL = f"CREATE OR REPLACE TABLE {DEV} SHALLOW CLONE {TARGET}"
SC = selector_service_pb2


def _session(c, defer_to="prod", metadata=INV):
    """A telemetry sessionStart that names the run's defer target, as dbt v2 sends it."""
    config = struct_pb2.Struct(fields={"defer_to": struct_pb2.Value(string_value=defer_to)})
    event = client_telemetry_service_pb2.ClientTelemetryEvent(
        session_start=client_telemetry_service_pb2.SessionStartRequest(config=config)
    )
    req = client_telemetry_service_pb2.SubmitTelemetryBatchRequest(events=[event])
    assert c.telemetry.SubmitTelemetryBatch(req, metadata=metadata, timeout=5).success


def _build(c, req, epoch):
    resp = c.sql.SubmitEnrichedSQL(req, timeout=5)
    assert _kind(resp) == "ready_to_execute"
    c.execution.ConfirmExecution(confirm(epoch)(resp.ready_to_execute.request_id), timeout=5)


def _build_prod(c, target_epoch=TARGET_EPOCH, body=None):
    """prod builds raw_orders, then stg_orders on top of it."""
    raw = sql_req(target=RAW, target_name="prod", sql="select 1 as id", body=body)
    _build(c, raw, RAW_EPOCH)
    _build(c, sql_req(target_name="prod", body=body), target_epoch)


def _dev_req(**kwargs):
    """dev's stg_orders, reading prod's raw_orders (deferred)."""
    return sql_req(target=DEV, target_epoch=None, **kwargs)


def _node(uid, body, relation=None):
    return SC.DbtNodeData(
        node_unique_id=uid,
        node_hash="h",
        node_configs_hash="c1",
        node_contract_hash="k1",
        node_body_hash=body,
        node_database_representation=relation,
    )


def _select(c, criteria, target="prod", project_id="", metadata=INV, schema=None):
    def rel(name):  # dbt 2.0.8 sends the session target's relation of each node
        return f"cat.{schema}.{name}" if schema else None

    nodes = [
        _node("model.p.raw_orders", "b1", rel("raw_orders")),
        _node("model.p.stg_orders", "b2", rel("stg_orders")),
        _node("model.p.new_model", "b1", rel("new_model")),
    ]
    req = SC.SelectorRequest(
        target=target, selector_criteria=criteria, project_id=project_id, nodes=nodes
    )
    resp = c.selector.GetStateSelection(req, metadata=metadata, timeout=5)
    return sorted(resp.node_unique_ids)


def _rpc_error(call):
    with pytest.raises(grpc.RpcError) as info:
        call()
    return info.value.code(), info.value.details()


@pytest.mark.parametrize(
    ("criteria", "expected"),
    [
        (SC.MODIFIED, ["model.p.new_model", "model.p.stg_orders"]),
        (SC.BODY, ["model.p.new_model", "model.p.stg_orders"]),
        (SC.NEW, ["model.p.new_model"]),
        (SC.UNMODIFIED, ["model.p.raw_orders"]),
        (SC.OLD, ["model.p.raw_orders", "model.p.stg_orders"]),
    ],
)
def test_selection_compares_with_the_base_targets_records(serve, tmp_path, criteria, expected):
    path = str(tmp_path / "state.duckdb")
    srv, c = serve(lambda: Store(path))
    _build_prod(c, body="b1")
    srv.stop()
    _, c = serve(lambda: Store(path))  # a new server reads the records from the store
    assert _select(c, criteria) == expected


def _prod_reuse(record, uid, body):
    """A prod reuse record of a node, as a dbt run of another server writes it."""
    name = uid.split(".")[-1]
    return record(
        record_id=f"reuse-{name}",
        recorded_at_ms=server_mod.now_ms(),
        project_name="p",
        target_name="prod",
        node_key=f"cat.sch.{name}",
        unique_id=uid,
        action="reuse",
        status="reused",
        node_hashes=server_mod.v2_node_hashes(_node(uid, body), TARGET),
    )


def test_selection_counts_a_node_only_reused_in_the_base_target(serve, record):
    # the build fell out of the lookback window; the reuse records are inside it
    store, factory = _store_factory()
    store.append([_prod_reuse(record, "model.p.raw_orders", "b1")])
    _, c = serve(factory)
    assert _select(c, SC.NEW) == ["model.p.new_model", "model.p.stg_orders"]
    assert _select(c, SC.UNMODIFIED) == ["model.p.raw_orders"]


def test_selection_reads_records_written_after_the_server_started(serve, record):
    store, factory = _store_factory()
    _, c = serve(factory)
    assert len(_select(c, SC.NEW)) == 3
    # a dbt-core run or a second server writes prod records to the shared store
    store.append([_prod_reuse(record, "model.p.raw_orders", "b1")])
    assert _select(c, SC.NEW) == ["model.p.new_model", "model.p.stg_orders"]


def test_selection_ignores_the_relation_of_another_target(serve):
    # live dbt 2.0.8: the selection request carries the dev relation of each node, the
    # build requests prod recorded carry none. A differing relation must not mark every
    # node modified
    _, c = serve()
    _build_prod(c, body="b1")
    assert _select(c, SC.MODIFIED, schema="dev") == ["model.p.new_model", "model.p.stg_orders"]
    assert _select(c, SC.UNMODIFIED, schema="dev") == ["model.p.raw_orders"]


@pytest.mark.parametrize("criteria", [SC.RELATION, SC.MACROS, SC.PERSISTED_DESCRIPTIONS])
def test_selection_fails_for_a_hash_dbt_does_not_send(serve, criteria):
    # dbt 2.0.8 sends no macros or persisted descriptions hash, and the relation only for
    # the session target: these sub-selectors cannot be compared, and guessing would
    # select nothing
    _, c = serve()
    _build_prod(c, body="b1")
    code, details = _rpc_error(lambda: _select(c, criteria, schema="dev"))
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "cannot compare" in details


def test_selection_uses_the_sessions_defer_target_when_the_request_names_none(serve):
    _, c = serve()
    _build_prod(c, body="b1")
    _session(c, "prod")
    assert _select(c, SC.NEW, target="") == ["model.p.new_model"]


def test_selection_without_a_base_target_fails_the_run(serve):
    _, c = serve()
    code, details = _rpc_error(lambda: _select(c, SC.MODIFIED, target=""))
    assert code == grpc.StatusCode.FAILED_PRECONDITION
    assert "no base target" in details


def test_selection_fails_when_the_request_and_the_session_disagree(serve):
    _, c = serve()
    _session(c, "prod")
    code, details = _rpc_error(lambda: _select(c, SC.MODIFIED, target="qa"))
    assert code == grpc.StatusCode.FAILED_PRECONDITION
    assert "'qa'" in details and "'prod'" in details


def test_selection_fails_for_another_project_id(serve):
    _, c = serve(project_id="42")
    code, _ = _rpc_error(lambda: _select(c, SC.MODIFIED, project_id="7"))
    assert code == grpc.StatusCode.FAILED_PRECONDITION


def test_selection_rejects_an_unspecified_criterion(serve):
    _, c = serve()
    code, _ = _rpc_error(lambda: _select(c, SC.SC_UNSPECIFIED))
    assert code == grpc.StatusCode.INVALID_ARGUMENT


def test_selection_fails_the_run_when_the_store_is_down(serve):
    def broken():
        raise OSError("no such store")

    _, c = serve(broken)
    code, details = _rpc_error(lambda: _select(c, SC.MODIFIED))
    assert code == grpc.StatusCode.UNAVAILABLE
    assert details == (
        "dbt-hodor cannot compare with prod: cannot open the state store: no such store"
    )


def test_selection_fails_the_run_when_records_cannot_be_read(serve):
    class Failing(Store):
        def load_latest(self, *args, **kwargs):
            raise OSError("read failed")

    _, c = serve(Failing)
    code, details = _rpc_error(lambda: _select(c, SC.MODIFIED))
    assert code == grpc.StatusCode.UNAVAILABLE
    assert details == "dbt-hodor cannot compare with prod: cannot read records: read failed"


def test_selection_waits_for_the_load_and_fails_on_timeout(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.05)
    monkeypatch.setattr(server_mod, "SELECTION_WAIT_S", 0.3)
    release = threading.Event()

    def hanging():
        release.wait(10)
        return Store()

    _, c = serve(hanging)
    try:
        code, details = _rpc_error(lambda: _select(c, SC.MODIFIED))
        assert code == grpc.StatusCode.UNAVAILABLE
        assert details == (
            "dbt-hodor cannot compare with prod: the state store did not answer in 0.3 s"
        )
    finally:
        release.set()


def test_selection_waits_past_the_decision_wait(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.05)

    def slow():
        time.sleep(0.5)
        return Store()

    _, c = serve(slow)
    assert len(_select(c, SC.NEW)) == 3  # no records yet: every node is new


def _defer(c, uids, target_name="prod", metadata=INV):
    req = execution_service_pb2.ResolveDeferredRelationsRequest(
        target_name=target_name, project_name="p", node_unique_ids=uids
    )
    resp = c.execution.ResolveDeferredRelations(req, metadata=metadata, timeout=5)
    return dict(resp.fqn_by_unique_id)


def test_defer_returns_the_base_relation(serve):
    _, c = serve()
    _build_prod(c)
    assert _defer(c, ["model.p.stg_orders", "model.p.unknown"]) == {"model.p.stg_orders": TARGET}


def test_defer_uses_the_sessions_defer_target(serve):
    _, c = serve()
    _build_prod(c)
    assert _defer(c, ["model.p.stg_orders"], target_name="") == {}
    _session(c, "prod")
    assert _defer(c, ["model.p.stg_orders"], target_name="") == {"model.p.stg_orders": TARGET}


def test_defer_to_the_current_target_defers_nothing(serve):
    _, c = serve()
    _build_prod(c)
    # this invocation runs prod itself
    c.sql.SubmitEnrichedSQL(sql_req(target=RAW, target_name="prod"), metadata=INV, timeout=5)
    assert _defer(c, ["model.p.stg_orders"]) == {}


def test_defer_defers_nothing_when_the_store_is_down(serve):
    def broken():
        raise OSError("no such store")

    _, c = serve(broken)
    assert _defer(c, ["model.p.stg_orders"]) == {}


def test_defer_waits_past_the_decision_wait_for_the_base_records(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.05)

    class Slow(Store):
        def load(self, *args, **kwargs):
            time.sleep(0.6)
            return super().load(*args, **kwargs)

    _, c = serve(Slow)
    _build_prod(c)  # the decisions do not wait that long: they build
    assert _defer(c, ["model.p.stg_orders"]) == {"model.p.stg_orders": TARGET}


def test_defer_answers_nothing_when_the_records_stay_away(serve, monkeypatch):
    monkeypatch.setattr(server_mod, "STORE_WAIT_S", 0.05)
    monkeypatch.setattr(server_mod, "SELECTION_WAIT_S", 0.3)

    class Hung(Store):
        def load(self, *args, **kwargs):
            time.sleep(1.5)
            return super().load(*args, **kwargs)

    _, c = serve(Hung)
    assert _defer(c, ["model.p.stg_orders"]) == {}


def test_defer_defers_nothing_when_it_fails(serve, monkeypatch):
    _, c = serve()
    _build_prod(c)

    def boom(*args):
        raise RuntimeError("boom")

    monkeypatch.setattr(server_mod, "deferred_relations", boom)
    assert _defer(c, ["model.p.stg_orders"]) == {}


def test_a_matching_base_build_is_cloned_with_its_epoch(serve):
    store, factory = _store_factory()
    srv, c = serve(factory, allow_clones=True)
    _build_prod(c)
    _session(c, "prod")
    resp = c.sql.SubmitEnrichedSQL(_dev_req(), metadata=INV, timeout=5)
    assert _kind(resp) == "ready_to_clone"
    ready = resp.ready_to_clone
    assert list(ready.clone_sqls) == [CLONE_SQL]
    assert (ready.clone_source, ready.clone_target) == (TARGET, DEV)
    assert ready.clone_required_last_modified_epoch == TARGET_EPOCH
    assert ready.explained_decision.decision_description == "cloned_from_base"
    assert ready.explained_decision.decision == shared_pb2.READY_TO_CLONE
    c.execution.ConfirmExecution(confirm(TARGET_EPOCH + 9)(ready.request_id), timeout=5)
    srv.stop()
    clone = store.appended[-1]
    assert (clone.action, clone.target_name, clone.node_key) == (
        "clone",
        "dev",
        "cat.dev.stg_orders",
    )
    assert (clone.reason, clone.data_version) == ("cloned_from_base", str(TARGET_EPOCH + 9))
    assert clone.node_hashes["relation"] == DEV


def test_failed_to_clone_writes_nothing(serve):
    store, factory = _store_factory()
    srv, c = serve(factory, allow_clones=True)
    _build_prod(c)
    _session(c, "prod")
    resp = c.sql.SubmitEnrichedSQL(_dev_req(), metadata=INV, timeout=5)
    req = execution_service_pb2.ConfirmExecutionRequest(
        request_id=resp.ready_to_clone.request_id, failed_to_clone=True
    )
    assert c.execution.ConfirmExecution(req, timeout=5).success
    srv.stop()
    assert [r.target_name for r in store.appended] == ["prod", "prod"]


def _no_clone(c, req, metadata=INV):
    resp = c.sql.SubmitEnrichedSQL(req, metadata=metadata, timeout=5)
    return _kind(resp)


def test_base_target_equal_to_the_current_one_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    _build(c, sql_req(target=RAW, sql="select 1 as id"), RAW_EPOCH)
    _build(c, sql_req(target_epoch=None), TARGET_EPOCH)
    _session(c, "dev")
    # dev's own table is gone: it builds, it is not cloned from itself
    assert _no_clone(c, sql_req(target_epoch=None)) == "ready_to_execute"


def test_no_defer_target_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    _build_prod(c)
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def test_base_build_without_an_epoch_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    _build(c, sql_req(target=RAW, target_name="prod", sql="select 1 as id"), RAW_EPOCH)
    resp = c.sql.SubmitEnrichedSQL(sql_req(target_name="prod"), timeout=5)
    no_epoch = execution_service_pb2.ConfirmExecutionRequest(
        request_id=resp.ready_to_execute.request_id
    )
    c.execution.ConfirmExecution(no_epoch, timeout=5)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def test_a_newer_upstream_than_the_base_build_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    _build_prod(c)
    _session(c, "prod")
    assert _no_clone(c, _dev_req(raw_epoch=RAW_EPOCH + 60_000)) == "ready_to_execute"


def test_an_upstream_that_maps_to_no_node_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    # raw_orders is a source: no record names it, so its version cannot be compared by id
    _build(c, sql_req(target_name="prod"), TARGET_EPOCH)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def test_allow_clones_false_does_not_clone(serve):
    _, c = serve(allow_clones=False)
    _build_prod(c)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def test_clones_are_off_by_default(serve):
    # a v2 shallow clone of an auto-clustered Databricks table breaks the next rebuild
    _, c = serve()
    _build_prod(c)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"
    refused = _register(c).unable_to_clone
    assert refused.explained_decision.decision_description == "clones_off"


def test_a_failing_clone_check_builds(serve, monkeypatch):
    _, c = serve(allow_clones=True)
    _build_prod(c)
    _session(c, "prod")

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(server_mod, "may_clone", boom)
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def _register(c, **kwargs):
    fields = {
        "target_table": DEV,
        "dialect": "databricks",
        "clone_source_table": TARGET,
        "clone_source_last_modified_epoch": TARGET_EPOCH,
        **kwargs,
    }
    return c.clone.RegisterClone(clone_service_pb2.CloneRequest(**fields), timeout=5)


def test_register_clone_accepts(serve):
    _, c = serve(allow_clones=True)
    ready = _register(c).ready_to_clone
    assert list(ready.clone_sqls) == [CLONE_SQL]
    assert ready.clone_required_last_modified_epoch == TARGET_EPOCH
    assert ready.explained_decision.decision == shared_pb2.READY_TO_CLONE


@pytest.mark.parametrize(
    ("server_kwargs", "request_kwargs"),
    [
        ({"allow_clones": False}, {}),
        ({}, {}),  # clones are off by default
        ({"allow_clones": True}, {"dialect": "redshift"}),
        ({"allow_clones": True}, {"clone_source_table": DEV}),
    ],
)
def test_register_clone_refuses(serve, server_kwargs, request_kwargs):
    _, c = serve(**server_kwargs)
    resp = _register(c, **request_kwargs)
    assert resp.HasField("unable_to_clone") and not resp.HasField("ready_to_clone")


def test_a_base_version_that_is_no_epoch_does_not_clone(serve, tmp_path):
    path = str(tmp_path / "state.duckdb")
    srv, c = serve(lambda: Store(path))
    _build_prod(c)
    srv.stop()

    class NoEpoch(Store):
        def load(self, *args, **kwargs):
            loaded = super().load(*args, **kwargs)
            return {k: dataclasses.replace(r, data_version="etag-1") for k, r in loaded.items()}

    _, c = serve(lambda: NoEpoch(path), allow_clones=True)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"


def test_register_clone_without_a_source_epoch_requires_none(serve):
    _, c = serve(allow_clones=True)
    req = clone_service_pb2.CloneRequest(
        target_table=DEV, dialect="databricks", clone_source_table=TARGET
    )
    ready = c.clone.RegisterClone(req, timeout=5).ready_to_clone
    assert list(ready.clone_sqls) == [CLONE_SQL]
    assert not ready.HasField("clone_required_last_modified_epoch")


def test_a_base_table_that_is_the_target_itself_is_not_cloned(serve):
    _, c = serve(allow_clones=True)
    _build_prod(c)
    _session(c, "prod")
    # dev writes to the same schema as prod: the source would be the target table
    assert _no_clone(c, sql_req(target_epoch=None)) == "ready_to_execute"


def test_an_upstream_two_nodes_claim_does_not_clone(serve):
    _, c = serve(allow_clones=True)
    _build_prod(c)
    # in dev, another node wrote prod's raw_orders relation
    other = sql_req(target=RAW, sql="select 2 as id")
    other.dbt_node_state.node_unique_id = "model.p.other"
    _build(c, other, RAW_EPOCH)
    _session(c, "prod")
    assert _no_clone(c, _dev_req()) == "ready_to_execute"

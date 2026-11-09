"""Live test of `dbt-hodor serve --store postgres`: the real server process on a real Postgres.

No dbt binary and no warehouse: a gRPC client plays dbt. Opt in: `pytest -m postgres
tests/integration/test_v2_postgres.py` with the variables of `tests/e2e/test_postgres_state.py`:

Lakebase:
    HODOR_PG_ENDPOINT=projects/<p>/branches/<b>/endpoints/<e>
    HODOR_PG_DATABASE=databricks_postgres
    HODOR_PG_SCHEMA=<a schema of its own>
    plus workspace credentials for databricks-sdk.

Any other Postgres, for example one in Docker:
    HODOR_PG_HOST=localhost HODOR_PG_USER=postgres HODOR_PG_PASSWORD=<password>
    HODOR_PG_DATABASE=postgres HODOR_PG_SCHEMA=hodor_v2_live HODOR_PG_SSLMODE=disable
    (HODOR_PG_PORT defaults to 5432)

The server gets these as HODOR_ENDPOINT or HODOR_HOST, HODOR_PORT, HODOR_USER, HODOR_PASSWORD,
HODOR_SSLMODE, HODOR_DATABASE, HODOR_SCHEMA and HODOR_TABLE. Each run uses one state table
named hodor_v2_live_<random>, dropped at the end. The schema is created if missing and dropped
at the end only when this run created it; nothing else is dropped.

The server stops the way Ctrl-C stops it: `serve` leaves its wait on KeyboardInterrupt, stops
the gRPC server, writes the queued records and closes the store. Windows cannot send Ctrl-C to
a child process in a group of its own, and `_thread.interrupt_main()` does not wake a
`time.sleep` there, so the child starts through a launcher that replaces the wait of `serve`
(`dbt_hodor.cli.time.sleep`) with one that raises KeyboardInterrupt when the test creates a stop
file. Everything after that point is the server's own code. The signal is not a thread reading
stdin: on Windows that read blocks every child process that inherits the pipe, and the
databricks-sdk runs the `databricks` CLI to authenticate, so the store would never open.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.postgres

psycopg = pytest.importorskip("psycopg")
grpc = pytest.importorskip("grpc")
pytest.importorskip("google.protobuf")

if not os.environ.get("HODOR_PG_DATABASE") or not os.environ.get("HODOR_PG_SCHEMA"):
    pytest.skip("Postgres env vars not set", allow_module_level=True)
if not (os.environ.get("HODOR_PG_ENDPOINT") or os.environ.get("HODOR_PG_HOST")):
    pytest.skip("neither HODOR_PG_ENDPOINT nor HODOR_PG_HOST is set", allow_module_level=True)

from dbt_hodor.core.records import lookup_index_name  # noqa: E402
from dbt_hodor.store.postgres import PostgresStore, lakebase_target  # noqa: E402
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache import shared_pb2  # noqa: E402
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    client_validation_service_pb2 as validation_pb2,
)
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    client_validation_service_pb2_grpc as validation_grpc,
)
from dbt_hodor.v2.proto.query_cache_protobuf.query_cache.services import (  # noqa: E402
    execution_service_pb2,
    execution_service_pb2_grpc,
    sql_service_pb2,
    sql_service_pb2_grpc,
)

DATABASE = os.environ["HODOR_PG_DATABASE"]
SCHEMA = os.environ["HODOR_PG_SCHEMA"]
ENDPOINT = os.environ.get("HODOR_PG_ENDPOINT")
SSLMODE = os.environ.get("HODOR_PG_SSLMODE", "require")
PORT = int(os.environ.get("HODOR_PG_PORT", "5432"))
TABLE = f"hodor_v2_live_{uuid.uuid4().hex[:8]}"
PROJECT = "hodor_v2_pg"
FILLER_NODES = 3000

TARGET = "`cat`.`sch`.`stg_orders`"
RAW = "`cat`.`sch`.`raw_orders`"
RAW_EPOCH = 1_793_483_000_000
TARGET_EPOCH = 1_793_483_505_000
LAUNCHER = """
import os, sys, time, types
from dbt_hodor import cli

real_sleep = time.sleep

def sleep(seconds):  # the wait of `serve`, ended by the stop file as by Ctrl-C
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if os.path.exists(os.environ["V2_TEST_STOP_FILE"]):
            raise KeyboardInterrupt
        real_sleep(0.1)

cli.time = types.SimpleNamespace(time=time.time, sleep=sleep)
sys.exit(cli.main())
"""


# -- Postgres ---------------------------------------------------------------------------------


def connection_args() -> dict[str, object]:
    """Connection for the test's own reads, matching what the server will use."""
    if ENDPOINT:
        host, user, credential = lakebase_target(ENDPOINT)
        return {"host": host, "user": user, "password": credential(), "sslmode": SSLMODE}
    return {
        "host": os.environ["HODOR_PG_HOST"],
        "user": os.environ["HODOR_PG_USER"],
        "password": os.environ.get("HODOR_PG_PASSWORD", ""),
        "sslmode": SSLMODE,
    }


def execute(sql: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(dbname=DATABASE, port=PORT, autocommit=True, **connection_args()) as con:
        with con.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []


def open_store() -> PostgresStore:
    """A second writer: the store class the server uses, opened by the test."""
    if ENDPOINT:
        host, user, credential = lakebase_target(ENDPOINT)
    else:
        host, user = os.environ["HODOR_PG_HOST"], os.environ["HODOR_PG_USER"]
        password = os.environ.get("HODOR_PG_PASSWORD", "")
        credential = lambda: password  # noqa: E731
    return PostgresStore(DATABASE, SCHEMA, TABLE, host, PORT, user, credential, SSLMODE)


def rows(where: str = "TRUE", params: tuple = ()) -> list[tuple]:
    """(node_key, target_name, action, reason) of the state table, oldest first."""
    return execute(
        f'SELECT node_key, target_name, action, reason FROM "{SCHEMA}"."{TABLE}" '
        f"WHERE {where} ORDER BY recorded_at_ms, record_id",
        params,
    )


def wait_for(predicate, what: str, seconds: float = 30.0):
    deadline = time.time() + seconds
    while time.time() < deadline:
        found = predicate()
        if found:
            return found
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture(scope="module", autouse=True)
def state_table():
    """This run's state table; the schema is dropped only if the run created it."""
    existed = bool(execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (SCHEMA,)))
    yield TABLE
    execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')
    if not existed:
        execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}"')  # no CASCADE: never drops anything else


# -- the server process -----------------------------------------------------------------------


def server_env(**override: str | None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("HODOR_")}
    env.update(
        HODOR_DATABASE=DATABASE,
        HODOR_SCHEMA=SCHEMA,
        HODOR_TABLE=TABLE,
        HODOR_SSLMODE=SSLMODE,
        PYTHONUNBUFFERED="1",
    )
    if ENDPOINT:
        env["HODOR_ENDPOINT"] = ENDPOINT
    else:
        env.update(
            HODOR_HOST=os.environ["HODOR_PG_HOST"],
            HODOR_PORT=str(PORT),
            HODOR_USER=os.environ["HODOR_PG_USER"],
            HODOR_PASSWORD=os.environ.get("HODOR_PG_PASSWORD", ""),
        )
    env.update(override)
    return {k: v for k, v in env.items() if v is not None}


class Server:
    """`dbt-hodor serve --store postgres --port 0` as a process, and a gRPC client for it."""

    def __init__(self, project_dir: Path, **env: str | None):
        self.started = time.perf_counter()
        self.stop_file = project_dir / "stop"
        env["V2_TEST_STOP_FILE"] = str(self.stop_file)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                LAUNCHER,
                "serve",
                "--store",
                "postgres",
                "--port",
                "0",
                "--project-dir",
                str(project_dir),
            ],
            env=server_env(**env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.log: list[str] = []
        first = self.proc.stdout.readline().strip()
        second = self.proc.stdout.readline().strip()
        assert first == "DBT_ENGINE_MANAGE_STATE=true", (first, second)
        url = second.removeprefix("RUN_CACHE_API_URL=")
        assert url != second, second
        threading.Thread(target=self._drain, daemon=True).start()
        self.channel = grpc.insecure_channel(url)
        self.validation = validation_grpc.ClientValidationStub(self.channel)
        self.sql = sql_service_pb2_grpc.SQLStub(self.channel)
        self.execution = execution_service_pb2_grpc.ExecutionStub(self.channel)

    def _drain(self) -> None:
        for line in self.proc.stdout:
            self.log.append(line.rstrip())

    def validate(self) -> bool:
        req = validation_pb2.ValidateClientVersionRequest(dbt_run_cache_version="2.0.8")
        return self.validation.ValidateClientVersion(req, timeout=10).is_supported

    def decide(self, target: str = "dev", **kwargs) -> tuple[str, str, str]:
        """(answer, reason, request id) for the model; the answer is the oneof field name."""
        resp = self.sql.SubmitEnrichedSQL(sql_req(target_name=target, **kwargs), timeout=60)
        kind = resp.WhichOneof("response")
        body = getattr(resp, kind)
        return kind, body.explained_decision.decision_description, getattr(body, "request_id", "")

    def confirm(self, request_id: str, epoch: int = TARGET_EPOCH) -> None:
        req = execution_service_pb2.ConfirmExecutionRequest(
            request_id=request_id, last_modified_epoch=epoch
        )
        assert self.execution.ConfirmExecution(req, timeout=10).success

    def stop(self) -> int:
        """Ctrl-C: the server stops, writes its queued records and closes the store."""
        self.channel.close()
        if self.proc.poll() is None:
            self.stop_file.touch()
            try:
                self.proc.wait(60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                raise
        return self.proc.returncode

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    (root / "dbt_project.yml").write_text(
        f'name: {PROJECT}\nversion: "1.0"\nprofile: {PROJECT}\n', encoding="utf-8"
    )
    return root


@pytest.fixture
def serve(project):
    running: list[Server] = []

    def start(**env: str | None) -> Server:
        server = Server(project, **env)
        running.append(server)
        return server

    yield start
    for server in running:
        server.kill()


# -- requests, as the unit tests build them ---------------------------------------------------


def _state(target_name: str, name: str = "stg_orders") -> shared_pb2.DbtNodeState:
    return shared_pb2.DbtNodeState(
        node_unique_id=f"model.{PROJECT}.{name}",
        target_name=target_name,
        project_name=PROJECT,
        resource_type="model",
        node_hash="h",
        node_configs_hash="c1",
        node_contract_hash="k1",
        profile_name=PROJECT,
    )


def sql_req(target_name: str = "dev") -> sql_service_pb2.SubmitEnrichedSQLRequest:
    return sql_service_pb2.SubmitEnrichedSQLRequest(
        target_table=TARGET,
        dialect="databricks",
        default_catalog="cat",
        execution_type=shared_pb2.FULL,
        sql="select id from cat.sch.raw_orders",
        freshness_tolerance_seconds=0,
        tables=[
            shared_pb2.TableModifiedInfo(name=RAW, last_modified_epoch=RAW_EPOCH),
            shared_pb2.TableModifiedInfo(name=TARGET, last_modified_epoch=TARGET_EPOCH),
        ],
        dbt_node_state=_state(target_name),
    )


# -- the tests --------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def first_build(tmp_path_factory):
    """One server run on an empty table: the first decision, its confirmation, a clean stop."""
    root = tmp_path_factory.mktemp("first") / "project"
    root.mkdir()
    (root / "dbt_project.yml").write_text(
        f'name: {PROJECT}\nversion: "1.0"\nprofile: {PROJECT}\n', encoding="utf-8"
    )
    server = Server(root)
    try:
        assert server.validate()
        decision = server.decide()
        server.confirm(decision[2])
        code = server.stop()
    finally:
        server.kill()
    return decision, code


def test_validate_answers_before_the_store_opens(serve):
    # a Postgres nothing listens on: the store cannot open, validate still answers
    # and every decision builds
    server = serve(
        HODOR_ENDPOINT=None,
        HODOR_HOST="127.0.0.1",
        HODOR_PORT="1",
        HODOR_USER="nobody",
        HODOR_PASSWORD="",
    )
    assert server.validate()
    kind, reason, _ = server.decide("unreachable")
    assert (kind, reason) == ("ready_to_execute", "hodor_error")
    assert server.stop() == 0


def test_first_decision_has_no_history_and_is_flushed_on_stop(first_build):
    (kind, reason, _), code = first_build
    assert (kind, reason) == ("ready_to_execute", "no_history")
    assert code == 0, "the server did not stop cleanly"
    # the process has exited: the confirmed record reached the table only through the flush
    assert rows("target_name = %s", ("dev",)) == [
        ("cat.sch.stg_orders", "dev", "build", "no_history")
    ]


def test_lookup_index_exists(first_build):
    index = lookup_index_name(TABLE)
    found = execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND indexname = %s",
        (SCHEMA, index),
    )
    assert found, f"no index {index} on {SCHEMA}.{TABLE}"
    assert "(project_name, target_name, recorded_at_ms)" in found[0][0]


def test_new_process_reuses_the_unchanged_node(serve, first_build):
    server = serve()
    assert server.validate()
    kind, reason, _ = server.decide()
    assert (kind, reason) == ("skip_execution", "no_new_data")
    assert server.stop() == 0
    # the reuse record is written on stop as well
    assert rows("target_name = %s AND action = 'reuse'", ("dev",)) == [
        ("cat.sch.stg_orders", "dev", "reuse", "no_new_data")
    ]


def test_record_from_another_writer_is_seen_after_the_next_validate(serve, first_build):
    server = serve()
    assert server.validate()
    # nothing is recorded for this target yet; the decision loads that (empty) answer
    assert server.decide("shared")[:2] == ("ready_to_execute", "no_history")

    # another writer appends a record of the same node under this target
    store = open_store()
    try:
        template = store.load_latest(PROJECT, "dev", 0)["cat.sch.stg_orders"]
        assert template.action == "build"
        store.append(
            [
                dataclasses.replace(
                    template,
                    record_id=str(uuid.uuid4()),
                    target_name="shared",
                    recorded_at_ms=int(time.time() * 1000),
                )
            ]
        )
    finally:
        store.close()

    # the records of a run are read once: this run still sees the old answer
    assert server.decide("shared")[:2] == ("ready_to_execute", "no_history")
    assert server.validate()  # the next run
    assert server.decide("shared")[:2] == ("skip_execution", "no_new_data")
    assert server.stop() == 0


def test_time_to_first_decision_with_3000_nodes(serve, first_build):
    """Prints the time from process start to the first answer; asserts only the outcome."""
    store = open_store()
    try:
        template = store.load_latest(PROJECT, "dev", 0)["cat.sch.stg_orders"]
        now = int(time.time() * 1000)
        filler = [
            dataclasses.replace(
                template,
                record_id=str(uuid.uuid4()),
                recorded_at_ms=now - i,
                node_key=f"cat.sch.filler_{i}",
                unique_id=f"model.{PROJECT}.filler_{i}",
                upstreams={f"cat.sch.up_{i}_{j}": str(RAW_EPOCH + j) for j in range(3)},
            )
            for i in range(FILLER_NODES)
        ]
        store.append(filler)
    finally:
        store.close()
    (count,) = execute(
        f'SELECT count(DISTINCT node_key) FROM "{SCHEMA}"."{TABLE}" WHERE target_name = %s',
        ("dev",),
    )[0]
    assert count == FILLER_NODES + 1

    server = serve()
    assert server.validate()
    kind, reason, _ = server.decide()
    took = time.perf_counter() - server.started
    print(f"\nserve start to first decision with {count} nodes: {took:.2f} s")
    assert reason != "store_slow"
    assert (kind, reason) == ("skip_execution", "no_new_data")
    assert server.stop() == 0

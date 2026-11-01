"""Live dbt v2 tests: dbt 2.0.8 on Databricks, asking the hodor state server.

Opt in: `pytest -m dbt_v2` with these env vars set:

HODOR_DBT_V2_PYTHON: python of a separate venv with dbt-oss 2.0.8 installed (its `dbt`
executable sits next to it);
HODOR_DBX_HOST, HODOR_DBX_HTTP_PATH, HODOR_DBX_TOKEN, HODOR_DBX_CATALOG, HODOR_DBX_SCHEMA.

The prod target builds into HODOR_DBX_CATALOG.<HODOR_DBX_SCHEMA>_v2, the dev target into
<HODOR_DBX_SCHEMA>_v2_dev. Both schemas are dropped at the end (CASCADE), so point them at
names that hold nothing else. The server runs in this process on a DuckDB store, except in
the test of `dbt-hodor serve`, and logs each request and answer for the assertions.

dbt reads the freshness tolerance from RUN_CACHE_FRESHNESS_TOLERANCE; the tests set it to 0,
so an upstream change always builds.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.dbt_v2

ENV = (
    "HODOR_DBT_V2_PYTHON",
    "HODOR_DBX_HOST",
    "HODOR_DBX_HTTP_PATH",
    "HODOR_DBX_TOKEN",
    "HODOR_DBX_CATALOG",
    "HODOR_DBX_SCHEMA",
)
if not all(os.environ.get(k) for k in ENV):
    pytest.skip("dbt v2 or Databricks env vars not set", allow_module_level=True)
pytest.importorskip("grpc")
pytest.importorskip("databricks.sql")

from databricks import sql as dbsql  # noqa: E402
from google.protobuf import json_format  # noqa: E402

from dbt_hodor.store.duckdb import DuckDBStore  # noqa: E402
from dbt_hodor.v2 import server as server_mod  # noqa: E402
from dbt_hodor.v2.server import StateServer  # noqa: E402

DBT = Path(os.environ["HODOR_DBT_V2_PYTHON"]).parent / ("dbt.exe" if os.name == "nt" else "dbt")
PROJECT = "hodor_v2"
SCHEMA = f"{os.environ['HODOR_DBX_SCHEMA']}_v2"  # not the schema of the other live tests
CATALOG = os.environ["HODOR_DBX_CATALOG"]
PROD = f"`{CATALOG}`.`{SCHEMA}`"
DEV = f"`{CATALOG}`.`{SCHEMA}_dev`"

CONNECTION = """
              type: databricks
              host: "{{ env_var('HODOR_DBX_HOST') }}"
              http_path: "{{ env_var('HODOR_DBX_HTTP_PATH') }}"
              token: "{{ env_var('HODOR_DBX_TOKEN') }}"
              catalog: "{{ env_var('HODOR_DBX_CATALOG') }}"
              threads: 4"""

FILES = {
    "dbt_project.yml": f"""
        name: {PROJECT}
        version: "1.0"
        profile: {PROJECT}
        dbt-cloud:
          project-id: "1"
    """,
    "profiles.yml": f"""
        {PROJECT}:
          target: prod
          outputs:
            prod:{CONNECTION}
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
            dev:{CONNECTION}
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}_dev"
    """,
    "seeds/raw_orders.csv": """
        id,amount
        1,10
        2,20
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount from {{ ref('raw_orders') }}
    """,
    "models/v_orders.sql": """
        {{ config(materialized='view') }}
        select id, amount * 2 as amount from {{ ref('stg_orders') }}
    """,
    "models/orders_summary.sql": """
        {{ config(materialized='table') }}
        select count(*) as n, sum(amount) as total from {{ ref('v_orders') }}
    """,
    "models/inc_orders.sql": """
        {{ config(materialized='incremental', unique_key='id') }}
        select id, amount from {{ ref('stg_orders') }}
    """,
    "models/const_codes.sql": """
        {{ config(materialized='table') }}
        select 1 as id, 'a' as code
    """,
    "models/maybe_fail.sql": """
        {{ config(materialized='table') }}
        select id, {% if var('fail', false) %} no_such_column {% else %} amount {% endif %}
          as amount from {{ ref('stg_orders') }}
    """,
    # built outside hodor by the connection tests, so no other test depends on it
    "models/side.sql": """
        {{ config(materialized='table') }}
        select 1 as id
    """,
}
DECISIONS = ("SubmitEnrichedSQL", "SubmitValues")


def execute(sql: str) -> list[tuple]:
    with (
        dbsql.connect(
            server_hostname=os.environ["HODOR_DBX_HOST"],
            http_path=os.environ["HODOR_DBX_HTTP_PATH"],
            access_token=os.environ["HODOR_DBX_TOKEN"],
            enable_telemetry=False,
        ) as con,
        con.cursor() as cur,
    ):
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []


class LoggingServer(StateServer):
    """The state server, keeping every request and answer as JSON."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[dict] = []
        self._log_lock = threading.Lock()

    def _wrap(self, name, out_cls):
        inner = super()._wrap(name, out_cls)

        def unary(request, context):
            response = inner(request, context)
            entry = {
                "rpc": name,
                "req": json_format.MessageToDict(request),
                "resp": json_format.MessageToDict(response),
            }
            with self._log_lock:
                self.calls.append(entry)
            return response

        return unary

    def _caught_up(self) -> None:
        deadline = time.time() + 30
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.1)
        time.sleep(0.5)

    def records(self, target: str) -> dict:
        """The latest successful record per node name (reuse records have status reused)."""
        self._caught_up()
        latest = self._store.load_latest(PROJECT, target, 0)
        return {r.unique_id.rsplit(".", 1)[-1]: r for r in latest.values()}

    def last_invocation(self, target: str) -> dict:
        """Node name: record, of the latest invocation."""
        self._caught_up()
        return {
            r.unique_id.rsplit(".", 1)[-1]: r for r in self._store.invocation(PROJECT, target, None)
        }


class Run:
    def __init__(self, proc: subprocess.CompletedProcess, calls: list[dict]):
        self.rc = proc.returncode
        self.out = proc.stdout + proc.stderr
        self.calls = calls

    def rpcs(self, name: str) -> list[dict]:
        return [c for c in self.calls if c["rpc"] == name]

    def decisions(self) -> dict[str, tuple[str, str]]:
        """Node name: (answer, reason) of each decision request."""
        out = {}
        for call in self.calls:
            if call["rpc"] not in DECISIONS:
                continue
            (kind, body), *_ = call["resp"].items()
            reason = body.get("explainedDecision", {}).get("decisionDescription", "")
            out[call["req"]["labels"]["dbt_node_name"]] = (kind, reason)
        return out

    def confirmed(self) -> set[str]:
        return {c["req"]["labels"]["dbt_node_name"] for c in self.rpcs("ConfirmExecution")}


def run_dbt(root: Path, url: str | None, *args: str, server=None, **env: str) -> Run:
    start = len(server.calls) if server else 0
    environ = dict(
        os.environ,
        HODOR_DBX_SCHEMA=SCHEMA,
        RUN_CACHE_FRESHNESS_TOLERANCE="0",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    if url:
        environ.update(DBT_ENGINE_MANAGE_STATE="true", RUN_CACHE_API_URL=url)
    environ.update(env)
    proc = subprocess.run(
        [str(DBT), *args, "--project-dir", str(root), "--profiles-dir", str(root)],
        env=environ,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=900,
    )
    return Run(proc, server.calls[start:] if server else [])


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    root = tmp_path_factory.mktemp("v2") / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    for schema in (PROD, DEV):
        execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    yield root
    for schema in (PROD, DEV):
        execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture(scope="module")
def server(project):
    path = str(project.parent / "state.duckdb")
    # clones are off by default (`serve --allow-clones` turns them on); the clone tests need them
    srv = LoggingServer(lambda: DuckDBStore(path), PROJECT, "1", 0, allow_clones=True)
    port = srv.start()
    srv.url = f"127.0.0.1:{port}"
    yield srv
    srv.stop()


@pytest.fixture
def dbt(project, server):
    return lambda *args, **env: run_dbt(project, server.url, *args, server=server, **env)


@pytest.fixture
def edit(project):
    """Rewrite project files for one test; the originals come back afterwards."""
    saved: dict[Path, str] = {}

    def write(name: str, body: str) -> None:
        path = project / name
        saved.setdefault(path, path.read_text(encoding="utf-8"))
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")

    yield write
    for path, body in saved.items():
        path.write_text(body, encoding="utf-8")


def ensure_prod(dbt) -> Run:
    run = dbt("build", "--target", "prod", "--exclude", "side")
    assert run.rc == 0, run.out[-3000:]
    return run


# -- reuse and builds ------------------------------------------------------------------------


def test_first_build_then_everything_reused(dbt, server):
    first = dbt("build", "--target", "prod", "--exclude", "side")
    assert first.rc == 0, first.out[-3000:]
    built = {"raw_orders", "stg_orders", "orders_summary", "inc_orders", "const_codes"}
    assert {n: d for n, d in first.decisions().items() if n in built} == {
        n: ("readyToExecute", "no_history") for n in built
    }
    # dbt 2.0.8 sends a view's request without its SQL: hodor cannot hash it, so it builds
    assert first.decisions()["v_orders"] == ("readyToExecute", "unsupported")
    assert built | {"v_orders", "maybe_fail"} <= first.confirmed()

    # the seed's confirm epoch is the epoch the model reading it lists for the seed table
    seed_epoch = next(
        c["req"]["lastModifiedEpoch"]
        for c in first.rpcs("ConfirmExecution")
        if c["req"]["labels"]["dbt_node_name"] == "raw_orders"
    )
    stg = next(
        c["req"] for c in first.rpcs("SubmitEnrichedSQL") if "stg_orders" in c["req"]["targetTable"]
    )
    assert [t["lastModifiedEpoch"] for t in stg["tables"] if "raw_orders" in t["name"]] == [
        seed_epoch
    ]
    values = first.rpcs("SubmitValues")[0]["req"]["dbtNodeState"]
    assert values["resourceType"] == "seed"

    # a model reading a view: dbt lists the view's own tables, and sends the view's query
    summary = next(
        c["req"]
        for c in first.rpcs("SubmitEnrichedSQL")
        if "orders_summary" in c["req"]["targetTable"]
    )
    assert [t["name"] for t in summary["tables"]] == [f"{PROD}.`stg_orders`"]
    assert [d["name"] for d in summary["queryDependencies"]] == [f"{PROD}.`v_orders`"]

    second = dbt("build", "--target", "prod", "--exclude", "side")
    assert second.rc == 0, second.out[-3000:]
    decided = second.decisions()
    assert decided.pop("v_orders") == ("readyToExecute", "unsupported")
    assert decided == {n: ("skipExecution", "no_new_data") for n in built | {"maybe_fail"}}
    reused = server.last_invocation("prod")["stg_orders"]
    assert (reused.action, reused.status, reused.node_hashes["engine"]) == ("reuse", "reused", "v2")


def test_seed_change_builds_downstream(dbt, edit):
    ensure_prod(dbt)
    edit("seeds/raw_orders.csv", "id,amount\n1,10\n2,20\n3,30")
    run = dbt("build", "--target", "prod", "--exclude", "side")
    assert run.rc == 0, run.out[-3000:]
    decided = run.decisions()
    assert decided["raw_orders"] == ("readyToExecute", "code_changed")
    for name in ("stg_orders", "orders_summary", "inc_orders", "maybe_fail"):
        assert decided[name] == ("readyToExecute", "new_upstream_data"), (name, decided)
    assert decided["const_codes"] == ("skipExecution", "no_new_data")
    assert execute(f"SELECT n, total FROM {PROD}.orders_summary") == [(3, 120)]

    # a config-only seed change: dbt sends it in the semantic extras
    edit(
        "dbt_project.yml",
        FILES["dbt_project.yml"]
        + f"""
        seeds:
          {PROJECT}:
            raw_orders:
              +column_types: {{amount: bigint}}
        """,
    )
    run = dbt("build", "--target", "prod", "-s", "raw_orders")
    assert run.rc == 0, run.out[-3000:]
    assert run.decisions()["raw_orders"] == ("readyToExecute", "config_changed")


def test_failed_build_is_not_recorded(dbt, server):
    ensure_prod(dbt)
    before = server.records("prod")["maybe_fail"]
    failed = dbt("build", "--target", "prod", "-s", "maybe_fail", "--vars", "{fail: true}")
    assert failed.rc != 0
    assert failed.decisions()["maybe_fail"] == ("readyToExecute", "code_changed")
    # dbt does not confirm a failed execution, so the server writes nothing for it
    assert "maybe_fail" not in failed.confirmed()
    assert not failed.rpcs("RecordExecutions")
    assert server.records("prod")["maybe_fail"].record_id == before.record_id

    # the same failing code again: no success was recorded, so it builds again
    again = dbt("build", "--target", "prod", "-s", "maybe_fail", "--vars", "{fail: true}")
    assert again.rc != 0
    assert again.decisions()["maybe_fail"] == ("readyToExecute", "code_changed")
    # back to the code of the last success: the table still holds that build
    back = dbt("build", "--target", "prod", "-s", "maybe_fail")
    assert back.rc == 0, back.out[-3000:]
    assert back.decisions()["maybe_fail"] == ("skipExecution", "no_new_data")


def test_full_refresh(dbt, server):
    ensure_prod(dbt)
    run = dbt("build", "--target", "prod", "--full-refresh", "-s", "stg_orders", "inc_orders")
    assert run.rc == 0, run.out[-3000:]
    # dbt asks for a table model as usual and sends no full-refresh flag: it can be reused
    assert run.decisions() == {"stg_orders": ("skipExecution", "no_new_data")}
    # the incremental model runs as a plain table without asking, and is reported after
    (recorded,) = run.rpcs("RecordExecutions")
    (entry,) = recorded["req"]["records"]
    assert entry["enrichedSql"]["executionType"] == "FULL"
    assert recorded["resp"] == {"recordsStored": 1}
    record = server.records("prod")["inc_orders"]
    assert (record.action, record.reason, record.status) == ("build", "full_refresh", "success")


# -- dbt state explain and state selection ---------------------------------------------------


def test_state_explain(dbt):
    ensure_prod(dbt)
    run = dbt("state", "explain", "--target", "prod")
    assert run.rc == 0, run.out[-3000:]
    assert f"SKIP_EXECUTION model.{PROJECT}.stg_orders - no_new_data" in run.out, run.out
    assert f"READY_TO_EXECUTE model.{PROJECT}.v_orders - unsupported" in run.out, run.out
    assert run.rpcs("GetExplainMessages")


def test_state_modified(dbt, edit):
    ensure_prod(dbt)
    # dev's relations differ from prod's: that alone is no modification
    same = dbt("build", "--target", "dev", "-s", "state:modified", "--exclude", "side")
    assert same.rc == 0, same.out[-3000:]
    (selection,) = same.rpcs("GetStateSelection")
    assert selection["req"]["target"] == "prod"
    # side has no prod build with this server, so it is new; dbt applies --exclude after
    assert set(selection["resp"].get("nodeUniqueIds", [])) <= {f"model.{PROJECT}.side"}
    assert not same.decisions()

    edit("models/const_codes.sql", "{{ config(materialized='table') }} select 2 as id, 'b' as code")
    changed = dbt("build", "--target", "dev", "-s", "state:modified", "--exclude", "side")
    assert changed.rc == 0, changed.out[-3000:]
    (selection,) = changed.rpcs("GetStateSelection")
    assert set(selection["resp"]["nodeUniqueIds"]) - {f"model.{PROJECT}.side"} == {
        f"model.{PROJECT}.const_codes"
    }

    # dbt sends no hash it could compare for the relation: the selection fails
    relation = dbt("build", "--target", "dev", "-s", "state:modified.relation")
    assert relation.rc != 0
    assert "cannot compare state:modified.relation" in relation.out


# -- defer and clones ------------------------------------------------------------------------


def test_defer_and_clone(dbt, edit):
    ensure_prod(dbt)
    execute(f"DROP TABLE IF EXISTS {DEV}.const_codes")
    # no --defer: dbt still names prod as its defer target, and the server clones
    cloned = dbt("build", "--target", "dev", "-s", "const_codes")
    assert cloned.rc == 0, cloned.out[-3000:]
    assert cloned.decisions()["const_codes"] == ("readyToClone", "cloned_from_base")
    (start,) = [
        e["sessionStart"]
        for c in cloned.rpcs("SubmitTelemetryBatch")
        for e in c["req"]["events"]
        if "sessionStart" in e
    ]
    assert start["config"]["fields"]["defer_to"] == {"stringValue": "prod"}
    history = execute(f"DESCRIBE HISTORY {DEV}.const_codes")
    assert history[0][4] == "CLONE", history[0]
    assert execute(f"SELECT id, code FROM {DEV}.const_codes") == [(1, "a")]
    # a clone record is not a build record of dev: the next run clones again
    again = dbt("build", "--target", "dev", "-s", "const_codes")
    assert again.decisions()["const_codes"] == ("readyToClone", "cloned_from_base")

    # --defer: the unselected view resolves to prod's relation
    deferred = dbt("build", "--target", "dev", "-s", "orders_summary", "--defer")
    assert deferred.rc == 0, deferred.out[-3000:]
    (resolved,) = deferred.rpcs("ResolveDeferredRelations")
    assert resolved["resp"]["fqnByUniqueId"] == {f"model.{PROJECT}.v_orders": f"{PROD}.`v_orders`"}
    (summary,) = deferred.rpcs("SubmitEnrichedSQL")
    assert f"{PROD}.`v_orders`" in summary["req"]["sql"]
    assert execute(f"SELECT n, total FROM {DEV}.orders_summary") == execute(
        f"SELECT n, total FROM {PROD}.orders_summary"
    )

    # an incremental model missing in dev: dbt clones it itself, registers the clone, and
    # runs its incremental step on top
    execute(f"DROP TABLE IF EXISTS {DEV}.inc_orders")
    registered = dbt("build", "--target", "dev", "-s", "inc_orders")
    assert registered.rc == 0, registered.out[-3000:]
    (clone,) = registered.rpcs("RegisterClone")
    assert clone["req"]["executionType"] == "APPEND"
    explained = clone["resp"]["readyToClone"]["explainedDecision"]
    assert explained["decisionDescription"] == "clone_registered"
    assert execute(f"SELECT count(*) FROM {DEV}.inc_orders") == execute(
        f"SELECT count(*) FROM {PROD}.inc_orders"
    )
    execute(f"DROP TABLE IF EXISTS {DEV}.inc_orders")

    # a changed model over its own shallow clone: Databricks cannot replace a shallow clone
    # of a table with automatic liquid clustering
    auto = dict(execute(f"SHOW TBLPROPERTIES {PROD}.const_codes")).get("clusterByAuto")
    edit("models/const_codes.sql", "{{ config(materialized='table') }} select 3 as id, 'c' as code")
    rebuilt = dbt("build", "--target", "dev", "-s", "const_codes")
    # changed code is not cloned; the reason depends on whether dev built it before
    # (test_state_modified does), since clone records are not build records
    action, reason = rebuilt.decisions()["const_codes"]
    assert (action, reason in ("no_history", "code_changed")) == ("readyToExecute", True)
    if auto == "true":
        assert rebuilt.rc != 0
        assert "CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR" in rebuilt.out
        # --full-refresh replaces the table the same way and fails too
        refreshed = dbt("build", "--target", "dev", "-s", "const_codes", "--full-refresh")
        assert refreshed.rc != 0
        assert "CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR" in refreshed.out
        # the workaround: drop the clone, then the model builds
        execute(f"DROP TABLE {DEV}.const_codes")
        again = dbt("build", "--target", "dev", "-s", "const_codes")
        assert again.rc == 0, again.out[-3000:]
        assert execute(f"SELECT id, code FROM {DEV}.const_codes") == [(3, "c")]
    else:
        assert rebuilt.rc == 0, rebuilt.out[-3000:]
    execute(f"DROP TABLE IF EXISTS {DEV}.const_codes")


# -- the connection to the server ------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_without_a_server_dbt_builds(project):
    run = run_dbt(project, f"127.0.0.1:{_free_port()}", "build", "--target", "prod", "-s", "side")
    assert run.rc == 0, run.out[-3000:]
    assert "client initialization failed" in run.out
    assert "executing normally" in run.out


def test_hung_server_dbt_builds_after_a_timeout(project):
    """A server that accepts the connection and never answers."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    held = []

    def accept():
        try:
            while True:
                held.append(listener.accept())
        except OSError:  # the listener was closed
            pass

    threading.Thread(target=accept, daemon=True).start()
    started = time.time()
    try:
        url = f"127.0.0.1:{listener.getsockname()[1]}"
        run = run_dbt(project, url, "build", "--target", "prod", "-s", "side")
    finally:
        listener.close()
    took = time.time() - started
    assert run.rc == 0, run.out[-3000:]
    assert "keep-alive timed out" in run.out
    assert "executing normally" in run.out
    print(f"dbt waited {took:.0f} s for the hung server")  # dbt 2.0.8: about 130 s


def test_unsupported_client_version_builds(project, monkeypatch, tmp_path):
    monkeypatch.setattr(server_mod, "SUPPORTED_CLIENT_VERSIONS", frozenset({"0.0.0"}))
    srv = LoggingServer(lambda: DuckDBStore(str(tmp_path / "s.duckdb")), PROJECT, "1", 0)
    url = f"127.0.0.1:{srv.start()}"
    try:
        run = run_dbt(project, url, "build", "--target", "prod", "-s", "side", server=srv)
    finally:
        srv.stop()
    assert run.rc == 0, run.out[-3000:]
    assert "does not support this client version" in run.out
    assert [c["rpc"] for c in run.calls] == ["ValidateClientVersion"]


@pytest.mark.parametrize(
    ("args", "env"),
    [(("--no-manage-state",), {}), ((), {"DBT_ENGINE_MANAGE_STATE": "false"})],
    ids=["flag", "env"],
)
def test_state_client_off(project, server, args, env):
    start = len(server.calls)
    run = run_dbt(project, server.url, "build", "--target", "prod", "-s", "side", *args, **env)
    assert run.rc == 0, run.out[-3000:]
    assert server.calls[start:] == []


def test_serve_cli(project, tmp_path):
    """`dbt-hodor serve` as a process, and `dbt-hodor explain` on its store file."""
    path = tmp_path / "serve.duckdb"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys; from dbt_hodor.cli import main; sys.exit(main())",
            "serve",
            "--project-dir",
            str(project),
            "--port",
            "0",
            "--path",
            str(path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        lines = [proc.stdout.readline().strip() for _ in range(2)]
        assert lines[0] == "DBT_ENGINE_MANAGE_STATE=true", lines
        url = lines[1].removeprefix("RUN_CACHE_API_URL=")
        first = run_dbt(project, url, "build", "--target", "prod", "-s", "const_codes")
        assert first.rc == 0, first.out[-3000:]
        second = run_dbt(project, url, "build", "--target", "prod", "-s", "const_codes")
        assert second.rc == 0, second.out[-3000:]
        assert "Reused model" in second.out, second.out[-3000:]
        time.sleep(1)
        # the server holds the DuckDB file: explain cannot open it meanwhile
        held = _explain(project, path)
        assert held.returncode != 0
        print("explain while serving:", (held.stdout + held.stderr)[-500:])
    finally:
        proc.terminate()
        proc.wait(30)
    after = _explain(project, path)
    assert after.returncode == 0, after.stdout + after.stderr
    assert "const_codes" in after.stdout


def _explain(project: Path, path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from dbt_hodor.cli import main; sys.exit(main())",
            "explain",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target",
            "prod",
        ],
        env=dict(os.environ, HODOR_DBX_SCHEMA=SCHEMA, HODOR_STORE="duckdb", HODOR_PATH=str(path)),
        capture_output=True,
        text=True,
        timeout=300,
    )

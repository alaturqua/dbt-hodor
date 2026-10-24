"""End to end: how hodor behaves with dbt Core commands and flags that the lifecycle tests do
not cover. Real dbt-duckdb runs in-process with hodor loaded as a dbt plugin."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from dbt.cli.main import dbtRunner  # noqa: E402
from dbt.task.run import ModelRunner  # noqa: E402
from dbt.task.seed import SeedRunner  # noqa: E402
from dbt.task.test import TestRunner as DataTestRunner  # noqa: E402

from dbt_hodor.plugin import ORIGINALS  # noqa: E402

FILES = {
    "dbt_project.yml": """
        name: core_e2e
        version: "1.0"
        profile: core_e2e
        models:
          core_e2e:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        core_e2e:
          target: local
          outputs:
            local:
              type: duckdb
              path: "{{ env_var('CORE_E2E_WAREHOUSE') }}"
              schema: main
              threads: "{{ env_var('CORE_E2E_THREADS', '1') | int }}"
              hodor_store: duckdb
              hodor_path: "{{ env_var('CORE_E2E_STATE') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            schema: raw
            tables:
              - name: orders
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount from {{ source('raw', 'orders') }}
    """,
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2 from {{ ref('stg_orders') }}
    """,
    "models/rpt_orders.sql": """
        {{ config(materialized='table') }}
        select count(*) as n from {{ ref('fct_orders') }}
    """,
    "models/schema.yml": """
        models:
          - name: fct_orders
            columns:
              - name: id
                data_tests: [not_null]
        unit_tests:
          - name: fct_orders_doubles
            model: fct_orders
            given:
              - input: ref('stg_orders')
                rows:
                  - {id: 1, amount: 5}
            expect:
              rows:
                - {id: 1, amount2: 10}
    """,
    "seeds/countries.csv": """
        code,name
        de,Germany
    """,
}


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    warehouse = tmp_path / "warehouse.duckdb"
    state = tmp_path / "state.duckdb"
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create schema raw")
        con.execute(
            "create table raw.orders as select * from (values (1, 10), (2, 20)) v(id, amount)"
        )
    monkeypatch.setenv("CORE_E2E_WAREHOUSE", str(warehouse))
    monkeypatch.setenv("CORE_E2E_STATE", str(state))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    return root, warehouse, state


def invoke(root: Path, command: str, *args: str):
    """Run a dbt command; the caller checks the exit status."""
    return dbtRunner().invoke(
        [command, "--project-dir", str(root), "--profiles-dir", str(root), *args]
    )


def results(res) -> dict[str, tuple[str, str]]:
    """(status, message) per node name of one invocation."""
    return {r.node.name: (str(r.status), r.message or "") for r in res.result.results}


def build(root: Path, *args: str) -> dict[str, tuple[str, str]]:
    res = invoke(root, "build", *args)
    assert res.success, res.exception
    return results(res)


def records(state: Path) -> list[tuple]:
    """(short name, decision, reason, action, status) in write order."""
    if not state.exists():
        return []
    with duckdb.connect(str(state), read_only=True) as con:
        rows = con.execute(
            "select unique_id, decision, reason, action, status from main.hodor_state "
            "order by recorded_at_ms, record_id"
        ).fetchall()
    return [(uid.split(".")[2], *rest) for uid, *rest in rows]


def latest(state: Path) -> dict[str, tuple[str, str, str]]:
    """Latest (decision, reason, action) per node short name."""
    return {name: (d, r, a) for name, d, r, a, _ in records(state)}


def unpatched() -> bool:
    return (
        ModelRunner.execute is ORIGINALS["ModelRunner.execute"]
        and DataTestRunner.execute is ORIGINALS["TestRunner.execute"]
        and SeedRunner.print_result_line is ORIGINALS["SeedRunner.print_result_line"]
    )


def count(warehouse: Path, relation: str) -> int:
    with duckdb.connect(str(warehouse)) as con:
        return con.execute(f"select count(*) from {relation}").fetchone()[0]


# -- plugin gating ------------------------------------------------------------------------


def test_commands_other_than_the_enabled_ones_stay_unpatched(project):
    root, _, state = project
    build(root)
    assert not unpatched()  # build installed the patches
    written = records(state)

    for command in ("compile", "ls"):
        res = invoke(root, command)
        assert res.success, res.exception
        assert unpatched(), command
        assert records(state) == written, command

    # the next enabled command installs the patches again
    assert build(root)["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert not unpatched()


def test_mode_off_in_the_environment_installs_no_patches(project, monkeypatch):
    root, _, state = project
    monkeypatch.setenv("HODOR_MODE", "off")
    build(root)
    assert unpatched()
    assert not state.exists()


# -- node types dbt runs differently ------------------------------------------------------


def test_microbatch_goes_straight_to_dbt(project):
    root, _, state = project
    (root / "models" / "mb.sql").write_text(
        "{{ config(materialized='incremental', incremental_strategy='microbatch',\n"
        "    event_time='ts', begin='2024-01-01', batch_size='day') }}\n"
        "select 1 as id, timestamp '2024-01-02 00:00:00' as ts\n",
        encoding="utf-8",
    )
    window = ("-s", "mb", "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-03")
    for _ in range(2):  # a second run never reuses it
        assert build(root, *window)["mb"] == ("success", "SUCCESS")
    assert not any(name == "mb" for name, *_ in records(state))


def test_seed_show_always_builds(project):
    root, _, state = project
    assert invoke(root, "seed").success
    assert results(invoke(root, "seed"))["countries"] == ("reused", "REUSED (no_new_data)")
    written = len(records(state))

    shown = invoke(root, "seed", "--show")
    assert shown.success, shown.exception
    assert results(shown)["countries"] == ("success", "INSERT 1")
    assert len(records(state)) == written  # passed to dbt untouched, nothing recorded


def test_unit_tests_always_run_and_are_not_recorded(project):
    root, _, state = project
    build(root)
    second = build(root)
    assert second["fct_orders"][0] == "reused"
    assert second["fct_orders_doubles"] == ("pass", "")  # not "(replayed)" and not reused
    assert not any(name == "fct_orders_doubles" for name, *_ in records(state))


# -- --empty ------------------------------------------------------------------------------


def add_snapshot(root: Path) -> None:
    (root / "snapshots").mkdir()
    (root / "snapshots" / "orders_snapshot.yml").write_text(
        "snapshots: [{name: orders_snapshot, relation: \"source('raw', 'orders')\", "
        "config: {unique_key: id, strategy: check, check_cols: all}}]",
        encoding="utf-8",
    )


ALL = ("stg_orders", "countries", "fct_orders", "rpt_orders", "orders_snapshot")


def test_empty_run_after_a_normal_run_leaves_nothing_to_reuse(project):
    root, warehouse, state = project
    add_snapshot(root)
    build(root)
    written = records(state)

    empty = build(root, "--empty")
    assert not any(status == "reused" for status, _ in empty.values())
    assert not any("REUSED" in message for _, message in empty.values())
    assert empty["fct_orders_doubles"] == ("pass", "")
    assert empty["not_null_fct_orders_id"] == ("pass", "")  # run by dbt, not replayed
    assert records(state) == written  # hodor stood aside, nothing recorded
    # a snapshot is not emptied by --empty: its rows stay current
    assert count(warehouse, "main.orders_snapshot where dbt_valid_to is null") == 2
    assert count(warehouse, "main.stg_orders") == 0
    assert count(warehouse, "main.countries") == 0

    # the normal run compares against the last normal record and finds changed data
    after = build(root)
    for name in ALL[:4]:
        assert after[name][0] == "success", name
        assert latest(state)[name][2] == "build", name
    assert after["orders_snapshot"][0] == "reused"  # its data was never emptied
    assert latest(state)["countries"][:2] == ("build", "target_modified")
    assert count(warehouse, "main.stg_orders") == 2
    assert count(warehouse, "main.fct_orders") == 2
    assert count(warehouse, "main.countries") == 1


def test_empty_run_first_ever_leaves_nothing_to_reuse(project):
    root, warehouse, state = project
    add_snapshot(root)
    empty = build(root, "--empty")
    assert not any(status == "reused" for status, _ in empty.values())
    assert records(state) == []

    build(root)
    for name in ALL:
        assert latest(state)[name][:2] == ("build", "no_history"), name
    assert count(warehouse, "main.countries") == 1
    assert count(warehouse, "main.stg_orders") == 2
    assert count(warehouse, "main.fct_orders") == 2


# -- failures -----------------------------------------------------------------------------


def add_failing_model(root: Path) -> Path:
    bad = root / "models" / "bad.sql"
    bad.write_text(
        "{{ config(materialized='table') }} select cast('x' as integer) as x "
        "from {{ ref('stg_orders') }}\n",
        encoding="utf-8",
    )
    return bad


def test_fail_fast_still_flushes_the_records_of_finished_nodes(project):
    root, _, state = project
    add_failing_model(root)
    res = invoke(root, "build", "--fail-fast")
    assert not res.success  # dbt's own exit status
    outcome = results(res)
    assert outcome["bad"][0] == "error"
    assert outcome["rpt_orders"] == ("skipped", "Skipping due to fail_fast")

    written = {name: (action, status) for name, _, _, action, status in records(state)}
    assert written["bad"] == ("build", "error")
    assert written["stg_orders"] == ("build", "success")
    assert "rpt_orders" not in written  # skipped nodes have no record
    for name, (status, _) in outcome.items():
        if status == "success":
            assert written[name] == ("build", "success"), name


def test_retry_runs_unpatched_and_writes_no_records(project):
    root, _, state = project
    bad = add_failing_model(root)
    assert not invoke(root, "build").success
    written = records(state)

    bad.write_text(bad.read_text().replace("'x'", "'1'"), encoding="utf-8")
    res = invoke(root, "retry")
    assert res.success, res.exception
    assert unpatched()  # retry is not an enabled command
    assert records(state) == written


def test_defer_with_an_unreadable_state_fails_in_dbt_not_in_hodor(project, monkeypatch):
    root, _, state = project
    (root / "corrupt").mkdir()
    (root / "corrupt" / "manifest.json").write_text("{not json", encoding="utf-8")
    failures = {}
    for mode in ("on", "off"):
        monkeypatch.setenv("HODOR_MODE", mode)
        for path in ("missing", "corrupt"):
            res = invoke(root, "build", "--defer", "--state", path)
            assert not res.success
            failures[mode, path] = str(res.exception)
    # dbt reads the state manifest first and stops with the same error with or without hodor
    for path in ("missing", "corrupt"):
        assert failures["on", path] == failures["off", path]
    assert "Could not find manifest" in failures["on", "missing"]
    assert "Could not read WritableManifest" in failures["on", "corrupt"]
    assert records(state) == []


# -- flags --------------------------------------------------------------------------------


def set_config(root: Path, model: str, config: str) -> None:
    path = root / "models" / f"{model}.sql"
    path.write_text(f"{{{{ config({config}) }}}}\n" + path.read_text(), encoding="utf-8")


def test_node_full_refresh_config_wins_over_the_flag(project):
    root, _, state = project
    set_config(root, "fct_orders", "materialized='table', full_refresh=false")
    set_config(root, "rpt_orders", "materialized='table', full_refresh=true")
    build(root)

    # the flag does not reach a node that sets full_refresh: false
    build(root, "-s", "fct_orders", "--full-refresh")
    assert latest(state)["fct_orders"] == ("reuse", "no_new_data", "reuse")

    # full_refresh: true builds without the flag
    build(root, "-s", "rpt_orders")
    assert latest(state)["rpt_orders"] == ("build", "full_refresh", "build")


@pytest.mark.parametrize("threads", ["1", "4"])
def test_dbt_threads_do_not_change_decisions(project, monkeypatch, threads):
    root, warehouse, state = project
    monkeypatch.setenv("CORE_E2E_THREADS", threads)
    nodes = ("stg_orders", "countries", "fct_orders", "not_null_fct_orders_id", "rpt_orders")

    def decisions() -> dict[str, tuple[str, str]]:
        seen = latest(state)
        return {n: (seen[n][0], seen[n][1]) for n in nodes}

    build(root)
    assert set(decisions().values()) == {("build", "no_history")}
    build(root)
    assert set(decisions().values()) == {("reuse", "no_new_data")}
    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.orders values (3, 30)")
    build(root)
    assert decisions() == {
        "stg_orders": ("build", "new_upstream_data"),
        "countries": ("reuse", "no_new_data"),
        "fct_orders": ("build", "new_upstream_data"),
        "not_null_fct_orders_id": ("build", "new_upstream_data"),
        "rpt_orders": ("build", "new_upstream_data"),
    }

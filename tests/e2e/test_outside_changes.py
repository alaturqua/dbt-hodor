"""End to end on dbt-duckdb: changes hodor's records cannot show, made by runs without hodor or
by values rendered only in hooks, must never lead to a reuse."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from dbt.cli.main import dbtRunner  # noqa: E402

FILES = {
    "dbt_project.yml": """
        name: outside
        version: "1.0"
        profile: outside
        models:
          outside:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        outside:
          target: local
          outputs:
            local:
              type: duckdb
              path: "{{ env_var('OUTSIDE_WH') }}"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('OUTSIDE_STATE') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            schema: raw
            tables:
              - name: orders
    """,
    "models/v.sql": """
        {{ config(materialized='view') }}
        select id, amount * {{ var('k', 1) }} as amount from {{ source('raw', 'orders') }}
    """,
    "models/t.sql": """
        {{ config(materialized='table') }}
        select sum(amount) as s from {{ ref('v') }}
    """,
    "models/hooked.sql": """
        {{ config(materialized='table',
                  post_hook="delete from {{ this }} where id = {{ var('drop_id', 0) }}") }}
        select id from {{ source('raw', 'orders') }}
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
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create schema raw")
        con.execute(
            "create table raw.orders as select * from (values (1, 10), (2, 20)) v(id, amount)"
        )
    monkeypatch.setenv("OUTSIDE_WH", str(warehouse))
    monkeypatch.setenv("OUTSIDE_STATE", str(tmp_path / "state.duckdb"))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    return root, warehouse, tmp_path / "state.duckdb"


def dbt(root: Path, *args: str) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), *args]
    )
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def reason(state: Path, name: str) -> str:
    with duckdb.connect(str(state), read_only=True) as con:
        return con.execute(
            "select reason from main.hodor_state where unique_id = ? "
            "order by recorded_at_ms desc, record_id limit 1",
            [f"model.outside.{name}"],
        ).fetchone()[0]


def query(warehouse: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(warehouse)) as con:
        return con.execute(sql).fetchall()


def test_a_view_replaced_by_a_run_without_hodor_builds(project, monkeypatch):
    root, warehouse, state = project
    dbt(root)
    monkeypatch.setenv("HODOR_MODE", "off")
    dbt(root, "--vars", "{k: 10}")  # replaces the view; hodor records nothing
    monkeypatch.setenv("HODOR_MODE", "on")
    out = dbt(root)
    assert out["v"][0] == "success", out
    assert reason(state, "v") == "target_modified"
    assert query(warehouse, "select s from main.t") == [(30,)]
    assert dbt(root)["v"] == ("reused", "REUSED (view_unchanged)")


def test_a_value_rendered_in_a_hook_is_part_of_the_config(project):
    root, warehouse, state = project
    dbt(root)
    out = dbt(root, "--vars", "{drop_id: 1}")
    assert out["hooked"][0] == "success", out
    assert reason(state, "hooked") == "config_changed"
    assert query(warehouse, "select count(*) from main.hooked") == [(1,)]
    assert dbt(root, "--vars", "{drop_id: 1}")["hooked"][0] == "reused"


def test_a_view_hodor_never_built_counts_as_unknown(project, monkeypatch):
    root, warehouse, state = project
    monkeypatch.setenv("HODOR_MODE", "off")
    dbt(root)
    monkeypatch.setenv("HODOR_MODE", "on")
    dbt(root, "--exclude", "v")
    monkeypatch.setenv("HODOR_MODE", "off")
    dbt(root, "-s", "v", "--vars", "{k: 10}")  # same file, other SQL
    monkeypatch.setenv("HODOR_MODE", "on")
    dbt(root, "--exclude", "v")
    assert reason(state, "t") == "unknown_upstream"
    assert query(warehouse, "select s from main.t") == [(300,)]


def test_a_hook_that_fails_while_it_runs_on_reuse_fails_the_node(project):
    root, warehouse, state = project
    (root / "models" / "audited.sql").write_text(
        "{{ config(materialized='table', state={'execute_hooks_on_reuse': true},\n"
        "          post_hook=[{'sql': 'insert into raw.audit values (1)', 'transaction': false},\n"
        "                     {'sql': 'select * from raw.gone', 'transaction': false}]) }}\n"
        "select id from {{ source('raw', 'orders') }}\n",
        encoding="utf-8",
    )
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create table raw.audit (n int)")
        con.execute("create table raw.gone (n int)")
    dbt(root, "-s", "audited")
    with duckdb.connect(str(warehouse)) as con:
        con.execute("drop table raw.gone")
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), "-s", "audited"]
    )
    assert not result.success
    assert str(result.result.results[0].status) == "error"
    # one insert from the build, one from the reuse, none from a second build
    assert query(warehouse, "select count(*) from raw.audit") == [(2,)]

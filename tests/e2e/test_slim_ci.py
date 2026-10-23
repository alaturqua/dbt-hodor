"""End to end: slim CI on dbt-duckdb. A prod target and a CI target in another database (the
CI catalog) with PR schemas (pr_<nr>_<schema>) and --defer to the prod manifest."""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from dbt.cli.main import dbtRunner  # noqa: E402

FILES = {
    "dbt_project.yml": """
        name: ci_e2e
        version: "1.0"
        profile: ci_e2e
        models:
          ci_e2e:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        ci_e2e:
          target: prod
          outputs:
            prod:
              type: duckdb
              path: "{{ env_var('E2E_PROD_DB') }}"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
            ci:
              type: duckdb
              path: "{{ env_var('E2E_CI_DB') }}"
              schema: main
              threads: 2
              attach:
                - path: "{{ env_var('E2E_PROD_DB') }}"
                  alias: prod
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
            ci_own:  # a CI target whose records live with the pull request
              type: duckdb
              path: "{{ env_var('E2E_CI_DB') }}"
              schema: main
              threads: 2
              attach:
                - path: "{{ env_var('E2E_PROD_DB') }}"
                  alias: prod
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_TMP') }}/state_pr_{{ env_var('PR_NUMBER') }}.duckdb"
    """,
    "macros/generate_schema_name.sql": """
        {% macro generate_schema_name(custom_schema_name, node) -%}
          {%- set base = custom_schema_name | trim if custom_schema_name else target.schema -%}
          {%- if target.name.startswith('ci') -%}pr_{{ env_var('PR_NUMBER') }}_{{ base }}
          {%- else -%}{{ base }}{%- endif -%}
        {%- endmacro %}
    """,
    "macros/add_order.sql": """
        {% macro add_order(id, amount) %}
          {% do run_query("insert into prod.raw.orders values (" ~ id ~ ", " ~ amount ~ ")") %}
        {% endmacro %}
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            database: prod
            schema: raw
            tables:
              - name: orders
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table', schema='staging') }}
        select id, amount from {{ source('raw', 'orders') }}
    """,
    "models/int_orders.sql": """
        {{ config(materialized='view', schema='staging') }}
        select id, amount from {{ ref('stg_orders') }}
    """,
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2 from {{ ref('int_orders') }}
    """,
    "models/rpt_orders.sql": """
        {{ config(materialized='table') }}
        select count(*) as n from {{ ref('fct_orders') }}
    """,
    "models/dim_parity.sql": """
        {{ config(materialized='table') }}
        select distinct id % 2 as parity from {{ ref('stg_orders') }}
    """,
    "models/inc_orders.sql": """
        {{ config(materialized='incremental', unique_key='id',
                  incremental_strategy='delete+insert') }}
        select id, amount from {{ ref('stg_orders') }}
        {% if is_incremental() %}
        where id > (select coalesce(max(id), 0) from {{ this }})
        {% endif %}
    """,
    "models/schema.yml": """
        models:
          - name: fct_orders
            columns:
              - name: id
                data_tests: [not_null]
          - name: dim_parity
            columns:
              - name: parity
                data_tests: [not_null]
    """,
}

DEFER = ("--defer", "--state", "prod_state")


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    prod_db, ci_db, state = (
        tmp_path / "prod.duckdb",
        tmp_path / "ci.duckdb",
        tmp_path / "state.duckdb",
    )
    with duckdb.connect(str(prod_db)) as con:
        con.execute("create schema raw")
        con.execute(
            "create table raw.orders as select * from (values (1, 10), (2, 20)) v(id, amount)"
        )
    duckdb.connect(str(ci_db)).close()
    monkeypatch.setenv("E2E_PROD_DB", str(prod_db))
    monkeypatch.setenv("E2E_CI_DB", str(ci_db))
    monkeypatch.setenv("E2E_STATE", str(state))
    monkeypatch.setenv("E2E_TMP", tmp_path.as_posix())
    monkeypatch.setenv("PR_NUMBER", "123")
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    monkeypatch.chdir(root)  # --state is relative to the project
    return root, prod_db, ci_db, state


def dbt(root: Path, target: str, *args: str) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(
        [
            "build",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            "--target",
            target,
            *args,
        ]
    )
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def deploy_prod(root: Path) -> dict[str, tuple[str, str]]:
    """A prod run, then its manifest becomes the state CI defers to."""
    out = dbt(root, "prod")
    state = root / "prod_state"
    state.mkdir(exist_ok=True)
    shutil.copy(root / "target" / "manifest.json", state / "manifest.json")
    return out


def decisions(state: Path, target: str) -> dict[str, tuple[str, str]]:
    """Latest (action, reason) per node of one target; tests by their short name."""
    with duckdb.connect(str(state), read_only=True) as con:
        rows = con.execute(
            "select unique_id, action, reason from main.hodor_state where target_name = ? "
            "order by recorded_at_ms, record_id",
            [target],
        ).fetchall()
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def edit(root: Path, model: str, old: str, new: str) -> None:
    path = root / "models" / f"{model}.sql"
    path.write_text(path.read_text().replace(old, new), encoding="utf-8")


def add_orders(prod_db: Path, *rows: tuple[int, int]) -> None:
    """Through dbt: dbt-duckdb keeps prod.duckdb attached, and Windows locks the file."""
    root = Path.cwd()
    for id_, amount in rows:
        result = dbtRunner().invoke(
            [
                "run-operation",
                "add_order",
                "--args",
                f"{{id: {id_}, amount: {amount}}}",
                "--project-dir",
                str(root),
                "--profiles-dir",
                str(root),
                "--target",
                "prod",
            ]
        )
        assert result.success, result.exception


def count(db: Path, relation: str) -> int:
    with duckdb.connect(str(db)) as con:  # dbt-duckdb keeps the file open read-write
        return con.execute(f"select count(*) from {relation}").fetchone()[0]


def test_every_push_reuses_the_last_ci_build(project):
    root, prod_db, ci_db, state = project
    deploy_prod(root)
    edit(root, "fct_orders", "amount * 2", "amount * 3")
    modified = ("-s", "state:modified+", *DEFER)

    # push 1: the modified model and its children build into the PR schema
    first = dbt(root, "ci", *modified)
    assert set(first) == {"fct_orders", "rpt_orders", "not_null_fct_orders_id"}
    assert all(status in ("success", "pass") for status, _ in first.values())
    assert count(ci_db, "pr_123_main.fct_orders") == 2

    # push 2, same code: fct_orders reads prod's int_orders view through defer, and hodor
    # follows it to prod's stg_orders table, so nothing builds again
    second = dbt(root, "ci", *modified)
    assert second["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["rpt_orders"] == ("reused", "REUSED (no_new_data)")
    assert decisions(state, "ci")["not_null_fct_orders_id"] == ("reuse", "no_new_data")

    # prod gets new data: the PR's models build again on the next push
    add_orders(prod_db, (3, 30))
    edit(root, "fct_orders", "amount * 3", "amount * 2")  # prod deploys main, not the PR
    deploy_prod(root)
    edit(root, "fct_orders", "amount * 2", "amount * 3")
    dbt(root, "ci", *modified)
    now = decisions(state, "ci")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["rpt_orders"] == ("build", "new_upstream_data")
    assert count(ci_db, "pr_123_main.fct_orders") == 3


def test_a_stale_ci_parent_never_hides_new_prod_data(project):
    """--favor-state reads prod even when an older CI copy of the parent exists."""
    root, prod_db, ci_db, state = project
    deploy_prod(root)
    edit(root, "stg_orders", "select id", "select distinct id")
    dbt(root, "ci", "-s", "state:modified+", *DEFER)  # builds stg_orders and fct_orders in CI
    assert count(ci_db, "pr_123_staging.stg_orders") == 2

    edit(root, "stg_orders", "select distinct id", "select id")  # the PR drops that change
    favor = ("-s", "fct_orders", *DEFER, "--favor-state")
    dbt(root, "ci", *favor)
    add_orders(prod_db, (5, 50))
    deploy_prod(root)
    dbt(root, "ci", *favor)
    assert decisions(state, "ci")["fct_orders"][0] == "build"
    assert count(ci_db, "pr_123_main.fct_orders") == 3


def test_deploys_build_changed_and_added_models_and_ignore_removed_ones(project):
    root, _, _, state = project
    deploy_prod(root)
    edit(root, "fct_orders", "amount * 2", "amount * 4")  # changed
    (root / "models" / "new_model.sql").write_text(  # added
        "{{ config(materialized='table') }} select 1 as x\n", encoding="utf-8"
    )
    (root / "models" / "dim_parity.sql").unlink()  # removed, with its test
    schema = root / "models" / "schema.yml"
    schema.write_text(schema.read_text().split("  - name: dim_parity")[0], encoding="utf-8")
    deploy = deploy_prod(root)
    now = decisions(state, "prod")
    assert now["fct_orders"] == ("build", "code_changed")
    assert now["rpt_orders"] == ("build", "new_upstream_data")
    assert now["new_model"] == ("build", "no_history")
    assert deploy["stg_orders"] == ("reused", "REUSED (no_new_data)")
    assert "dim_parity" not in deploy


def test_a_modified_view_rebuilds_its_children(project):
    """The children of a changed view read new rows: they are not reused."""
    root, _, ci_db, state = project
    deploy_prod(root)
    edit(root, "int_orders", "select id, amount", "select id, amount + 100 as amount")
    dbt(root, "ci", "-s", "state:modified+", *DEFER)
    with duckdb.connect(str(ci_db)) as con:
        rows = con.execute("select * from pr_123_main.fct_orders order by id").fetchall()
    assert rows == [(1, 220), (2, 240)]

    deploy_prod(root)  # the same change on one target: the children build, not reuse
    now = decisions(state, "prod")
    assert now["fct_orders"] == ("build", "upstream_code_changed")
    assert now["not_null_fct_orders_id"][0] == "build"


def test_a_view_rebuilds_when_it_reads_another_relation(project):
    root, _, ci_db, state = project
    deploy_prod(root)
    dbt(root, "ci", "-s", "int_orders", *DEFER, "--favor-state")  # reads prod's stg_orders
    out = dbt(root, "ci", "-s", "stg_orders", "int_orders")  # now reads the CI stg_orders
    assert out["int_orders"] == ("success", "OK"), out
    assert decisions(state, "ci")["int_orders"] == ("build", "code_changed")
    with duckdb.connect(str(ci_db)) as con:
        sql = con.execute(
            "select sql from duckdb_views() where view_name = 'int_orders'"
        ).fetchone()[0]
    assert "pr_123_staging" in sql


def test_ci_records_can_live_in_a_state_table_of_their_own(project, tmp_path):
    root, _, _, state = project
    deploy_prod(root)
    assert dbt(root, "ci_own", "-s", "dim_parity", *DEFER)["dim_parity"] == ("success", "OK")
    assert dbt(root, "ci_own", "-s", "dim_parity", *DEFER)["dim_parity"][0] == "reused"
    pr_state = tmp_path / "state_pr_123.duckdb"
    assert decisions(pr_state, "ci_own")["dim_parity"] == ("reuse", "no_new_data")
    assert decisions(state, "ci_own") == {}  # the base state table holds no CI record


def test_a_broken_recorded_test_result_runs_the_test(project):
    root, _, _, state = project
    deploy_prod(root)
    with duckdb.connect(str(state)) as con:
        con.execute(
            'update main.hodor_state set test_result = \'{"failures": "x"}\' '
            "where resource_type = 'test'"
        )
    out = deploy_prod(root)
    assert out["not_null_dim_parity_parity"] == ("pass", ""), out
    assert decisions(state, "prod")["not_null_dim_parity_parity"] == ("build", "hodor_error")


def test_a_dbt_clone_builds_once_then_reuses(project):
    """A table made by dbt clone has no record: hodor builds it once."""
    root, _, ci_db, state = project
    deploy_prod(root)
    result = dbtRunner().invoke(
        ["clone", "-s", "dim_parity", "--state", "prod_state", "--target", "ci"]
        + ["--project-dir", str(root), "--profiles-dir", str(root)]
    )
    assert result.success, result.exception
    assert count(ci_db, "pr_123_main.dim_parity") == 2
    assert dbt(root, "ci", "-s", "dim_parity", *DEFER)["dim_parity"] == ("success", "OK")
    assert decisions(state, "ci")["dim_parity"] == ("build", "no_history")
    assert dbt(root, "ci", "-s", "dim_parity", *DEFER)["dim_parity"][0] == "reused"


def test_an_upgrade_from_hash_version_1_builds_once(project):
    root, _, _, state = project
    deploy_prod(root)
    with duckdb.connect(str(state)) as con:  # records as an older hodor wrote them
        con.execute(
            "update main.hodor_state set hash_version = '1', code_hash = 'v1', "
            """sql_facts = json_merge_patch(sql_facts, '{"code_hash": "v1"}')"""
        )
    deploy_prod(root)
    assert decisions(state, "prod")["dim_parity"] == ("build", "code_changed")
    assert deploy_prod(root)["dim_parity"][0] == "reused"


def fct_rows(db: Path) -> list[tuple]:
    with duckdb.connect(str(db)) as con:
        return con.execute("select * from pr_123_main.fct_orders order by id").fetchall()


def test_a_table_rebuilds_when_it_switches_from_the_base_view_to_the_ci_view(project):
    root, _, ci_db, state = project
    deploy_prod(root)
    edit(root, "int_orders", "select id, amount", "select id, amount + 100 as amount")
    dbt(root, "ci", "-s", "fct_orders", *DEFER)  # reads prod's int_orders, the old SQL
    assert fct_rows(ci_db) == [(1, 20), (2, 40)]
    dbt(root, "ci", "-s", "int_orders", "fct_orders", *DEFER)  # now the CI int_orders
    assert decisions(state, "ci")["fct_orders"] == ("build", "upstream_code_changed")
    assert fct_rows(ci_db) == [(1, 220), (2, 240)]


def test_a_view_changed_through_a_macro_rebuilds_its_children(project):
    root, _, ci_db, state = project
    macro = root / "macros" / "bump.sql"
    macro.write_text("{% macro bump(x) %}{{ x }}{% endmacro %}\n", encoding="utf-8")
    edit(root, "int_orders", "select id, amount", "select id, {{ bump('amount') }} as amount")
    deploy_prod(root)
    macro.write_text("{% macro bump(x) %}{{ x }} + 100{% endmacro %}\n", encoding="utf-8")
    dbt(root, "ci", "-s", "state:modified+", *DEFER)
    assert fct_rows(ci_db) == [(1, 220), (2, 240)]


def test_unrendered_code_still_sees_a_view_read_another_relation(project):
    root, _, ci_db, state = project
    model = root / "models" / "int_orders.sql"
    model.write_text(
        "{{ config(state={'compare_unrendered_code': true}) }}\n" + model.read_text(),
        encoding="utf-8",
    )
    deploy_prod(root)
    dbt(root, "ci", "-s", "int_orders", *DEFER, "--favor-state")  # reads prod's stg_orders
    dbt(root, "ci", "-s", "stg_orders", "int_orders")  # now reads the CI stg_orders
    assert decisions(state, "ci")["int_orders"] == ("build", "code_changed")

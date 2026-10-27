"""End to end: real dbt-duckdb runs in-process with hodor loaded as a dbt plugin."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from dbt.cli.main import dbtRunner  # noqa: E402

from dbt_hodor import cli  # noqa: E402

FILES = {
    "dbt_project.yml": """
        name: hodor_e2e
        version: "1.0"
        profile: hodor_e2e
        models:
          hodor_e2e:
            +state:
              lag_tolerance: 0s
        snapshots:
          hodor_e2e:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        hodor_e2e:
          target: local
          outputs:
            local:
              type: duckdb
              path: "{{ env_var('HODOR_E2E_WAREHOUSE') }}"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('HODOR_E2E_STATE') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            schema: raw
            tables:
              - name: orders
    """,
    "models/stg_orders.sql": "select id, amount from {{ source('raw', 'orders') }}",
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2, '{{ invocation_id }}' as loaded_by
        from {{ ref('stg_orders') }}
    """,
    "models/inc_orders.sql": """
        {{ config(
            materialized='incremental', unique_key='id', incremental_strategy='delete+insert'
        ) }}
        select id, amount from {{ ref('stg_orders') }}
        {% if is_incremental() %}
        where id > (select coalesce(max(id), 0) from {{ this }})
        {% endif %}
    """,
    "models/star_view.sql": "select * from {{ source('raw', 'orders') }}",
    "models/numbers.sql": "{{ config(materialized='table') }} select * from range(3)",
    "models/hooked.sql": """
        {{ config(materialized='table', post_hook="insert into main.audit select 1") }}
        select id from {{ ref('stg_orders') }}
    """,
    "seeds/countries.csv": """
        code,name
        de,Germany
        fr,France
    """,
    "models/dim_country.sql": """
        {{ config(materialized='table') }}
        select code, upper(name) as name from {{ ref('countries') }}
    """,
    "snapshots/orders_snapshot.yml": """
        snapshots:
          - name: orders_snapshot
            relation: source('raw', 'orders')
            config:
              unique_key: id
              strategy: check
              check_cols: all
    """,
    "models/schema.yml": """
        models:
          - name: fct_orders
            columns:
              - name: id
                data_tests: [not_null, unique]
          - name: hooked
            config:
              state:
                execute_hooks_on_any_reuse: true
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
        con.execute("create table main.audit (x int)")
        con.execute(
            "create table raw.orders as select * from (values (1, 10), (2, 20)) v(id, amount)"
        )
    monkeypatch.setenv("HODOR_E2E_WAREHOUSE", str(warehouse))
    monkeypatch.setenv("HODOR_E2E_STATE", str(state))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    return root, warehouse, state


def dbt(root: Path, *args: str) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), *args]
    )
    assert result.success, result.exception
    return {
        r.node.name + (f"_v{r.node.version}" if getattr(r.node, "version", None) else ""): (
            str(r.status),
            r.message or "",
        )
        for r in result.result.results
    }


def records(state: Path, invocation: int | None = None) -> list[tuple]:
    with duckdb.connect(str(state), read_only=True) as con:
        return con.execute(
            "select unique_id, mode, decision, reason, action, status from main.hodor_state "
            "order by recorded_at_ms, unique_id"
        ).fetchall()


def decisions(state: Path) -> dict[str, tuple[str, str]]:
    """Latest (action, reason) per node name."""
    out = {}
    for unique_id, _, _, reason, action, _ in records(state):
        out[unique_id if unique_id.startswith("test.") else "_".join(unique_id.split(".")[2:])] = (
            action,
            reason,
        )
    return out


def test_full_lifecycle(project):
    root, warehouse, state = project

    # 1. first run: nothing recorded yet
    first = dbt(root)
    assert {status for status, _ in first.values()} <= {"success", "pass"}
    assert all(reason == "no_history" for _, reason in decisions(state).values())

    # 2. nothing changed
    second = dbt(root)
    assert second["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["stg_orders"] == ("reused", "REUSED (view_unchanged)")
    assert second["countries"] == ("reused", "REUSED (no_new_data)")
    assert second["dim_country"] == ("reused", "REUSED (no_new_data)")
    assert second["orders_snapshot"] == ("reused", "REUSED (no_new_data)")
    assert second["star_view"][0] == "success"  # select * from a source always builds
    assert second["numbers"][0] == "success"  # range(3) is an untraceable input
    now = decisions(state)
    assert now["star_view"] == ("build", "select_star_view")
    assert now["numbers"] == ("build", "unknown_upstream")
    assert now["inc_orders"] == ("build", "code_changed")  # first is_incremental() run
    tests = [k for k in now if k.startswith("test.")]
    assert tests and all(now[t] == ("reuse", "no_new_data") for t in tests)

    # 3. the incremental model settles too
    dbt(root)
    assert decisions(state)["inc_orders"] == ("reuse", "no_new_data")

    # 4. new upstream data
    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.orders values (3, 30)")
    dbt(root)
    now = decisions(state)
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["inc_orders"] == ("build", "new_upstream_data")
    assert now["stg_orders"] == ("reuse", "view_unchanged")
    assert now["orders_snapshot"] == ("build", "new_upstream_data")
    assert now["dim_country"] == ("reuse", "no_new_data")
    assert all(now[t] == ("build", "new_upstream_data") for t in tests)
    with duckdb.connect(str(warehouse)) as con:  # same config as dbt-duckdb in this process
        assert con.execute("select count(*) from main.inc_orders").fetchone()[0] == 3

    # 5. a changed seed file rebuilds the seed and its children
    seed = root / "seeds" / "countries.csv"
    seed.write_text(seed.read_text() + "it,Italy\n", encoding="utf-8")
    dbt(root)
    now = decisions(state)
    assert now["countries"] == ("build", "code_changed")
    assert now["dim_country"] == ("build", "new_upstream_data")
    assert now["orders_snapshot"] == ("reuse", "no_new_data")
    with duckdb.connect(str(warehouse)) as con:
        assert con.execute("select count(*) from main.dim_country").fetchone()[0] == 3

    # 6. a code change
    model = root / "models" / "fct_orders.sql"
    model.write_text(model.read_text().replace("amount * 2", "amount * 3"), encoding="utf-8")
    dbt(root)
    assert decisions(state)["fct_orders"] == ("build", "code_changed")

    # 7. a write that bypassed dbt
    with duckdb.connect(str(warehouse)) as con:
        con.execute("delete from main.fct_orders where id = 1")
    dbt(root)
    assert decisions(state)["fct_orders"] == ("build", "target_modified")

    # 8. --full-refresh builds everything
    dbt(root, "--full-refresh")
    assert decisions(state)["fct_orders"] == ("build", "full_refresh")


def test_shadow_mode_builds_everything_but_records_decisions(project, monkeypatch):
    root, _, state = project
    dbt(root)
    monkeypatch.setenv("HODOR_MODE", "shadow")
    shadow = dbt(root)
    assert all(status in ("success", "pass") for status, _ in shadow.values())
    last = {r[0]: r for r in records(state)}
    fct = last["model.hodor_e2e.fct_orders"]
    assert fct[1:] == ("shadow", "reuse", "no_new_data", "build", "success")
    # shadow builds are history, so `on` can reuse right away
    monkeypatch.setenv("HODOR_MODE", "on")
    assert dbt(root)["fct_orders"][0] == "reused"


def test_off_mode_records_nothing(project, monkeypatch):
    root, _, state = project
    monkeypatch.setenv("HODOR_MODE", "off")
    dbt(root)
    assert not state.exists()


def test_hooks_run_on_reuse_when_asked(project):
    root, warehouse, _ = project
    dbt(root)
    second = dbt(root)
    # lag_tolerance 0s from the project and the hook flag from the model both apply
    assert second["hooked"] == ("reused", "REUSED (no_new_data)")
    with duckdb.connect(str(warehouse)) as con:
        assert con.execute("select count(*) from main.audit").fetchone()[0] == 2


def test_vars_set_the_mode_for_one_invocation(project, monkeypatch):
    root, _, state = project
    monkeypatch.delenv("HODOR_MODE")
    dbt(root, "--vars", "{hodor_mode: off}")  # unquoted YAML off is a boolean
    assert not state.exists()
    dbt(root, "--vars", "{hodor_mode: on}")
    assert dbt(root, "--vars", "{hodor_mode: on}")["fct_orders"][0] == "reused"
    monkeypatch.setenv("HODOR_MODE", "shadow")  # the env var wins over --vars
    assert dbt(root, "--vars", "{hodor_mode: on}")["fct_orders"][0] == "success"


VERSIONED = {
    "models/customers_v1.sql": "{{ config(materialized='table') }} select id from "
    "{{ source('raw', 'orders') }}",
    "models/customers_v2.sql": "{{ config(materialized='table') }} select id, amount from "
    "{{ source('raw', 'orders') }}",
    "models/customer_count.sql": "{{ config(materialized='table') }} select count(*) as n "
    "from {{ ref('customers') }}",
    "models/versions.yml": """
        models:
          - name: customers
            latest_version: 2
            versions:
              - v: 1
              - v: 2
    """,
}


def test_model_versions_are_separate_nodes(project):
    root, _, state = project
    for name, body in VERSIONED.items():
        (root / name).write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    dbt(root)
    second = dbt(root)
    assert second["customers_v1"][0] == second["customers_v2"][0] == "reused"
    assert second["customer_count"][0] == "reused"

    # moving latest_version changes what ref('customers') compiles to
    versions = root / "models" / "versions.yml"
    versions.write_text(versions.read_text().replace("latest_version: 2", "latest_version: 1"))
    dbt(root)
    now = decisions(state)
    assert now["customer_count"] == ("build", "code_changed")
    assert now["customers_v1"] == now["customers_v2"] == ("reuse", "no_new_data")


CONSTRAINED = {
    "models/crm.yml": """
        sources:
          - name: crm
            schema: raw
            tables:
              - name: customers
    """,
    "models/dim_customer.sql": "{{ config(materialized='table') }} select id from "
    "{{ source('crm', 'customers') }}",
    "models/fct_sales.sql": "{{ config(materialized='table') }} select id, amount from "
    "{{ source('raw', 'orders') }}",
    "models/constraints.yml": """
        models:
          - name: fct_sales
            config:
              contract: {enforced: true}
            constraints:
              - type: primary_key
                columns: [id]
            columns:
              - name: id
                data_type: integer
                constraints: [{type: not_null}]
                data_tests:
                  - relationships:
                      arguments: {to: "ref('dim_customer')", field: id}
              - name: amount
                data_type: integer
    """,
}


def test_contracts_constraints_and_relationships(project):
    root, warehouse, state = project
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create table raw.customers as select * from (values (1), (2)) v(id)")
    for name, body in CONSTRAINED.items():
        (root / name).write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    dbt(root)
    second = dbt(root)
    assert second["fct_sales"] == ("reused", "REUSED (no_new_data)")
    rel_id = next(k for k in decisions(state) if k.startswith("test.hodor_e2e.relationships_"))
    assert decisions(state)[rel_id] == ("reuse", "no_new_data")

    # the relationships test reads both models: new data on the `to` side reruns it
    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.customers values (3)")
    dbt(root)
    now = decisions(state)
    assert now["dim_customer"] == ("build", "new_upstream_data")
    assert now["fct_sales"] == ("reuse", "no_new_data")
    assert now[rel_id] == ("build", "new_upstream_data")

    # a model-level constraint is not in config or columns, but it still rebuilds the table
    spec = root / "models" / "constraints.yml"
    spec.write_text(spec.read_text().replace("columns: [id]", "columns: [id, amount]"))
    dbt(root)
    assert decisions(state)["fct_sales"] == ("build", "config_changed")


VOLATILE = {
    "models/stamped.sql": "{{ config(materialized='table') }} select id, current_timestamp as ts, "
    "current_date as d, current_user as u from {{ ref('stg_orders') }}",
    "models/windowed.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_date >= date '2000-01-01'",
    "models/by_user.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_user is not null",
    "models/clocked.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_timestamp > timestamp '2000-01-01'",
    "models/forced.sql": "{{ config(materialized='table') }} select id, current_date as d from "
    "{{ ref('stg_orders') }}",
    "models/never.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_date >= date '2000-01-01'",
    "models/volatile.yml": """
        models:
          - name: clocked
            config: {state: {lag_tolerance: 1h}}
          - name: forced
            config: {state: {evaluate_volatile_sql: true}}
          - name: never
            config: {state: {evaluate_volatile_sql: false}}
    """,
    "tests/no_future_orders.sql": "select * from {{ ref('stamped') }} where ts > current_timestamp",
}


def test_volatile_sql(project, monkeypatch):
    from dbt_hodor.providers import DuckDBProvider

    root, _, state = project
    for name, body in VOLATILE.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    dbt(root)
    dbt(root)
    now = decisions(state)
    assert now["stamped"] == ("reuse", "no_new_data")  # stamp columns only
    assert now["windowed"] == ("reuse", "no_new_data")  # same date
    assert now["by_user"] == ("reuse", "no_new_data")  # same user
    assert now["clocked"] == ("reuse", "within_lag_tolerance")  # the clock moved, 1h tolerance
    assert now["forced"] == now["never"] == ("reuse", "no_new_data")
    assert now["test.hodor_e2e.no_future_orders"] == ("build", "volatile_sql")  # tolerance 0

    # the next day
    real = DuckDBProvider.volatile_values
    monkeypatch.setattr(
        DuckDBProvider, "volatile_values", lambda self: {**real(self), "date": "2999-01-01"}
    )
    dbt(root)
    now = decisions(state)
    assert now["windowed"] == ("build", "volatile_sql")
    assert now["forced"] == ("build", "volatile_sql")  # evaluate_volatile_sql: true counts stamps
    assert now["stamped"] == ("reuse", "no_new_data")
    assert now["never"] == ("reuse", "no_new_data")  # evaluate_volatile_sql: false
    assert now["by_user"] == ("reuse", "no_new_data")


OVERRIDE = """
{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql',
    partitioned_by=none, sorted_by=none) -%}
  {%- if relation.identifier.startswith('fct_orders') -%}
    {%- set compiled_code = 'select *, 1 as _audit from (' ~ compiled_code ~ ') as src' -%}
  {%- endif -%}
  {{ return(dbt.duckdb__create_table_as(temporary, relation, compiled_code, language,
    partitioned_by, sorted_by)) }}
{%- endmacro %}

{% macro audit_row() %}insert into main.audit select 1{% endmacro %}
"""


def test_project_macros_in_materializations_and_hooks(project):
    root, warehouse, state = project
    macros = root / "macros" / "audit.sql"
    macros.parent.mkdir(parents=True, exist_ok=True)
    macros.write_text(OVERRIDE, encoding="utf-8")
    hooked = root / "models" / "hooked.sql"
    hooked.write_text(
        hooked.read_text().replace('"insert into main.audit select 1"', '"{{ audit_row() }}"')
    )
    dbt(root)
    second = dbt(root)
    assert second["fct_orders"][0] == second["hooked"][0] == "reused"

    # the override adds a column that the compiled SQL does not show
    macros.write_text(OVERRIDE.replace("1 as _audit", "2 as _audit"), encoding="utf-8")
    dbt(root)
    now = decisions(state)
    assert now["fct_orders"] == ("build", "config_changed")
    assert now["dim_country"] == ("build", "config_changed")  # every table runs the override
    assert now["stg_orders"] == ("reuse", "view_unchanged")  # views do not
    assert now["hooked"] == ("build", "config_changed")
    with duckdb.connect(str(warehouse)) as con:
        assert con.execute("select max(_audit) from main.fct_orders").fetchone()[0] == 2

    # a macro called only from a hook
    macros.write_text(
        macros.read_text().replace("audit select 1", "audit select 2"), encoding="utf-8"
    )
    dbt(root)
    now = decisions(state)
    assert now["hooked"] == ("build", "config_changed")
    assert now["fct_orders"] == ("reuse", "no_new_data")


def test_explain_reads_the_state_of_the_project_and_target(project, capsys):
    root, warehouse, _ = project
    dbt(root)
    dbt(root)
    explain = ["explain", "--project-dir", str(root), "--profiles-dir", str(root)]
    capsys.readouterr()
    assert cli.main([*explain, "-s", "fct_orders"]) == 0
    out = capsys.readouterr().out
    assert "model.hodor_e2e.fct_orders: reuse (no_new_data)" in out
    assert "  code hash    same (" in out
    assert "test." not in out  # only the selected node

    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.orders values (3, 30)")
    dbt(root, "-s", "fct_orders")
    capsys.readouterr()
    assert cli.main([*explain, "-s", "fct_orders", "--json"]) == 0
    rep = json.loads(capsys.readouterr().out)
    (fct,) = rep["nodes"]
    assert (fct["action"], fct["reason"]) == ("build", "new_upstream_data")
    changed = [u for u in fct["upstreams"] if u["last"] != u["now"]]
    assert [u["table"].split(".")[-1] for u in changed] == ["orders"]

    first = fct["last_build"]["invocation_id"]
    assert cli.main([*explain, "--invocation", first, "-s", "fct_orders", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["invocation_id"] == first
    with pytest.raises(SystemExit, match="no records"):
        cli.main([*explain, "--invocation", "nope"])


def test_prune_is_a_dry_run_until_yes(project, capsys):
    root, _, state = project
    dbt(root)
    before = len(records(state))
    assert before > 0
    prune = ["prune", "--project-dir", str(root), "--profiles-dir", str(root)]

    # the default keeps hodor_lookback_days, so today's records stay
    capsys.readouterr()
    assert cli.main([*prune, "--yes"]) == 0
    assert "Deleted 0 records" in capsys.readouterr().out
    assert len(records(state)) == before

    # --keep-days 0 covers everything, and warns that the next run loses its history
    assert cli.main([*prune, "--keep-days", "0"]) == 0
    out = capsys.readouterr().out
    assert f"Would delete {before} records" in out
    assert "Run again with --yes" in out
    assert "hodor_lookback_days is 90" in out
    assert len(records(state)) == before  # a dry run deletes nothing

    assert cli.main([*prune, "--keep-days", "0", "--yes"]) == 0
    assert f"Deleted {before} records" in capsys.readouterr().out
    assert records(state) == []

    # with the history gone, the next run has nothing to reuse
    dbt(root)
    reasons = decisions(state)
    assert reasons and all(reason == "no_history" for _, reason in reasons.values())


PYTHON_PARENT = {
    "models/py_orders.py": """
        def model(dbt, session):
            dbt.config(materialized="table")
            return dbt.source("raw", "orders")
    """,
    "models/py_child.sql": "{{ config(materialized='table') }} select count(*) as n from "
    "{{ ref('py_orders') }}",
}


def test_child_of_an_unsupported_parent_sees_the_parent_build(project):
    root, warehouse, state = project
    for name, body in PYTHON_PARENT.items():
        (root / name).write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    dbt(root)
    dbt(root, "-s", "py_child")  # the child's record catches up with the parent's table

    # the Python parent always builds; its new data must reach the child in the same run
    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.orders values (3, 30)")
    dbt(root)
    assert decisions(state)["py_child"] == ("build", "new_upstream_data")
    with duckdb.connect(str(warehouse)) as con:
        assert con.execute("select n from main.py_child").fetchone()[0] == 3


def test_on_run_start_writes_are_seen(project):
    root, warehouse, state = project
    dbt(root)
    project_file = root / "dbt_project.yml"
    project_file.write_text(
        project_file.read_text() + 'on-run-start: "insert into raw.orders values (3, 30)"\n'
    )
    dbt(root)
    assert decisions(state)["fct_orders"] == ("build", "new_upstream_data")
    with duckdb.connect(str(warehouse)) as con:
        assert con.execute("select count(*) from main.fct_orders").fetchone()[0] == 3


def test_hooks_with_run_values_do_not_rebuild(project):
    root, _, state = project
    (root / "models" / "audited.sql").write_text(
        "{{ config(materialized='table', post_hook=\"insert into main.audit select 1 "
        "where '{{ invocation_id }}' <> '{{ run_started_at }}'\") }}\n"
        "select id from {{ ref('stg_orders') }}\n",
        encoding="utf-8",
    )
    dbt(root)
    dbt(root)
    assert decisions(state)["audited"] == ("reuse", "no_new_data")

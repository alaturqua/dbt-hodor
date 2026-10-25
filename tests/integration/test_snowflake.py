"""Live test on Snowflake. Opt in: `pytest -m snowflake tests/integration`.

Skips unless these variables are set: HODOR_TEST_SNOWFLAKE_ACCOUNT, _USER, _PRIVATE_KEY_PATH,
_ROLE, _WAREHOUSE, _DATABASE and _SCHEMA (key-pair authentication). The test creates the
schema `<_SCHEMA>_<short uuid>` in that database and drops it with CASCADE at the end. The
state lives in a local DuckDB file.
"""

from __future__ import annotations

import os
import textwrap
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.snowflake

NAMES = ("ACCOUNT", "USER", "PRIVATE_KEY_PATH", "ROLE", "WAREHOUSE", "DATABASE", "SCHEMA")
ENV = {name: os.environ.get(f"HODOR_TEST_SNOWFLAKE_{name}") for name in NAMES}
if not all(ENV.values()):
    pytest.skip(
        "set HODOR_TEST_SNOWFLAKE_" + ", _".join(NAMES) + " to run", allow_module_level=True
    )

duckdb = pytest.importorskip("duckdb")
snowflake_connector = pytest.importorskip("snowflake.connector")

from dbt.cli.main import dbtRunner  # noqa: E402


def execute(sql: str) -> list[tuple]:
    con = snowflake_connector.connect(
        account=ENV["ACCOUNT"],
        user=ENV["USER"],
        private_key_file=ENV["PRIVATE_KEY_PATH"],
        role=ENV["ROLE"],
        warehouse=ENV["WAREHOUSE"],
        database=ENV["DATABASE"],
    )
    try:
        cur = con.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        con.close()


FILES = {
    "dbt_project.yml": """
        name: hodor_snowflake
        version: "1.0"
        profile: hodor_snowflake
        models:
          hodor_snowflake:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        hodor_snowflake:
          target: live
          outputs:
            live:
              type: snowflake
              account: "{{ env_var('HODOR_TEST_SNOWFLAKE_ACCOUNT') }}"
              user: "{{ env_var('HODOR_TEST_SNOWFLAKE_USER') }}"
              private_key_path: "{{ env_var('HODOR_TEST_SNOWFLAKE_PRIVATE_KEY_PATH') }}"
              role: "{{ env_var('HODOR_TEST_SNOWFLAKE_ROLE') }}"
              warehouse: "{{ env_var('HODOR_TEST_SNOWFLAKE_WAREHOUSE') }}"
              database: "{{ env_var('HODOR_TEST_SNOWFLAKE_DATABASE') }}"
              schema: "{{ env_var('HODOR_TEST_SNOWFLAKE_RUN_SCHEMA') }}"
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('HODOR_TEST_SNOWFLAKE_STATE') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            schema: "{{ env_var('HODOR_TEST_SNOWFLAKE_RUN_SCHEMA') }}"
            tables:
              - name: raw_orders
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount from {{ source('raw', 'raw_orders') }}
    """,
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2 from {{ ref('stg_orders') }}
    """,
    "models/all_orders.sql": """
        {{ config(materialized='table') }}
        select * from {{ source('raw', 'raw_orders') }}
    """,
    "models/v_orders.sql": """
        {{ config(materialized='view') }}
        select id, amount2 from {{ ref('fct_orders') }}
    """,
}


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    schema = f"{ENV['SCHEMA']}_{uuid.uuid4().hex[:8]}"
    state = tmp_path / "state.duckdb"
    monkeypatch.setenv("HODOR_TEST_SNOWFLAKE_RUN_SCHEMA", schema)
    monkeypatch.setenv("HODOR_TEST_SNOWFLAKE_STATE", str(state))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    try:
        execute(f"create schema {schema}")
        execute(f"create table {schema}.raw_orders as select 1 as id, 10 as amount")
        yield root, schema, state
    finally:
        execute(f"drop schema if exists {schema} cascade")


def dbt(root: Path) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(["build", "--project-dir", str(root), "--profiles-dir", str(root)])
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def decisions(state: Path) -> dict[str, tuple[str, str]]:
    """Latest (action, reason) per node name, read back from the state file."""
    with duckdb.connect(str(state), read_only=True) as con:
        rows = con.execute(
            "select unique_id, action, reason from main.hodor_state order by recorded_at_ms"
        ).fetchall()
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def test_snowflake_lifecycle(project):
    root, schema, state = project
    raw = f"{schema}.raw_orders"

    # 1. first run builds everything
    first = dbt(root)
    assert {status for status, _ in first.values()} == {"success"}
    assert {reason for _, reason in decisions(state).values()} == {"no_history"}

    # 2. nothing changed: tables are reused, and so is the unchanged view
    second = dbt(root)
    assert second["stg_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["all_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["v_orders"] == ("reused", "REUSED (view_unchanged)")

    # 3. a new row in the source rebuilds the tables; the view definition is unchanged
    execute(f"insert into {raw} values (2, 20)")
    dbt(root)
    now = decisions(state)
    assert now["stg_orders"] == ("build", "new_upstream_data")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["all_orders"] == ("build", "new_upstream_data")
    assert now["v_orders"] == ("reuse", "view_unchanged")
    assert execute(f"select count(*) from {schema}.fct_orders") == [(2,)]
    assert dbt(root)["fct_orders"] == ("reused", "REUSED (no_new_data)")

    # 4. a changed view definition rebuilds the view
    path = root / "models" / "v_orders.sql"
    path.write_text(path.read_text().replace("amount2 from", "amount2 + 1 as amount2 from"))
    third = dbt(root)
    assert third["v_orders"][0] == "success"
    assert decisions(state)["v_orders"] == ("build", "code_changed")

    # 5. ADD COLUMN moves no commit token but still rebuilds a select * table
    execute(f"alter table {raw} add column note varchar")
    dbt(root)
    assert decisions(state)["all_orders"] == ("build", "new_upstream_data")
    columns = execute(
        f"select column_name from {ENV['DATABASE']}.information_schema.columns "
        f"where table_schema = '{schema.upper()}' and table_name = 'ALL_ORDERS' "
        "order by ordinal_position"
    )
    assert [c[0] for c in columns] == ["ID", "AMOUNT", "NOTE"]
    assert dbt(root)["all_orders"] == ("reused", "REUSED (no_new_data)")

    # 6. create or replace with the same columns and no rows sends the commit token back to
    # 0, but the created time moves: downstream rebuilds
    execute(f"create or replace table {raw} (id number, amount number, note varchar)")
    dbt(root)
    now = decisions(state)
    assert now["stg_orders"] == ("build", "new_upstream_data")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["all_orders"] == ("build", "new_upstream_data")
    assert execute(f"select count(*) from {schema}.fct_orders") == [(0,)]
    assert dbt(root)["fct_orders"] == ("reused", "REUSED (no_new_data)")

"""Live test on a Trino Iceberg catalog. Opt in: `pytest -m trino tests/integration`.

Needs a Trino server with an `iceberg` catalog (see docs/development.md for the Docker command).
Host and port come from HODOR_TEST_TRINO_HOST and HODOR_TEST_TRINO_PORT (localhost and 8080).
The test creates one schema per run in the iceberg catalog and drops it at the end.
"""

from __future__ import annotations

import os
import textwrap
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.trino

HOST = os.environ.get("HODOR_TEST_TRINO_HOST", "localhost")
PORT = int(os.environ.get("HODOR_TEST_TRINO_PORT", "8080"))
CATALOG = "iceberg"

duckdb = pytest.importorskip("duckdb")
trino_dbapi = pytest.importorskip("trino.dbapi")

from dbt.cli.main import dbtRunner  # noqa: E402


def execute(sql: str) -> list[tuple]:
    con = trino_dbapi.connect(host=HOST, port=PORT, user="hodor", catalog=CATALOG)
    try:
        cur = con.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        con.close()


try:
    execute("select 1")
except Exception as exc:  # noqa: BLE001 - any failure means no server to test against
    pytest.skip(f"Trino not reachable at {HOST}:{PORT}: {exc}", allow_module_level=True)

FILES = {
    "dbt_project.yml": """
        name: hodor_trino
        version: "1.0"
        profile: hodor_trino
        models:
          hodor_trino:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        hodor_trino:
          target: live
          outputs:
            live:
              type: trino
              method: none
              user: hodor
              host: "{{ env_var('HODOR_TEST_TRINO_HOST') }}"
              port: "{{ env_var('HODOR_TEST_TRINO_PORT') | int }}"
              http_scheme: http
              database: iceberg
              schema: "{{ env_var('HODOR_TEST_TRINO_SCHEMA') }}"
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('HODOR_TEST_TRINO_STATE') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            database: iceberg
            schema: "{{ env_var('HODOR_TEST_TRINO_SCHEMA') }}"
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
    schema = f"hodor_{uuid.uuid4().hex[:12]}"
    execute(f"create schema {CATALOG}.{schema} with (location = 'local:///{schema}')")
    state = tmp_path / "state.duckdb"
    monkeypatch.setenv("HODOR_TEST_TRINO_HOST", HOST)
    monkeypatch.setenv("HODOR_TEST_TRINO_PORT", str(PORT))
    monkeypatch.setenv("HODOR_TEST_TRINO_SCHEMA", schema)
    monkeypatch.setenv("HODOR_TEST_TRINO_STATE", str(state))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    try:
        execute(f"create table {CATALOG}.{schema}.raw_orders as select 1 as id, 10 as amount")
        yield root, schema, state
    finally:
        execute(f"drop view if exists {CATALOG}.{schema}.v_orders")
        for (table,) in execute(f"show tables from {CATALOG}.{schema}"):
            execute(f"drop table if exists {CATALOG}.{schema}.{table}")
        execute(f"drop schema if exists {CATALOG}.{schema}")


def dbt(root: Path) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(["build", "--project-dir", str(root), "--profiles-dir", str(root)])
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def decisions(state: Path) -> dict[str, tuple[str, str]]:
    """Latest (action, reason) per node name."""
    with duckdb.connect(str(state), read_only=True) as con:
        rows = con.execute(
            "select unique_id, action, reason from main.hodor_state order by recorded_at_ms"
        ).fetchall()
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def test_iceberg_lifecycle(project):
    root, schema, state = project
    raw = f"{CATALOG}.{schema}.raw_orders"
    history = f'{CATALOG}.{schema}."raw_orders$history"'
    first_snapshot = execute(f"select snapshot_id from {history}")[0][0]

    # 1. first run builds everything
    first = dbt(root)
    assert {status for status, _ in first.values()} == {"success"}
    assert {reason for _, reason in decisions(state).values()} == {"no_history"}

    # 2. nothing changed: tables are reused, and so is the unchanged view
    second = dbt(root)
    assert second["stg_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert second["v_orders"] == ("reused", "REUSED (view_unchanged)")

    # 3. a new row in the source rebuilds the tables
    execute(f"insert into {raw} values (2, 20)")
    dbt(root)
    now = decisions(state)
    assert now["stg_orders"] == ("build", "new_upstream_data")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["v_orders"] == ("reuse", "view_unchanged")
    assert execute(f"select count(*) from {CATALOG}.{schema}.fct_orders") == [(2,)]
    assert dbt(root)["fct_orders"] == ("reused", "REUSED (no_new_data)")

    # 4. a rollback makes an older snapshot current again: downstream rebuilds
    execute(
        f"call {CATALOG}.system.rollback_to_snapshot('{schema}', 'raw_orders', {first_snapshot})"
    )
    dbt(root)
    now = decisions(state)
    assert now["stg_orders"] == ("build", "new_upstream_data")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert execute(f"select count(*) from {CATALOG}.{schema}.fct_orders") == [(1,)]

    # 5. a changed view definition rebuilds the view
    path = root / "models" / "v_orders.sql"
    path.write_text(path.read_text().replace("amount2 from", "amount2 + 1 as amount2 from"))
    third = dbt(root)
    assert third["v_orders"][0] == "success"
    assert decisions(state)["v_orders"] == ("build", "code_changed")

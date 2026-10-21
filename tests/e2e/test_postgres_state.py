"""End to end with the state in Postgres: real dbt-duckdb runs, hodor records to Postgres.

Opt in: `pytest -m postgres`. The warehouse stays DuckDB, so this tests the store and the
decision path, not a warehouse. Works against Databricks Lakebase or any other Postgres.

Lakebase:
    HODOR_PG_ENDPOINT=projects/<p>/branches/<b>/endpoints/<e>
    HODOR_PG_DATABASE=databricks_postgres
    HODOR_PG_SCHEMA=<a schema of its own>
    plus workspace credentials for databricks-sdk (DATABRICKS_CONFIG_PROFILE or
    DATABRICKS_HOST and DATABRICKS_TOKEN).

Any other Postgres, for example one in Docker:
    HODOR_PG_HOST=localhost HODOR_PG_USER=postgres HODOR_PG_PASSWORD=postgres
    HODOR_PG_DATABASE=postgres HODOR_PG_SCHEMA=hodor_e2e HODOR_PG_SSLMODE=disable

HODOR_PG_SCHEMA is created at the start and dropped with CASCADE at the end, so point it at
a schema that holds nothing else.
"""

from __future__ import annotations

import os
import textwrap

import pytest

pytestmark = pytest.mark.postgres

duckdb = pytest.importorskip("duckdb")
psycopg = pytest.importorskip("psycopg")
pytest.importorskip("dbt.adapters.duckdb")

if not os.environ.get("HODOR_PG_DATABASE") or not os.environ.get("HODOR_PG_SCHEMA"):
    pytest.skip("Postgres env vars not set", allow_module_level=True)
if not (os.environ.get("HODOR_PG_ENDPOINT") or os.environ.get("HODOR_PG_HOST")):
    pytest.skip("neither HODOR_PG_ENDPOINT nor HODOR_PG_HOST is set", allow_module_level=True)

from dbt.cli.main import dbtRunner  # noqa: E402

from dbt_hodor.store.postgres import lakebase_target  # noqa: E402

DATABASE = os.environ["HODOR_PG_DATABASE"]
SCHEMA = os.environ["HODOR_PG_SCHEMA"]
ENDPOINT = os.environ.get("HODOR_PG_ENDPOINT")
SSLMODE = os.environ.get("HODOR_PG_SSLMODE", "require")
PORT = int(os.environ.get("HODOR_PG_PORT", "5432"))

FILES = {
    "dbt_project.yml": """
        name: hodor_pg
        version: "1.0"
        profile: hodor_pg
        models:
          hodor_pg:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        hodor_pg:
          target: local
          outputs:
            local:
              type: duckdb
              path: "{{ env_var('HODOR_PG_WAREHOUSE') }}"
              schema: main
              threads: 2
              hodor_store: postgres
              hodor_database: "{{ env_var('HODOR_PG_DATABASE') }}"
              hodor_schema: "{{ env_var('HODOR_PG_SCHEMA') }}"
              hodor_sslmode: "{{ env_var('HODOR_PG_SSLMODE', 'require') }}"
              hodor_port: "{{ env_var('HODOR_PG_PORT', '5432') }}"
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
        select id, amount * 2 as amount2 from {{ ref('stg_orders') }}
    """,
}


def connection_args() -> dict[str, object]:
    """Connection for the test's own reads, matching what the store will use."""
    if ENDPOINT:
        host, user, credential = lakebase_target(ENDPOINT)
        return {"host": host, "user": user, "password": credential(), "sslmode": SSLMODE}
    return {
        "host": os.environ["HODOR_PG_HOST"],
        "user": os.environ["HODOR_PG_USER"],
        "password": os.environ.get("HODOR_PG_PASSWORD", ""),
        "sslmode": SSLMODE,
    }


def execute(sql: str) -> list[tuple]:
    with psycopg.connect(dbname=DATABASE, port=PORT, autocommit=True, **connection_args()) as con:
        with con.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []


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
    monkeypatch.setenv("HODOR_PG_WAREHOUSE", str(warehouse))
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    # The store's own settings. hodor_endpoint carries the Lakebase host, user and token;
    # a plain Postgres needs host, user and password instead.
    if ENDPOINT:
        monkeypatch.setenv("HODOR_ENDPOINT", ENDPOINT)
    else:
        monkeypatch.setenv("HODOR_HOST", os.environ["HODOR_PG_HOST"])
        monkeypatch.setenv("HODOR_USER", os.environ["HODOR_PG_USER"])
        monkeypatch.setenv("HODOR_PASSWORD", os.environ.get("HODOR_PG_PASSWORD", ""))
    execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    yield root, warehouse
    execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def dbt(root, *args: str) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), *args]
    )
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def decisions() -> dict[str, tuple[str, str]]:
    """Latest (action, reason) per node name, read straight from the Postgres state table."""
    rows = execute(
        f'SELECT unique_id, action, reason FROM "{SCHEMA}".hodor_state '
        "ORDER BY recorded_at_ms, record_id"
    )
    return {unique_id.split(".")[-1]: (action, reason) for unique_id, action, reason in rows}


def test_state_lifecycle_in_postgres(project):
    root, warehouse = project

    # 1. first run: the store creates its own schema and table, and records no history
    first = dbt(root)
    assert {status for status, _ in first.values()} <= {"success", "pass"}
    assert all(reason == "no_history" for _, reason in decisions().values())

    # 2. nothing changed: both models are reused, decided from the Postgres records
    second = dbt(root)
    assert second["fct_orders"][0] == "reused"
    assert decisions()["fct_orders"] == ("reuse", "no_new_data")

    # 3. new upstream data rebuilds the table. The view is reused: a view holds no data.
    with duckdb.connect(str(warehouse)) as con:
        con.execute("insert into raw.orders values (3, 30)")
    third = dbt(root)
    assert third["fct_orders"][0] == "success"
    assert decisions()["fct_orders"] == ("build", "new_upstream_data")
    assert decisions()["stg_orders"] == ("reuse", "view_unchanged")

    # 4. a code change rebuilds that model
    model = root / "models" / "fct_orders.sql"
    model.write_text(model.read_text(encoding="utf-8").replace("* 2", "* 3"), encoding="utf-8")
    dbt(root)
    assert decisions()["fct_orders"] == ("build", "code_changed")

    # 5. every record landed in Postgres, one per node per run
    count = execute(f'SELECT count(*) FROM "{SCHEMA}".hodor_state')[0][0]
    assert count == 8  # 2 nodes x 4 runs

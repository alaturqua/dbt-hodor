"""Live slim CI test on Databricks. Opt in: `pytest -m databricks` with these env vars set:

HODOR_DBX_HOST, HODOR_DBX_HTTP_PATH, HODOR_DBX_TOKEN, HODOR_DBX_CATALOG, HODOR_DBX_SCHEMA,
HODOR_DBX_CI_CATALOG.

Prod builds into HODOR_DBX_CATALOG.<HODOR_DBX_SCHEMA>_slim_ci, its own schema, so this test and
test_databricks.py can share the env vars and run in one session. CI builds into the CI catalog, in
the PR schema pr_<nr>_<HODOR_DBX_SCHEMA>_slim_ci, and defers to prod's manifest.
Both schemas are dropped at the end (CASCADE), so point them at names that hold nothing else.
"""

from __future__ import annotations

import os
import shutil
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.databricks

ENV = (
    "HODOR_DBX_HOST",
    "HODOR_DBX_HTTP_PATH",
    "HODOR_DBX_TOKEN",
    "HODOR_DBX_CATALOG",
    "HODOR_DBX_SCHEMA",
    "HODOR_DBX_CI_CATALOG",
)
if not all(os.environ.get(k) for k in ENV):
    pytest.skip("Databricks env vars not set", allow_module_level=True)

from databricks import sql as dbsql  # noqa: E402
from dbt.cli.main import dbtRunner  # noqa: E402

PR = "7"
SCHEMA = f"{os.environ['HODOR_DBX_SCHEMA']}_slim_ci"  # not the schema of test_databricks.py
PROD = f"`{os.environ['HODOR_DBX_CATALOG']}`.`{SCHEMA}`"
CI = f"`{os.environ['HODOR_DBX_CI_CATALOG']}`.`pr_{PR}_{SCHEMA}`"

CONNECTION = """
              type: databricks
              host: "{{ env_var('HODOR_DBX_HOST') }}"
              http_path: "{{ env_var('HODOR_DBX_HTTP_PATH') }}"
              token: "{{ env_var('HODOR_DBX_TOKEN') }}"
              threads: 4
              connection_parameters:
                enable_telemetry: false"""

FILES = {
    "dbt_project.yml": """
        name: hodor_slim_ci
        version: "1.0"
        profile: hodor_slim_ci
        models:
          hodor_slim_ci:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": f"""
        hodor_slim_ci:
          target: prod
          outputs:
            prod:{CONNECTION}
              catalog: "{{{{ env_var('HODOR_DBX_CATALOG') }}}}"
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
              hodor_schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
            ci:{CONNECTION}
              catalog: "{{{{ env_var('HODOR_DBX_CI_CATALOG') }}}}"
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
              hodor_schema: "pr_{PR}_{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
    """,
    "macros/generate_schema_name.sql": f"""
        {{% macro generate_schema_name(custom_schema_name, node) -%}}
          {{%- set base = custom_schema_name | trim if custom_schema_name else target.schema -%}}
          {{%- if target.name == 'ci' -%}}pr_{PR}_{{{{ base }}}}
          {{%- else -%}}{{{{ base }}}}{{%- endif -%}}
        {{%- endmacro %}}
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            database: "{{ env_var('HODOR_DBX_CATALOG') }}"
            schema: "{{ env_var('HODOR_DBX_SCHEMA') }}"
            tables:
              - name: raw_orders
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount from {{ source('raw', 'raw_orders') }}
    """,
    "models/int_orders.sql": "select id, amount from {{ ref('stg_orders') }}",
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
        {{ config(materialized='incremental', unique_key='id') }}
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


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    for schema in (PROD, CI):  # the delta store needs its schema before the first run
        execute(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'dbt-hodor live test'")
    execute(
        f"CREATE OR REPLACE TABLE {PROD}.raw_orders AS "
        "SELECT * FROM VALUES (1, 10), (2, 20) AS v(id, amount)"
    )
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("HODOR_DBX_SCHEMA", SCHEMA)  # the profile and the sources read it
    yield root
    for schema in (CI, PROD):
        execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


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


def deploy_prod(root: Path) -> None:
    dbt(root, "prod")
    state = root / "prod_state"
    state.mkdir(exist_ok=True)
    shutil.copy(root / "target" / "manifest.json", state / "manifest.json")


def ci(root: Path, *args: str) -> dict[str, tuple[str, str]]:
    return dbt(root, "ci", "--defer", "--state", str(root / "prod_state"), *args)


def decisions(schema: str, target: str) -> dict[str, tuple[str, str]]:
    rows = execute(
        f"SELECT unique_id, action, reason FROM {schema}.hodor_state "
        f"WHERE target_name = '{target}' "
        "QUALIFY row_number() OVER (PARTITION BY unique_id ORDER BY recorded_at_ms DESC) = 1"
    )
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def edit(root: Path, model: str, old: str, new: str) -> None:
    path = root / "models" / f"{model}.sql"
    path.write_text(path.read_text().replace(old, new), encoding="utf-8")


def count(relation: str) -> int:
    return execute(f"SELECT count(*) FROM {relation}")[0][0]


def test_live_slim_ci(project):
    root = project
    deploy_prod(root)

    # dbt clone copies prod's tables; they have no hodor record, so they build once
    selected = ("-s", "dim_parity fct_orders")
    result = dbtRunner().invoke(
        ["clone", *selected, "--state", str(root / "prod_state"), "--target", "ci"]
        + ["--project-dir", str(root), "--profiles-dir", str(root)]
    )
    assert result.success, result.exception
    assert execute(f"DESCRIBE HISTORY {CI}.fct_orders LIMIT 1")[0][4] == "CLONE"
    ci(root, *selected)
    assert decisions(CI, "ci")["dim_parity"] == ("build", "no_history")

    # the next push reuses the CI builds
    second = ci(root, *selected)
    assert {status for status, _ in second.values()} == {"reused", "pass"}, second

    # the PR changes fct_orders: it builds in CI, and so does its child
    edit(root, "fct_orders", "amount * 2", "amount * 3")
    modified = ("-s", "state:modified+")
    third = ci(root, *modified)
    assert set(third) == {"fct_orders", "rpt_orders", "not_null_fct_orders_id"}
    assert decisions(CI, "ci")["fct_orders"] == ("build", "code_changed")
    assert execute(f"SELECT max(amount2) FROM {CI}.fct_orders") == [(60,)]

    # the next push, same code: fct_orders reads prod's view through defer, nothing builds
    fourth = ci(root, *modified)
    assert fourth["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert fourth["rpt_orders"] == ("reused", "REUSED (no_new_data)")

    # prod gets new data and deploys main: the PR's models build again
    execute(f"INSERT INTO {PROD}.raw_orders VALUES (3, 30)")
    edit(root, "fct_orders", "amount * 3", "amount * 2")
    deploy_prod(root)
    edit(root, "fct_orders", "amount * 2", "amount * 3")
    ci(root, *modified)
    now = decisions(CI, "ci")
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["rpt_orders"] == ("build", "new_upstream_data")
    assert count(f"{CI}.fct_orders") == 3

    # CI records stay in the CI store
    assert decisions(PROD, "ci") == {}

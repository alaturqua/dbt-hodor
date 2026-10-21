"""Live test on Databricks. Opt in: `pytest -m databricks` with these env vars set:

HODOR_DBX_HOST, HODOR_DBX_HTTP_PATH, HODOR_DBX_TOKEN, HODOR_DBX_CATALOG, HODOR_DBX_SCHEMA.

Everything is created in HODOR_DBX_SCHEMA, and the schema is dropped at the end (CASCADE),
so point it at a schema that holds nothing else.
"""

from __future__ import annotations

import os
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
)
if not all(os.environ.get(k) for k in ENV):
    pytest.skip("Databricks env vars not set", allow_module_level=True)

from databricks import sql as dbsql  # noqa: E402
from dbt.cli.main import dbtRunner  # noqa: E402

CATALOG = os.environ["HODOR_DBX_CATALOG"]
SCHEMA = os.environ["HODOR_DBX_SCHEMA"]
FQ = f"`{CATALOG}`.`{SCHEMA}`"

FILES = {
    "dbt_project.yml": """
        name: hodor_live
        version: "1.0"
        profile: hodor_live
        flags:
          use_materialization_v2: __V2__
        models:
          hodor_live:
            +state:
              lag_tolerance: 0s
        snapshots:
          hodor_live:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": """
        hodor_live:
          target: live
          outputs:
            live:
              type: databricks
              host: "{{ env_var('HODOR_DBX_HOST') }}"
              http_path: "{{ env_var('HODOR_DBX_HTTP_PATH') }}"
              token: "{{ env_var('HODOR_DBX_TOKEN') }}"
              catalog: "{{ env_var('HODOR_DBX_CATALOG') }}"
              schema: "{{ env_var('HODOR_DBX_SCHEMA') }}"
              threads: 4
              connection_parameters:
                enable_telemetry: false
              hodor_schema: "{{ env_var('HODOR_DBX_SCHEMA') }}"
    """,
    "models/sources.yml": """
        sources:
          - name: raw
            schema: "{{ env_var('HODOR_DBX_SCHEMA') }}"
            tables:
              - name: raw_orders
    """,
    "models/stg_orders.sql": "select id, amount from {{ source('raw', 'raw_orders') }}",
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2, '{{ invocation_id }}' as loaded_by
        from {{ ref('stg_orders') }}
    """,
    "models/inc_orders.sql": """
        {{ config(materialized='incremental', unique_key='id') }}
        select id, amount from {{ ref('stg_orders') }}
        {% if is_incremental() %}
        where id > (select coalesce(max(id), 0) from {{ this }})
        {% endif %}
    """,
    "models/rw_orders.sql": """
        {{ config(
            materialized='incremental',
            incremental_strategy='replace_where',
            incremental_predicates='id >= 2'
        ) }}
        select id, amount from {{ ref('stg_orders') }} where id >= 2
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
            relation: source('raw', 'raw_orders')
            config:
              unique_key: id
              strategy: check
              check_cols: all
    """,
    "models/numbers.sql": "{{ config(materialized='table') }} select * from range(3)",
    "models/safe_orders.sql": "{{ config(materialized='table', "
    "use_safer_relation_operations=true) }} select id, amount from {{ ref('stg_orders') }}",
    "models/safe_child.sql": "{{ config(materialized='table') }} select count(*) as n from "
    "{{ ref('safe_orders') }}",
    "models/pk_orders.sql": "{{ config(materialized='table') }} select id, amount from "
    "{{ ref('stg_orders') }}",
    "models/secured_orders.sql": "{{ config(materialized='table') }} select id, amount from "
    "{{ ref('stg_orders') }}",
    "models/windowed.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_date() >= date'2000-01-01'",
    "models/by_user.sql": "{{ config(materialized='table') }} select id from "
    "{{ ref('stg_orders') }} where current_user() is not null",
    "models/stamped.sql": "{{ config(materialized='table') }} select id, "
    "current_timestamp() as ts, current_user() as built_by from {{ ref('stg_orders') }}",
    "models/schema.yml": """
        models:
          - name: fct_orders
            columns:
              - name: id
                data_tests: [not_null, unique]
          - name: pk_orders
            config:
              contract: {enforced: true}
            constraints:
              - type: primary_key
                columns: [id]
            columns:
              - name: id
                data_type: int
                constraints: [{type: not_null}]
              - name: amount
                data_type: int
                constraints: [{type: not_null}]
          - name: secured_orders
            config:
              row_filter: {function: big_only, columns: [amount]}
            columns:
              - name: amount
                column_mask: {function: mask_zero}
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


@pytest.fixture(
    scope="module", params=[False, True], ids=["materialization_v1", "materialization_v2"]
)
def project(request, tmp_path_factory):
    """Each run builds the whole project once with use_materialization_v2 off, once on."""
    root = tmp_path_factory.mktemp("hodor_live")
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        body = body.replace("__V2__", str(request.param).lower())
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    execute(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'dbt-hodor live test; dropped after use'")
    execute(
        f"CREATE OR REPLACE TABLE {FQ}.raw_orders AS "
        "SELECT * FROM VALUES (1, 10), (2, 20) AS v(id, amount)"
    )
    # Unity Catalog functions for the row filter and the column masks
    execute(f"CREATE OR REPLACE FUNCTION {FQ}.big_only(amount INT) RETURN amount > 10")
    execute(f"CREATE OR REPLACE FUNCTION {FQ}.mask_zero(amount INT) RETURN 0")
    execute(f"CREATE OR REPLACE FUNCTION {FQ}.mask_minus(amount INT) RETURN -1")
    yield root, request.param
    execute(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")


def dbt(root: Path, *args: str) -> dict[str, str]:
    os.environ["HODOR_MODE"] = "on"
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), *args]
    )
    assert result.success, result.exception
    return {r.node.name: str(r.status) for r in result.result.results}


def decisions() -> dict[str, tuple[str, str]]:
    rows = execute(
        f"SELECT unique_id, action, reason FROM {FQ}.hodor_state "
        "QUALIFY row_number() OVER (PARTITION BY unique_id ORDER BY recorded_at_ms DESC) = 1"
    )
    return {uid if uid.startswith("test.") else uid.split(".")[2]: (a, r) for uid, a, r in rows}


def test_live_lifecycle(project):
    project, v2 = project
    dbt(project)
    assert all(reason == "no_history" for _, reason in decisions().values())

    second = dbt(project)
    now = decisions()
    assert second["fct_orders"] == "reused"
    assert now["fct_orders"] == ("reuse", "no_new_data")
    assert now["stg_orders"] == ("reuse", "view_unchanged")
    assert now["numbers"] == ("build", "unknown_upstream")
    assert second["rw_orders"] == second["countries"] == second["orders_snapshot"] == "reused"
    assert now["dim_country"] == ("reuse", "no_new_data")
    # volatile SQL: same date and user, stamps never count; this runs the session query
    for name in ("windowed", "by_user", "stamped", "pk_orders", "secured_orders", "safe_orders"):
        assert now[name] == ("reuse", "no_new_data"), name
    assert now["safe_child"] == ("reuse", "no_new_data")
    assert execute(f"SELECT count(*) FROM {FQ}.secured_orders") == [(1,)]  # row filter: amount > 10
    if v2:  # dbt-databricks applies column masks on tables only with use_materialization_v2
        assert execute(f"SELECT max(amount) FROM {FQ}.secured_orders") == [(0,)]
    tests = [k for k in now if k.startswith("test.")]
    assert tests and all(now[t] == ("reuse", "no_new_data") for t in tests)

    # maintenance and zero-row writes are not data changes
    execute(f"OPTIMIZE {FQ}.raw_orders")
    execute(f"OPTIMIZE {FQ}.fct_orders")
    execute(f"INSERT INTO {FQ}.raw_orders SELECT * FROM {FQ}.raw_orders WHERE 1 = 0")
    execute(f"ALTER TABLE {FQ}.raw_orders SET TBLPROPERTIES ('hodor.test' = '1')")
    dbt(project)
    now = decisions()
    assert now["fct_orders"] == ("reuse", "no_new_data")
    assert now["inc_orders"] == ("reuse", "no_new_data")
    assert now["rw_orders"] == ("reuse", "no_new_data")
    assert now["orders_snapshot"] == ("reuse", "no_new_data")

    # real new data
    execute(f"INSERT INTO {FQ}.raw_orders VALUES (3, 30)")
    dbt(project)
    now = decisions()
    assert now["fct_orders"] == ("build", "new_upstream_data")
    assert now["inc_orders"] == ("build", "new_upstream_data")
    assert now["rw_orders"] == ("build", "new_upstream_data")
    assert now["orders_snapshot"] == ("build", "new_upstream_data")
    # with v2, safe_orders is swapped in from a staging table: its Delta versions restart,
    # and its child must still see new data
    assert now["safe_orders"] == ("build", "new_upstream_data")
    assert now["safe_child"] == ("build", "new_upstream_data")
    assert execute(f"SELECT n FROM {FQ}.safe_child") == [(3,)]
    assert now["countries"] == ("reuse", "no_new_data")
    assert all(now[t] == ("build", "new_upstream_data") for t in tests)
    assert execute(f"SELECT count(*) FROM {FQ}.inc_orders") == [(3,)]
    assert execute(f"SELECT count(*) FROM {FQ}.rw_orders") == [(2,)]
    assert execute(f"SELECT count(*) FROM {FQ}.orders_snapshot") == [(3,)]

    # a changed seed file rebuilds the seed and its children
    seed = project / "seeds" / "countries.csv"
    seed.write_text(seed.read_text() + "it,Italy" + chr(10), encoding="utf-8")
    dbt(project)
    now = decisions()
    assert now["countries"] == ("build", "code_changed")
    assert now["dim_country"] == ("build", "new_upstream_data")
    assert execute(f"SELECT count(*) FROM {FQ}.dim_country") == [(3,)]

    # a changed column mask and a changed model-level constraint rebuild only their tables
    spec = project / "models" / "schema.yml"
    spec.write_text(
        spec.read_text()
        .replace("function: mask_zero", "function: mask_minus")
        .replace("columns: [id]", "columns: [id, amount]"),
        encoding="utf-8",
    )
    dbt(project)
    now = decisions()
    assert now["secured_orders"] == ("build", "config_changed")
    assert now["pk_orders"] == ("build", "config_changed")
    assert now["fct_orders"] == ("reuse", "no_new_data")
    if v2:
        assert execute(f"SELECT max(amount) FROM {FQ}.secured_orders") == [(-1,)]

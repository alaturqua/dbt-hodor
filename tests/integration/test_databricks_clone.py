"""Live clone test on Databricks. Opt in: `pytest -m databricks` with these env vars set:

HODOR_DBX_HOST, HODOR_DBX_HTTP_PATH, HODOR_DBX_TOKEN, HODOR_DBX_CATALOG, HODOR_DBX_SCHEMA.

The base target builds into HODOR_DBX_CATALOG.<HODOR_DBX_SCHEMA>_clone, which also holds the
state table of both targets. The dev target builds into <HODOR_DBX_SCHEMA>_clone_dev and names
the base target in `defer_to_target`, so unchanged tables are cloned from it. Both schemas are
dropped at the end (CASCADE), so point them at names that hold nothing else.
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

SCHEMA = f"{os.environ['HODOR_DBX_SCHEMA']}_clone"  # not the schema of the other live tests
PROD = f"`{os.environ['HODOR_DBX_CATALOG']}`.`{SCHEMA}`"
DEV = f"`{os.environ['HODOR_DBX_CATALOG']}`.`{SCHEMA}_dev`"

CONNECTION = """
              type: databricks
              host: "{{ env_var('HODOR_DBX_HOST') }}"
              http_path: "{{ env_var('HODOR_DBX_HTTP_PATH') }}"
              token: "{{ env_var('HODOR_DBX_TOKEN') }}"
              catalog: "{{ env_var('HODOR_DBX_CATALOG') }}"
              hodor_schema: "{{ env_var('HODOR_DBX_SCHEMA') }}"
              threads: 4
              connection_parameters:
                enable_telemetry: false"""

FILES = {
    "dbt_project.yml": """
        name: hodor_clone
        version: "1.0"
        profile: hodor_clone
        models:
          hodor_clone:
            +state:
              lag_tolerance: 0s
    """,
    "profiles.yml": f"""
        hodor_clone:
          target: prod
          outputs:
            prod:{CONNECTION}
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}"
            dev:{CONNECTION}
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}_dev"
              defer_to_target: prod
            dev_pre_clone:{CONNECTION}
              schema: "{{{{ env_var('HODOR_DBX_SCHEMA') }}}}_dev"
              defer_to_target: prod
              pre_clone: true
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
    # automatic liquid clustering: a shallow clone of it cannot be replaced in place
    "models/auto_orders.sql": """
        {{ config(materialized='table', auto_liquid_cluster=true) }}
        select id, amount from {{ source('raw', 'raw_orders') }}
    """,
    "models/inc_orders.sql": """
        {{ config(materialized='incremental', unique_key='id') }}
        select id, amount from {{ source('raw', 'raw_orders') }}
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
    for schema in (PROD, DEV):  # the delta store needs its schema before the first run
        execute(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT 'dbt-hodor live test'")
    execute(
        f"CREATE OR REPLACE TABLE {PROD}.raw_orders AS "
        "SELECT * FROM VALUES (1, 10), (2, 20) AS v(id, amount)"
    )
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("HODOR_DBX_SCHEMA", SCHEMA)  # the profile and the source read it
    yield root
    for schema in (DEV, PROD):
        execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def dbt(root: Path, target: str, *args: str) -> dict[str, tuple[str, str]]:
    result = dbtRunner().invoke(
        ["build", "--project-dir", str(root), "--profiles-dir", str(root), "--target", target]
        + list(args)
    )
    assert result.success, result.exception
    return {r.node.name: (str(r.status), r.message or "") for r in result.result.results}


def decisions(target: str) -> dict[str, tuple[str, str]]:
    rows = execute(
        f"SELECT unique_id, action, reason FROM {PROD}.hodor_state "
        f"WHERE target_name = '{target}' "
        "QUALIFY row_number() OVER (PARTITION BY unique_id ORDER BY recorded_at_ms DESC) = 1"
    )
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def history(relation: str) -> list[tuple[str, str]]:
    """(operation, operationParameters) of the newest commits first."""
    return [(r[4], str(r[5])) for r in execute(f"DESCRIBE HISTORY {relation} LIMIT 5")]


def rows(relation: str) -> list[tuple]:
    return execute(f"SELECT id, amount FROM {relation} ORDER BY id")


def test_live_clone_from_the_base_target(project):
    root = project
    dbt(root, "prod")

    # dev clones both tables: same code, config and source data as the base build
    first = dbt(root, "dev")
    assert first["stg_orders"] == ("success", "CLONED (cloned_from_base)")
    assert first["inc_orders"] == ("success", "CLONED (cloned_from_base)")
    assert first["auto_orders"] == ("success", "CLONED (cloned_from_base)")
    assert decisions("dev") == {
        "stg_orders": ("clone", "cloned_from_base"),
        "auto_orders": ("clone", "cloned_from_base"),
        "inc_orders": ("clone", "cloned_from_base"),
    }
    operation, parameters = history(f"{DEV}.stg_orders")[0]
    assert operation == "CLONE" and "isShallow" in parameters, parameters
    assert rows(f"{DEV}.stg_orders") == [(1, 10), (2, 20)]

    # pre_clone: the changed incremental is cloned, then runs on top of the clone
    model = root / "models" / "inc_orders.sql"
    model.write_text(
        model.read_text()
        + "where id > 0 {% if is_incremental() %} union all select 3, 30 {% endif %}\n",
        encoding="utf-8",
    )
    dbt(root, "dev_pre_clone", "-s", "inc_orders")
    assert decisions("dev_pre_clone")["inc_orders"] == ("build", "pre_cloned_from_base")
    operations = [op for op, _ in history(f"{DEV}.inc_orders")]
    # newest first; other commits (such as CLUSTER BY) may come in between
    assert "MERGE" in operations and "CLONE" in operations, operations
    assert operations.index("MERGE") < operations.index("CLONE"), operations
    assert rows(f"{DEV}.inc_orders") == [(1, 10), (2, 20), (3, 30)]

    # a base table written since its build is not cloned: dev builds its own
    execute(f"INSERT INTO {PROD}.stg_orders VALUES (9, 90)")
    dbt(root, "dev", "-s", "stg_orders")
    assert decisions("dev")["stg_orders"] == ("build", "no_history")
    assert rows(f"{DEV}.stg_orders") == [(1, 10), (2, 20)]

    # changed code over hodor's own clone: hodor drops the clone, then the table builds.
    # Without the drop, Databricks fails the replace of a shallow clone of a table with
    # automatic liquid clustering (CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR)
    properties = dict(execute(f"SHOW TBLPROPERTIES {DEV}.auto_orders"))
    assert properties.get("clusterByAuto") == "true", properties
    model = root / "models" / "auto_orders.sql"
    model.write_text(model.read_text() + "where id > 1\n", encoding="utf-8")
    rebuilt = dbt(root, "dev", "-s", "auto_orders")
    assert rebuilt["auto_orders"][0] == "success", rebuilt
    assert decisions("dev")["auto_orders"] == ("build", "no_history")  # a clone is no build
    assert history(f"{DEV}.auto_orders")[0][0] != "CLONE"
    assert rows(f"{DEV}.auto_orders") == [(2, 20)]
    assert rows(f"{PROD}.auto_orders") == [(1, 10), (2, 20)]  # the base table is untouched

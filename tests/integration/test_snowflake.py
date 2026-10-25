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

from dbt_hodor.metadata import UNKNOWN  # noqa: E402
from dbt_hodor.providers import SnowflakeProvider  # noqa: E402


def connect():
    return snowflake_connector.connect(
        account=ENV["ACCOUNT"],
        user=ENV["USER"],
        private_key_file=ENV["PRIVATE_KEY_PATH"],
        role=ENV["ROLE"],
        warehouse=ENV["WAREHOUSE"],
        database=ENV["DATABASE"],
    )


def execute(sql: str) -> list[tuple]:
    con = connect()
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
              - name: raw_lower
                identifier: raw_lower
                quoting:
                  identifier: true
    """,
    "models/lower_orders.sql": """
        {{ config(materialized='table') }}
        select id from {{ source('raw', 'raw_lower') }}
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
        # a quoted lowercase name: Snowflake stores it as written, not in uppercase
        execute(f'create table {schema}."raw_lower" as select 1 as id')
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


NODES = ("stg_orders", "fct_orders", "all_orders", "lower_orders", "v_orders")
BUILD = ("build", "new_upstream_data")
REUSE = ("reuse", "no_new_data")
VIEW_REUSE = ("reuse", "view_unchanged")


def expect(state: Path, **expected: tuple[str, str]) -> None:
    """Every node's latest (action, reason), against the full expectation."""
    assert set(expected) == set(NODES)
    assert decisions(state) == expected


def test_snowflake_lifecycle(project):
    root, schema, state = project
    raw = f"{schema}.raw_orders"
    lower = f'{schema}."raw_lower"'

    # 1. first run builds everything
    first = dbt(root)
    assert {status for status, _ in first.values()} == {"success"}
    no_history = ("build", "no_history")
    expect(state, **dict.fromkeys(NODES, no_history))

    # 2. nothing changed: tables are reused, and so is the unchanged view
    second = dbt(root)
    for name in ("stg_orders", "fct_orders", "all_orders", "lower_orders"):
        assert second[name] == ("reused", "REUSED (no_new_data)")
    assert second["v_orders"] == ("reused", "REUSED (view_unchanged)")
    expect(state, **dict.fromkeys(NODES[:4], REUSE), v_orders=VIEW_REUSE)

    # 3. new rows in both sources rebuild the tables; the view definition is unchanged
    execute(f"insert into {raw} values (2, 20)")
    execute(f"insert into {lower} values (2)")
    dbt(root)
    expect(state, **dict.fromkeys(NODES[:4], BUILD), v_orders=VIEW_REUSE)
    assert execute(f"select count(*) from {schema}.fct_orders") == [(2,)]
    assert execute(f"select count(*) from {schema}.lower_orders") == [(2,)]
    again = dbt(root)
    assert again["fct_orders"] == ("reused", "REUSED (no_new_data)")
    assert again["lower_orders"] == ("reused", "REUSED (no_new_data)")
    expect(state, **dict.fromkeys(NODES[:4], REUSE), v_orders=VIEW_REUSE)

    # 4. a changed view definition rebuilds the view; the tables stay reused
    path = root / "models" / "v_orders.sql"
    path.write_text(path.read_text().replace("amount2 from", "amount2 + 1 as amount2 from"))
    third = dbt(root)
    assert third["v_orders"][0] == "success"
    expect(state, **dict.fromkeys(NODES[:4], REUSE), v_orders=("build", "code_changed"))

    # 5. ADD COLUMN moves last_ddl: every table that reads the source rebuilds, and
    # the one that reads only the other source is reused
    execute(f"alter table {raw} add column note varchar")
    dbt(root)
    expect(
        state,
        stg_orders=BUILD,
        fct_orders=BUILD,
        all_orders=BUILD,
        lower_orders=REUSE,
        v_orders=VIEW_REUSE,
    )
    columns = execute(
        f"select column_name from {ENV['DATABASE']}.information_schema.columns "
        f"where table_schema = '{schema.upper()}' and table_name = 'ALL_ORDERS' "
        "order by ordinal_position"
    )
    assert [c[0] for c in columns] == ["ID", "AMOUNT", "NOTE"]
    dbt(root)
    expect(state, **dict.fromkeys(NODES[:4], REUSE), v_orders=VIEW_REUSE)

    # 6. create or replace with the same columns and no rows sends the commit token back to
    # 0, but the created time moves: downstream rebuilds
    execute(f"create or replace table {raw} (id number, amount number, note varchar)")
    execute(f"create or replace table {lower} (id number)")
    dbt(root)
    expect(state, **dict.fromkeys(NODES[:4], BUILD), v_orders=VIEW_REUSE)
    assert execute(f"select count(*) from {schema}.fct_orders") == [(0,)]
    assert execute(f"select count(*) from {schema}.lower_orders") == [(0,)]
    dbt(root)
    expect(state, **dict.fromkeys(NODES[:4], REUSE), v_orders=VIEW_REUSE)


class Shim:
    """Just enough of a dbt adapter for providers.fetch: one connector session."""

    def __init__(self, con):
        self.connections = self
        self._con = con

    def get_if_exists(self):
        return self._con

    def add_query(self, sql, auto_begin=True):
        cursor = self._con.cursor()
        cursor.execute(sql)
        return None, cursor


def test_snowflake_token_moves_on_each_change():
    schema = f"{ENV['SCHEMA']}_{uuid.uuid4().hex[:8]}"
    key = f"{ENV['DATABASE']}.{schema}.t".lower()
    con = connect()
    shim = Shim(con)

    def run(sql: str) -> None:
        con.cursor().execute(sql)

    def parts(target: str = key) -> list[str]:
        token = SnowflakeProvider(shim).refresh(target).token
        assert token is not None, f"unknown token for {target}"
        return token.split("|")

    previous: list[str] = []

    def moved(sql: str, expected: set[str]) -> list[str]:
        """Run the change; exactly the named parts (commit, created, last_ddl) must move."""
        nonlocal previous
        run(sql)
        now = parts()
        names = ("commit", "created", "last_ddl")
        assert {n for n, a, b in zip(names, now, previous, strict=True) if a != b} == expected
        previous = now
        return now

    try:
        run(f"create schema {schema}")
        run(f"create table {schema}.t (id number, v number)")
        previous = parts()
        assert previous[0] == "0"  # a new empty table

        moved(f"insert into {schema}.t values (1, 1)", {"commit"})
        # a plain read changes nothing
        run(f"select * from {schema}.t")
        assert parts() == previous
        moved(f"delete from {schema}.t", {"commit"})
        moved(f"update {schema}.t set v = 2 where id = -1", {"commit"})
        moved(f"insert into {schema}.t values (1, 1)", {"commit"})
        moved(f"truncate table {schema}.t", {"commit"})
        moved(
            f"merge into {schema}.t using (select 1 as id, 1 as v) s on t.id = s.id "
            "when not matched then insert (id, v) values (s.id, s.v)",
            {"commit"},
        )
        moved(f"alter table {schema}.t add column note varchar", {"last_ddl"})
        moved(f"comment on table {schema}.t is 'x'", {"last_ddl"})
        empty = moved(
            f"create or replace table {schema}.t (id number, v number, note varchar)",
            {"commit", "created", "last_ddl"},
        )
        assert empty[0] == "0"  # back to 0, which only created and last_ddl tell apart
        moved(
            f"create or replace table {schema}.t as select 1 as id, 1 as v, 'x' as note",
            {"commit", "created", "last_ddl"},
        )
        run(f"create table {schema}.other as select 2 as id, 2 as v, 'y' as note")
        moved(f"alter table {schema}.t swap with {schema}.other", {"commit", "created", "last_ddl"})

        missing = SnowflakeProvider(shim).refresh(f"{key}_missing")
        assert missing == UNKNOWN
    finally:
        run(f"drop schema if exists {schema} cascade")
        con.close()

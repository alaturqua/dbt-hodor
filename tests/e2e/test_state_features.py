"""End to end: `state:` selectors and --defer without --state on dbt-duckdb. A prod and a
dev target share one DuckDB store; dev names prod as its base target."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from dbt.cli.main import dbtRunner, dbtRunnerResult  # noqa: E402

FILES = {
    "dbt_project.yml": """
        name: state_e2e
        version: "1.0"
        profile: state_e2e
    """,
    "profiles.yml": """
        state_e2e:
          target: prod
          outputs:
            prod:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/prod.duckdb"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
            dev:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/dev.duckdb"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
              defer_to_target: prod
            dev_no_base:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/dev.duckdb"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
            dev_lost_store:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/dev.duckdb"
              schema: main
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_TMP') }}/missing/state.duckdb"
              defer_to_target: prod
            shared_prod:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/shared.duckdb"
              schema: prod
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
            shared_dev:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/shared.duckdb"
              schema: dev
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
              defer_to_target: shared_prod
            shared_dev_lost_store:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/shared.duckdb"
              schema: dev
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_TMP') }}/missing/state.duckdb"
              defer_to_target: shared_prod
            shared_dev_no_clones:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/shared.duckdb"
              schema: dev
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
              defer_to_target: shared_prod
              allow_clones: false
            shared_dev_pre_clone:
              type: duckdb
              path: "{{ env_var('E2E_TMP') }}/shared.duckdb"
              schema: dev
              threads: 2
              hodor_store: duckdb
              hodor_path: "{{ env_var('E2E_STATE') }}"
              defer_to_target: shared_prod
              pre_clone: true
    """,
    "models/stg_orders.sql": """
        {{ config(materialized='table') }}
        select 1 as id, 10 as amount
    """,
    "models/fct_orders.sql": """
        {{ config(materialized='table') }}
        select id, amount * 2 as amount2 from {{ ref('stg_orders') }}
    """,
    "models/rpt_orders.sql": """
        {{ config(materialized='view') }}
        select count(*) as n from {{ ref('fct_orders') }}
    """,
}


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    for name, body in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    monkeypatch.setenv("E2E_TMP", tmp_path.as_posix())
    monkeypatch.setenv("E2E_STATE", (tmp_path / "state.duckdb").as_posix())
    monkeypatch.setenv("HODOR_MODE", "on")
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    monkeypatch.chdir(root)
    return root


def invoke(root: Path, command: str, target: str, *args: str, callbacks=None) -> dbtRunnerResult:
    return dbtRunner(callbacks=callbacks).invoke(
        [command, "--project-dir", str(root), "--profiles-dir", str(root), "--target", target]
        + list(args)
    )


def ls(root: Path, target: str, *args: str) -> set[str]:
    result = invoke(root, "ls", target, "--resource-type", "model", "--output", "name", *args)
    assert result.success, result.exception
    return set(result.result)


def build(root: Path, target: str = "prod") -> None:
    result = invoke(root, "build", target)
    assert result.success, result.exception


def test_state_modified_compares_with_the_base_target_records(project):
    build(project)
    build(project)  # the latest prod records are reuses: they carry the hashes too
    build(project, "dev")  # parents to read: dev does not defer to prod
    fct = project / "models" / "fct_orders.sql"
    fct.write_text(fct.read_text().replace("* 2", "* 3"), encoding="utf-8")
    (project / "models" / "dim_orders.sql").write_text(
        "select id from {{ ref('stg_orders') }}\n", encoding="utf-8"
    )

    assert ls(project, "dev", "-s", "state:modified") == {"fct_orders", "dim_orders"}
    assert ls(project, "dev", "--exclude", "state:modified") == {"stg_orders", "rpt_orders"}
    assert ls(project, "dev", "-s", "state:new") == {"dim_orders"}
    assert ls(project, "dev", "-s", "state:modified+") == {
        "fct_orders",
        "rpt_orders",
        "dim_orders",
    }
    # a build selects first, then opens the same store to write its records
    built = invoke(project, "build", "dev", "-s", "state:modified")
    assert built.success, built.exception
    assert {r.node.name for r in built.result.results} == {"fct_orders", "dim_orders"}


def test_a_node_only_reused_in_the_base_target_is_not_new(project):
    build(project)
    build(project)  # every prod node is reused
    # the builds fell out of the lookback window: only the reuse records are left
    with duckdb.connect(str(project.parent / "state.duckdb")) as con:
        con.execute("delete from main.hodor_state where action = 'build'")
        reused = con.execute("select distinct status from main.hodor_state").fetchall()
    assert reused == [("reused",)]
    assert ls(project, "dev", "-s", "state:new") == set()
    assert ls(project, "dev", "-s", "state:modified") == set()


SOURCES_AND_TESTS = """
    sources:
      - name: raw
        schema: main
        tables:
          - name: things
    models:
      - name: stg_orders
        columns:
          - name: id
            data_tests: [not_null]
"""


def test_unchanged_tests_are_not_modified_and_sources_always_are(project):
    (project / "models" / "schema.yml").write_text(
        textwrap.dedent(SOURCES_AND_TESTS).strip() + "\n", encoding="utf-8"
    )
    (project / "models" / "from_src.sql").write_text(
        "select * from {{ source('raw', 'things') }}\n", encoding="utf-8"
    )
    for db in ("prod.duckdb", "dev.duckdb"):
        with duckdb.connect(str(project.parent / db)) as con:
            con.execute("create table main.things as select 1 as x")
    build(project)

    def selected(*args: str) -> set[str]:
        result = invoke(project, "ls", "dev", "--output", "name", *args)
        assert result.success, result.exception
        return set(result.result)

    # a generic test compares equal: its record hashes the macros it had at parse time
    assert selected("-s", "state:modified") == {"raw.things"}
    # a source has no record, so it is always new and modified, and so is its downstream
    assert selected("-s", "state:new") == {"raw.things"}
    assert selected("-s", "state:modified+") == {"raw.things", "from_src"}
    assert "not_null_stg_orders_id" in selected("-s", "state:unmodified")


def test_a_new_latest_version_is_a_modification(project):
    (project / "models" / "dim_customers_v1.sql").write_text("select 1 as id\n", encoding="utf-8")
    (project / "models" / "dim_customers_v2.sql").write_text("select 2 as id\n", encoding="utf-8")
    versions = """
        models:
          - name: dim_customers
            latest_version: 1
            versions:
              - v: 1
              - v: 2
    """
    schema = project / "models" / "schema.yml"
    schema.write_text(textwrap.dedent(versions).strip() + "\n", encoding="utf-8")
    build(project)
    schema.write_text(schema.read_text().replace("latest_version: 1", "latest_version: 2"))
    # an unpinned ref now resolves to v2: dbt-core marks both versions modified
    assert ls(project, "dev", "-s", "state:modified") == {"dim_customers.v1", "dim_customers.v2"}


def test_without_a_base_target_dbt_keeps_its_own_error(project):
    build(project)
    result = invoke(project, "ls", "dev_no_base", "-s", "state:modified")
    assert not result.success
    assert "no comparison manifest" in str(result.exception)


def test_a_store_that_cannot_be_read_fails_the_selection(project):
    result = invoke(project, "ls", "dev_lost_store", "-s", "state:modified")
    assert not result.success
    assert result.exception.msg.startswith("dbt-hodor cannot compare with prod: ")


def warnings(events: list) -> list[str]:
    return [e.info.msg for e in events if e.info.level == "warn"]


def test_defer_without_state_reads_the_base_target_relation(project):
    build(project, "shared_prod")
    build(project, "shared_prod")  # reuses record no success: the latest records are the builds
    built = invoke(project, "build", "shared_dev", "-s", "fct_orders", "--defer")
    assert built.success, built.exception
    (result,) = built.result.results
    assert '"shared"."prod"."stg_orders"' in result.node.compiled_code
    with duckdb.connect(str(project.parent / "shared.duckdb")) as con:
        assert con.execute("select amount2 from dev.fct_orders").fetchall() == [(20,)]


def test_defer_to_a_parent_never_built_in_the_base_target_fails(project):
    build(project, "shared_prod")
    (project / "models" / "stg_new.sql").write_text("select 1 as id\n", encoding="utf-8")
    (project / "models" / "fct_new.sql").write_text(
        "select id from {{ ref('stg_new') }}\n", encoding="utf-8"
    )
    built = invoke(project, "build", "shared_dev", "-s", "fct_new", "--defer")
    # no record of stg_new in prod: dbt reads dev's relation, which does not exist
    (result,) = built.result.results
    assert result.status == "error"
    assert '"shared"."dev"."stg_new"' in result.node.compiled_code
    assert "does not exist" in result.message


def test_defer_with_a_store_that_cannot_be_read_leaves_dbt_alone(project):
    events: list = []
    built = invoke(
        project,
        "build",
        "shared_dev_lost_store",
        "-s",
        "fct_orders",
        "--defer",
        callbacks=[events.append],
    )
    assert not built.success
    assert "--state or --defer-state are required for deferral" in str(built.exception)
    (warning,) = [m for m in warnings(events) if m.startswith("dbt-hodor")]
    assert warning.startswith("dbt-hodor could not defer to shared_prod: ")


def test_defer_without_a_base_target_keeps_dbt_error(project):
    built = invoke(project, "build", "dev_no_base", "-s", "fct_orders", "--defer")
    assert not built.success
    assert "--state or --defer-state are required for deferral" in str(built.exception)


# DuckDB cannot clone: dbt's can_clone_table is false. These macros let the tests clone by copy.
CLONE_MACROS = """
{% macro duckdb__can_clone_table() %}{{ return(True) }}{% endmacro %}
{% macro duckdb__create_or_replace_clone(this_relation, defer_relation) %}
  create or replace table {{ this_relation.render() }} as
  select * from {{ defer_relation.render() }}
{% endmacro %}
"""


def can_clone(root: Path, body: str = CLONE_MACROS) -> None:
    (root / "macros").mkdir(exist_ok=True)
    (root / "macros" / "clone.sql").write_text(body, encoding="utf-8")


def latest(root: Path, target: str) -> dict[str, tuple[str, str]]:
    """(action, reason) of each node's newest record in the target."""
    with duckdb.connect(str(root.parent / "state.duckdb"), read_only=True) as con:
        rows = con.execute(
            "select unique_id, action, reason from main.hodor_state where target_name = ? "
            "qualify row_number() over (partition by unique_id order by recorded_at_ms desc) = 1",
            [target],
        ).fetchall()
    return {uid.split(".")[2]: (action, reason) for uid, action, reason in rows}


def shared(root: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(root.parent / "shared.duckdb")) as con:
        return con.execute(sql).fetchall()


def run(root: Path, target: str, *args: str, callbacks=None) -> dict[str, str]:
    built = invoke(root, "build", target, *args, callbacks=callbacks)
    assert built.success, built.exception
    return {r.node.name: r.message or "" for r in built.result.results}


def test_an_unchanged_node_is_cloned_from_the_base_target(project):
    can_clone(project)
    build(project, "shared_prod")
    events: list = []
    messages = run(project, "shared_dev", callbacks=[events.append])
    lines = [e.info.msg for e in events if e.info.name == "LogModelResult"]
    assert any("CLONED sql table model dev.fct_orders" in line for line in lines), lines
    assert messages["stg_orders"] == "CLONED (cloned_from_base)"
    assert messages["fct_orders"] == "CLONED (cloned_from_base)"
    assert latest(project, "shared_dev") == {
        "stg_orders": ("clone", "cloned_from_base"),
        "fct_orders": ("clone", "cloned_from_base"),
        "rpt_orders": ("build", "no_history"),  # a view is never cloned
    }
    assert shared(project, "select amount2 from dev.fct_orders") == [(20,)]
    assert shared(project, "select n from dev.rpt_orders") == [(1,)]


def test_without_can_clone_table_the_node_builds(project):
    build(project, "shared_prod")
    run(project, "shared_dev")
    assert latest(project, "shared_dev") == {
        "stg_orders": ("build", "no_history"),
        "fct_orders": ("build", "no_history"),
        "rpt_orders": ("build", "no_history"),
    }


def test_a_base_table_written_since_its_build_is_not_cloned(project):
    can_clone(project)
    build(project, "shared_prod")
    shared(project, "update prod.fct_orders set amount2 = 99")
    run(project, "shared_dev")
    found = latest(project, "shared_dev")
    assert found["stg_orders"] == ("clone", "cloned_from_base")
    assert found["fct_orders"] == ("build", "no_history")
    assert shared(project, "select amount2 from dev.fct_orders") == [(20,)]


@pytest.mark.parametrize("target", ["shared_dev_no_clones", "shared_dev"])
def test_no_clone_with_clones_off_or_in_shadow_mode(project, monkeypatch, target):
    can_clone(project)
    build(project, "shared_prod")
    if target == "shared_dev":
        monkeypatch.setenv("HODOR_MODE", "shadow")
    run(project, target)
    assert {action for action, _ in latest(project, target).values()} == {"build"}


def test_a_clone_that_fails_builds_the_node(project):
    can_clone(project, CLONE_MACROS.replace("create or replace table", "create or replace tabel"))
    build(project, "shared_prod")
    events: list = []
    run(project, "shared_dev", callbacks=[events.append])
    assert {action for action, _ in latest(project, "shared_dev").values()} == {"build"}
    assert shared(project, "select amount2 from dev.fct_orders") == [(20,)]
    cloned = [m for m in warnings(events) if m.startswith("dbt-hodor: cannot clone")]
    assert len(cloned) == 2, warnings(events)  # stg_orders and fct_orders, once each


INCREMENTAL = """
{{ config(materialized='incremental', unique_key='id') }}
select id, amount from {{ ref('stg_orders') }}
"""


def test_pre_clone_runs_the_changed_incremental_on_top_of_the_clone(project):
    can_clone(project)
    model = project / "models" / "inc_orders.sql"
    model.write_text(INCREMENTAL, encoding="utf-8")
    build(project, "shared_prod")
    # changed code, which adds a row only when it runs incrementally
    changed = "where id > 0 {% if is_incremental() %} union all select 2, 99 {% endif %}\n"
    model.write_text(INCREMENTAL + changed, encoding="utf-8")
    run(project, "shared_dev_pre_clone", "-s", "stg_orders inc_orders")
    assert latest(project, "shared_dev_pre_clone")["inc_orders"] == (
        "build",
        "pre_cloned_from_base",
    )
    assert shared(project, "select id, amount from dev.inc_orders order by id") == [
        (1, 10),
        (2, 99),
    ]


def test_a_clone_that_fails_after_writing_builds_from_nothing(project):
    # the clone statement writes the table, then fails
    can_clone(
        project,
        CLONE_MACROS.replace(
            "render() }}\n{% endmacro %}", "render() }}; select error('boom')\n{% endmacro %}"
        ),
    )
    model = project / "models" / "inc_orders.sql"
    model.write_text(
        INCREMENTAL.replace("unique_key='id'", "incremental_strategy='append'"), encoding="utf-8"
    )
    build(project, "shared_prod")
    run(project, "shared_dev", "-s", "stg_orders inc_orders")
    assert latest(project, "shared_dev")["inc_orders"] == ("build", "no_history")
    assert shared(project, "select id, amount from dev.inc_orders") == [(1, 10)]  # not twice


def test_a_node_whose_parent_is_deferred_builds(project):
    # on dbt-core the parent read through --defer is not matched to the base record
    can_clone(project)
    build(project, "shared_prod")
    messages = run(project, "shared_dev", "-s", "fct_orders", "--defer")
    assert messages == {"fct_orders": "OK"}
    assert latest(project, "shared_dev") == {"fct_orders": ("build", "no_history")}
    assert shared(project, "select amount2 from dev.fct_orders") == [(20,)]

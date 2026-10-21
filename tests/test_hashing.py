import pytest

from dbt_hodor.hashing import (
    INVOCATION_PLACEHOLDER,
    RUN_STARTED_PLACEHOLDER,
    SqlAnalysis,
    analyze_node_sql,
    analyze_sql,
    config_hash,
    normalize_runtime_values,
    text_hash,
    unrendered_hash,
)

BASE = "select a, b from cat.sch.t where a > 1"


def test_normalize_runtime_values_replaces_both_values():
    sql = "select 'inv-1' as invocation_id, '2026-10-20 04:00:00' as run_started_at"
    out = normalize_runtime_values(sql, "inv-1", "2026-10-20 04:00:00")
    assert out == (
        f"select '{INVOCATION_PLACEHOLDER}' as invocation_id, "
        f"'{RUN_STARTED_PLACEHOLDER}' as run_started_at"
    )


def test_normalize_runtime_values_ignores_empty_values():
    assert normalize_runtime_values(BASE, None, "") == BASE


def test_text_hash_is_stable_and_sensitive():
    assert text_hash(BASE) == text_hash(BASE)
    assert text_hash(BASE) != text_hash(BASE + " ")


@pytest.mark.parametrize(
    "variant",
    [
        "SELECT A, B FROM CAT.SCH.T WHERE A > 1",
        "select `a`, `b` from `cat`.`sch`.`t` where a > 1",
        "select a, b -- comment\nfrom cat.sch.t /* another */ where a > 1",
        "select   a,\n  b\nfrom cat.sch.t\nwhere a > 1",
    ],
)
def test_code_hash_ignores_case_quoting_comments_and_whitespace(variant):
    base = analyze_sql(BASE)
    assert base.code_hash.startswith("qualify:")
    assert analyze_sql(variant).code_hash == base.code_hash


def test_code_hash_ignores_table_alias_names():
    one = analyze_sql("select x.a from cat.sch.t as x join cat.sch.u as y on x.id = y.id")
    two = analyze_sql("select p.a from cat.sch.t as p join cat.sch.u as q on p.id = q.id")
    assert one.code_hash == two.code_hash
    # Without a schema sqlglot cannot qualify bare columns: one extra build, never a wrong reuse.
    assert (
        analyze_sql("select t.a from cat.sch.t").code_hash
        != analyze_sql("select a from cat.sch.t").code_hash
    )


@pytest.mark.parametrize(
    "changed",
    [
        "select a, c from cat.sch.t where a > 1",
        "select a, b from cat.sch.t where a > 2",
        "select a, b from cat.sch.u where a > 1",
        "select a, b from cat.sch.t where a >= 1",
        "select distinct a, b from cat.sch.t where a > 1",
    ],
)
def test_code_hash_detects_logic_changes(changed):
    assert analyze_sql(changed).code_hash != analyze_sql(BASE).code_hash


def test_renamed_columns_through_cte_do_not_collide():
    # canonicalize_internal_names gave these two the same output; qualify must not.
    one = "with s as (select a, b from cat.sch.t) select a, b from s"
    two = "with s as (select z, b from cat.sch.t) select z as a, b from s"
    assert analyze_sql(one).code_hash != analyze_sql(two).code_hash


@pytest.mark.parametrize(
    ("sql", "star"),
    [
        ("select * from cat.sch.t", True),
        ("select t.* from cat.sch.t as t", True),
        ("select a from cat.sch.t", False),
        ("with s as (select a from cat.sch.t) select * from s", False),
        ("with s as (select * from cat.sch.t) select * from s", True),
        ("select * from (select a from cat.sch.t) as x", False),
        ("select * from (select * from cat.sch.t) as x", True),
        ("select a from cat.sch.t union all select * from cat.sch.u", True),
        ("with s as (select a from cat.sch.t) select * from s join cat.sch.u on true", True),
    ],
)
def test_select_star_detection(sql, star):
    assert analyze_sql(sql).select_star is star


def test_table_references():
    sql = """
        with base as (select * from `Cat`.`Sch`.`Orders`)
        select * from base
        join cat.sch.customers c on true
        join delta.`/Volumes/Cat/Raw/Files/x` d on true
        where exists (select 1 from cat.other.flags)
    """
    analysis = analyze_sql(sql)
    assert analysis.tables == {"cat.sch.orders", "cat.sch.customers", "cat.other.flags"}
    assert analysis.delta_paths == {"/Volumes/Cat/Raw/Files/x"}
    assert analysis.opaque_inputs is False


@pytest.mark.parametrize(
    "sql",
    [
        "select * from read_files('/Volumes/c/s/v/f.csv', format => 'csv')",
        "select a from sch.t",
        "select a from t",
    ],
)
def test_untraceable_inputs_are_opaque(sql):
    assert analyze_sql(sql).opaque_inputs is True


@pytest.mark.parametrize("sql", ["select from where (", "select 1; select 2"])
def test_text_fallback_is_conservative(sql):
    analysis = analyze_sql(sql)
    assert analysis.code_hash.startswith("text:")
    assert analysis.select_star is True
    assert analysis.opaque_inputs is True


def test_sql_analysis_json_round_trip():
    analysis = analyze_sql("select * from cat.sch.t join delta.`/a/b` on true")
    assert SqlAnalysis.from_json(analysis.to_json()) == analysis


@pytest.mark.parametrize(
    "data",
    [
        None,
        "text",
        {},
        {
            "code_hash": "x",
            "select_star": "no",
            "tables": [],
            "delta_paths": [],
            "opaque_inputs": False,
        },
        {
            "code_hash": "x",
            "select_star": False,
            "tables": [1],
            "delta_paths": [],
            "opaque_inputs": False,
        },
    ],
)
def test_sql_analysis_from_json_rejects_malformed(data):
    assert SqlAnalysis.from_json(data) is None


def test_analyze_node_sql_uses_stored_facts_when_text_unchanged():
    sql = "select 'inv-1' as id, a from cat.sch.t"
    key, fresh = analyze_node_sql(sql, invocation_id="inv-1")
    stored = SqlAnalysis("qualify:stored", False, frozenset({"cat.sch.t"}))
    # a new invocation id must not defeat the fast path
    same_key, cached = analyze_node_sql(
        sql.replace("inv-1", "inv-2"), "inv-2", None, key, stored.to_json()
    )
    assert same_key == key
    assert cached == stored
    assert fresh.code_hash != stored.code_hash


def test_analyze_node_sql_reanalyzes_on_change_or_bad_facts():
    key, fresh = analyze_node_sql(BASE)
    stored = SqlAnalysis("qualify:stored", False).to_json()
    assert (
        analyze_node_sql(BASE + " and b = 1", None, None, key, stored)[1].code_hash
        != "qualify:stored"
    )
    assert analyze_node_sql(BASE, None, None, key, {"bad": 1})[1] == fresh


def test_config_hash_ignores_non_data_keys():
    base = {"materialized": "table", "tags": ["a"], "meta": {"owner": "x"}}
    assert config_hash(base) == config_hash({"materialized": "table", "tags": ["b"]})
    assert config_hash(base) != config_hash({"materialized": "incremental"})
    row_filter = {"function": "region_filter", "columns": ["region"]}
    assert config_hash({"row_filter": row_filter}) != config_hash({})


def test_config_hash_uses_column_types_and_constraints_only():
    cols = {"a": {"data_type": "int", "description": "one"}}
    assert config_hash({}, cols) == config_hash(
        {}, {"a": {"data_type": "int", "description": "two"}}
    )
    assert config_hash({}, cols) != config_hash({}, {"a": {"data_type": "bigint"}})


def test_config_hash_sees_column_masks_and_tags():
    cols = {"ssn": {"data_type": "string", "constraints": []}}
    masked = {"ssn": {**cols["ssn"], "column_mask": {"function": "mask_ssn"}}}
    assert config_hash({}, cols) == config_hash(
        {}, {"ssn": {**cols["ssn"], "column_mask": None}}
    )  # unset: hash unchanged
    assert config_hash({}, masked) != config_hash({}, cols)
    assert config_hash({}, masked) != config_hash(
        {}, {"ssn": {**cols["ssn"], "column_mask": {"function": "mask_all"}}}
    )
    assert config_hash({}, {"ssn": {**cols["ssn"], "databricks_tags": {"pii": "yes"}}}) != (
        config_hash({}, cols)
    )


def test_config_hash_sees_model_level_constraints():
    pk = [{"type": "primary_key", "columns": ["id"]}]
    assert config_hash({}, None, []) == config_hash({})  # no constraints: hash unchanged
    assert config_hash({}, None, pk) != config_hash({})
    assert config_hash({}, None, pk) != config_hash(
        {}, None, [{"type": "primary_key", "columns": ["id", "day"]}]
    )


@pytest.mark.parametrize(
    ("sql", "dialect", "logic", "stamps"),
    [
        ("select a from c.s.t", "databricks", set(), set()),
        # stamps: output columns of the final SELECT that read no column
        ("select current_date() as d from c.s.t", "databricks", set(), {"date"}),
        ("select a, current_timestamp() as _loaded_at from c.s.t", "databricks", set(), {"always"}),
        ("select cast(current_date() as string) as d from c.s.t", "databricks", set(), {"date"}),
        (
            "select current_user() as built_by, uuid() as id from c.s.t",
            "databricks",
            set(),
            {"session", "always"},
        ),
        (
            "select a, now() as ts from c.s.t union all select b, now() from c.s.u",
            "databricks",
            set(),
            {"always"},
        ),
        # logic: filters, joins, values computed from columns, CTEs and subqueries
        (
            "select a from c.s.t where d >= date_sub(current_date(), 7)",
            "databricks",
            {"date"},
            set(),
        ),
        ("select datediff(current_date(), dob) as age from c.s.t", "databricks", {"date"}, set()),
        ("select a from c.s.t where is_member('admins')", "databricks", {"session"}, set()),
        ("select a from c.s.t where rand() < 0.1", "databricks", {"always"}, set()),
        (
            "with x as (select current_date() as d) select t.a from c.s.t join x on t.d = x.d",
            "databricks",
            {"date"},
            set(),
        ),
        (
            "select a, current_date() as d from c.s.t where ts > current_timestamp()",
            "databricks",
            {"always"},
            {"date"},
        ),
        # not volatile
        ("select unix_timestamp(x) as ts from c.s.t", "databricks", set(), set()),
        ("select uuid, user from c.s.t", "databricks", set(), set()),  # columns, not functions
        ("select a from c.s.t where unix_timestamp() > 0", "databricks", {"always"}, set()),
        (
            "select a from c.s.t where monotonically_increasing_id() > 0",
            "databricks",
            {"always"},
            set(),
        ),
        (
            "select a from c.s.t where today() > d and now() > ts",
            "duckdb",
            {"date", "always"},
            set(),
        ),
        ("select current_timestamp as ts from c.s.t", "duckdb", set(), {"always"}),
    ],
)
def test_volatile_classes(sql, dialect, logic, stamps):
    analysis = analyze_sql(sql, dialect)
    assert (analysis.volatile, analysis.volatile_stamps) == (frozenset(logic), frozenset(stamps))
    assert SqlAnalysis.from_json(analysis.to_json()) == analysis


def test_invocation_id_and_run_started_at_are_volatile():
    stamp = "select a, 'inv-1' as loaded_by, '2026-10-21 04:00:00' as loaded_at from c.s.t"
    _, analysis = analyze_node_sql(stamp, "inv-1", "2026-10-21 04:00:00")
    assert (analysis.volatile, analysis.volatile_stamps) == (set(), {"always"})
    _, analysis = analyze_node_sql(
        "select a from c.s.t where ts > '2026-10-21 04:00:00'", None, "2026-10-21 04:00:00"
    )
    assert analysis.volatile == {"always"}
    _, fallback = analyze_node_sql("select 'inv-1' from (((", "inv-1")
    assert fallback.code_hash.startswith("text:") and fallback.volatile == {"always"}


def test_cached_facts_without_volatile_are_reanalyzed():
    data = analyze_sql(BASE).to_json()
    del data["volatile_stamps"]
    assert SqlAnalysis.from_json(data) is None


def test_config_hash_ignores_data_type_spelling():
    def cols(data_type):
        return {"amount": {"data_type": data_type, "constraints": []}}

    assert config_hash({}, cols("decimal(10,2)")) == config_hash({}, cols("DECIMAL(10, 2)"))
    assert config_hash({}, cols("decimal(10,2)")) != config_hash({}, cols("decimal(12,2)"))
    assert config_hash({}, cols("struct<a:int>")) != config_hash({}, cols("struct<a:bigint>"))


def test_config_hash_sees_execution_macros():
    macro = {"macro.p.duckdb__create_table_as": "select *, 1 as _audit"}
    assert config_hash({}, None, None, {}) == config_hash({})
    assert config_hash({}, None, None, macro) != config_hash({})


def test_unrendered_hash_sees_macro_changes():
    one = unrendered_hash("select {{ m() }}", {"macro.p.m": "1"}, {"materialized": "view"})
    two = unrendered_hash("select {{ m() }}", {"macro.p.m": "2"}, {"materialized": "view"})
    assert one != two

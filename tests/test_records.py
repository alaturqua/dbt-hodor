from dbt_hodor.records import (
    COLUMN_NAMES,
    Record,
    add_column_sql,
    create_table_sql,
    latest_builds_sql,
    latest_records_sql,
)


def test_row_round_trip(record):
    original = record(test_result={"status": "pass", "failures": 0})
    row = original.to_row()
    assert len(row) == len(COLUMN_NAMES)
    assert row[COLUMN_NAMES.index("upstreams")] == '{"c.raw.orders":"3","c.s.dim":"12"}'
    assert Record.from_row(row) == original


def test_from_row_casts_ms_and_defaults_upstreams(record):
    row = list(record(upstreams={}).to_row())
    row[COLUMN_NAMES.index("recorded_at_ms")] = "1000000"
    row[COLUMN_NAMES.index("upstreams")] = None
    parsed = Record.from_row(row)
    assert parsed.recorded_at_ms == 1_000_000
    assert parsed.upstreams == {}


def test_create_table_sql_per_dialect():
    delta = create_table_sql("`c`.`s`.`hodor_state`", "delta")
    assert "record_id STRING" in delta
    assert "recorded_at_ms BIGINT" in delta
    assert delta.endswith("CLUSTER BY (target_name, node_key)")
    duck = create_table_sql("hodor_state", "duckdb")
    assert "record_id VARCHAR" in duck
    assert "CLUSTER BY" not in duck
    assert "record_id TEXT" in create_table_sql("hodor_state", "postgres")


def test_latest_builds_sql_avoids_qualify():
    sql = latest_builds_sql("t", "?", "?", "?").upper()
    assert "QUALIFY" not in sql
    assert "ROW_NUMBER()" in sql


def test_create_table_sql_for_trino():
    trino = create_table_sql('"c"."s"."hodor_state"', "trino")
    assert "record_id varchar" in trino
    assert "recorded_at_ms bigint" in trino
    assert "CLUSTER BY" not in trino


def test_node_hashes_round_trip(record):
    hashes = {"engine": "v1", "relation": '"Db"."S"."Fct"', "body": "b", "macros": None}
    original = record(node_hashes=hashes)
    row = original.to_row()
    assert row[COLUMN_NAMES.index("node_hashes")] == (
        '{"body":"b","engine":"v1","macros":null,"relation":"\\"Db\\".\\"S\\".\\"Fct\\""}'
    )
    assert Record.from_row(row).node_hashes == hashes
    assert record().node_hashes is None
    assert Record.from_row(record().to_row()).node_hashes is None


def test_add_column_sql_per_dialect():
    assert add_column_sql("t", "delta") == "ALTER TABLE t ADD COLUMN node_hashes STRING"
    assert add_column_sql("t", "duckdb") == "ALTER TABLE t ADD COLUMN node_hashes VARCHAR"
    assert add_column_sql("t", "postgres") == "ALTER TABLE t ADD COLUMN node_hashes TEXT"
    assert add_column_sql("t", "trino") == "ALTER TABLE t ADD COLUMN node_hashes varchar"


def test_latest_records_sql_takes_any_action_and_successful_status():
    sql = latest_records_sql("t", "?", "?", "?")
    assert "action IN" not in sql and "action =" not in sql
    assert "status IN ('success', 'pass', 'warn', 'fail')" in sql
    assert "QUALIFY" not in sql.upper()


def test_latest_records_sql_for_selection_takes_reuse_records_too():
    sql = latest_records_sql("t", "?", "?", "?", reused=True)
    assert "status IN ('success', 'pass', 'warn', 'fail', 'reused')" in sql
    assert "action IN" not in sql


def test_reads_select_null_when_the_table_has_no_node_hashes():
    sql = latest_records_sql("t", "?", "?", "?", has_hashes=False)
    assert "NULL AS node_hashes" in sql
    assert "NULL AS node_hashes" not in latest_records_sql("t", "?", "?", "?")


def test_latest_records_sql_with_any_status_drops_the_status_filter():
    sql = latest_records_sql("t", "?", "?", "?", any_status=True)
    assert "status" not in sql.split("WHERE", 1)[1].split(")", 1)[0]
    assert "status IN" not in sql
    assert "recorded_at_ms >= ?" in sql and "project_name = ?" in sql

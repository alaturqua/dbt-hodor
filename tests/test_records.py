from dbt_hodor.records import COLUMN_NAMES, Record, create_table_sql, latest_builds_sql


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

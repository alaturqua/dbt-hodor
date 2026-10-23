from datetime import datetime, timezone

import pytest

from dbt_hodor.metadata import (
    UNKNOWN,
    DataVersion,
    HistoryEntry,
    delta_data_version,
    duckdb_fingerprint_sql,
    history_entry_from_row,
    is_data_change,
)


def entry(version, operation, metrics=None, ts=None):
    return HistoryEntry(version, ts, operation, metrics or {})


@pytest.mark.parametrize(
    ("operation", "metrics", "changed"),
    [
        ("OPTIMIZE", {"numOutputRows": "10"}, False),
        ("optimize", {}, False),
        ("VACUUM END", {}, False),
        ("SET TBLPROPERTIES", {}, False),
        ("CHANGE COLUMN", {}, False),
        ("WRITE", {"numOutputRows": "0"}, False),
        ("WRITE", {"numOutputRows": "5"}, True),
        ("MERGE", {"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0"}, False),
        ("MERGE", {"numTargetRowsInserted": "0", "numTargetRowsUpdated": "3"}, True),
        ("DELETE", {"numDeletedRows": 2}, True),
        ("CREATE OR REPLACE TABLE AS SELECT", {"numOutputRows": "10"}, True),
        # a replace that writes 0 rows empties the table (seen on a workspace)
        ("CREATE OR REPLACE TABLE AS SELECT", {"numOutputRows": "0", "numRemovedFiles": "5"}, True),
        ("WRITE", {"numOutputRows": "0", "numRemovedFiles": "3"}, True),
        ("REPLACE TABLE AS SELECT", {"numOutputRows": "0", "numRemovedFiles": "0"}, False),
        ("CREATE TABLE", {}, True),
        ("SOME NEW OPERATION", {}, True),
        ("WRITE", {"numOutputRows": "many"}, True),
        ("WRITE", {"numFiles": "0"}, True),
    ],
)
def test_is_data_change(operation, metrics, changed):
    assert is_data_change(entry(1, operation, metrics)) is changed


def test_history_entry_from_row_parses_metric_forms_and_utc_timestamps():
    naive = datetime(2026, 10, 20, 4, 0, 0)
    ms = int(datetime(2026, 10, 20, 4, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    rows = [
        {
            "version": 3,
            "timestamp": naive,
            "operation": "WRITE",
            "operationMetrics": '{"numOutputRows": "1"}',
        },
        {
            "version": "3",
            "timestamp": naive,
            "operation": "WRITE",
            "operationMetrics": {"numOutputRows": "1"},
        },
        {
            "version": 3,
            "timestamp": naive,
            "operation": "WRITE",
            "operationMetrics": [("numOutputRows", "1")],
        },
    ]
    for row in rows:
        assert history_entry_from_row(row) == HistoryEntry(3, ms, "WRITE", {"numOutputRows": "1"})
    bare = history_entry_from_row({"version": 1, "operationMetrics": "not json"})
    assert bare == HistoryEntry(1, None, "", {})


def test_delta_data_version_takes_latest_data_change():
    history = [
        entry(5, "OPTIMIZE", ts=500),
        entry(4, "WRITE", {"numOutputRows": "0"}, ts=400),
        entry(3, "MERGE", {"numTargetRowsInserted": "7"}, ts=300),
        entry(2, "WRITE", {"numOutputRows": "1"}, ts=200),
    ]
    assert delta_data_version(history) == DataVersion("3@300", 300)


def test_a_recreated_table_with_the_same_version_is_a_change():
    # a table swapped in from a staging table restarts its versions
    before = delta_data_version([entry(1, "WRITE", {"numOutputRows": "2"}, ts=100)])
    after = delta_data_version([entry(1, "WRITE", {"numOutputRows": "3"}, ts=900)])
    assert before.token != after.token


def test_delta_data_version_unknown_without_data_change():
    assert delta_data_version([entry(9, "OPTIMIZE")]) == UNKNOWN
    assert delta_data_version([]) == UNKNOWN
    assert not UNKNOWN.known


def test_duckdb_fingerprint_is_order_independent_and_content_sensitive():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("create table a as select * from (values (1, 'x'), (2, 'y')) v(id, name)")
    con.execute("create table b as select * from (values (2, 'y'), (1, 'x')) v(id, name)")
    con.execute("create table c as select * from (values (1, 'x'), (2, 'z')) v(id, name)")
    con.execute("create table e (id int)")

    def token(rel):
        return con.execute(duckdb_fingerprint_sql(rel)).fetchone()[0]

    assert token("a") == token("b")
    assert token("a") != token("c")
    assert token("a").startswith("2:")
    assert token("e") == "0:0"

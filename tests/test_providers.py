import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from dbt_hodor.metadata import UNKNOWN, DataVersion
from dbt_hodor.providers import (
    DeltaProvider,
    DuckDBProvider,
    TrinoProvider,
    fetch,
    store_executor,
)


class FakeCursor:
    def __init__(self, names, rows):
        self.description = [(n,) for n in names] if names else None
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnections:
    def __init__(self, adapter):
        self._adapter = adapter

    def get_if_exists(self):
        return self._adapter.open.get(threading.get_ident())

    def add_query(self, sql, auto_begin=True):
        assert self.get_if_exists() is not None, "query outside a connection"
        assert auto_begin is False
        with self._adapter.lock:
            self._adapter.queries.append(sql)
        names, rows = self._adapter.answer(sql)
        return None, FakeCursor(names, rows)


class FakeAdapter:
    def __init__(self, answer):
        self.answer = answer
        self.queries = []
        self.opened = []
        self.open = {}
        self.lock = threading.Lock()
        self.connections = FakeConnections(self)

    @contextmanager
    def connection_named(self, name):
        tid = threading.get_ident()
        with self.lock:
            self.opened.append(name)
        self.open[tid] = name
        try:
            yield
        finally:
            self.open.pop(tid, None)


def fingerprints(sql):
    if "broken" in sql:
        raise RuntimeError("no such table")
    return ["fp"], [(f"fp-of:{sql.split(' from ')[1]}",)]


def test_fetch_opens_a_connection_only_when_the_thread_has_none():
    adapter = FakeAdapter(lambda sql: (["x"], [(1,)]))
    assert fetch(adapter, "select 1") == (["x"], [(1,)])
    assert adapter.opened == ["hodor"]
    with adapter.connection_named("model.p.x"):
        fetch(adapter, "select 1")
    assert adapter.opened == ["hodor", "model.p.x"]


def test_fetch_handles_statements_without_rows():
    adapter = FakeAdapter(lambda sql: ([], []))
    assert fetch(adapter, "insert into t values (1)") == ([], [])


def test_duckdb_provider_quotes_caches_and_refreshes():
    adapter = FakeAdapter(fingerprints)
    provider = DuckDBProvider(adapter)
    first = provider.version("db.main.orders")
    assert first == DataVersion('fp-of:"db"."main"."orders" as t')
    assert provider.version("db.main.orders") is first
    assert len(adapter.queries) == 1
    provider.refresh("db.main.orders")
    assert len(adapter.queries) == 2


def test_query_failures_give_unknown_and_are_cached():
    adapter = FakeAdapter(fingerprints)
    messages = []
    provider = DuckDBProvider(adapter, messages.append)
    assert provider.version("db.main.broken") == UNKNOWN
    assert provider.version("db.main.broken") == UNKNOWN
    assert len(adapter.queries) == 1
    assert "no such table" in messages[0]


def test_prefetch_reads_each_missing_key_once_in_parallel():
    adapter = FakeAdapter(fingerprints)
    provider = DuckDBProvider(adapter)
    provider.version("db.main.t0")
    keys = [f"db.main.t{i}" for i in range(10)] + ["db.main.t3"]
    provider.prefetch(keys, threads=4)
    assert len(adapter.queries) == 10  # t0 was cached, t3 is listed twice
    assert adapter.opened.count("hodor_metadata") == 4
    before = len(adapter.queries)
    for key in keys:
        provider.version(key)
    assert len(adapter.queries) == before


def test_prefetch_with_nothing_to_do_opens_nothing():
    adapter = FakeAdapter(fingerprints)
    DuckDBProvider(adapter).prefetch([], threads=8)
    assert adapter.opened == []


def history(sql):
    rows = [
        (9, datetime(2026, 10, 20, 4, 0), "OPTIMIZE", {"numOutputRows": "5"}),
        (8, datetime(2026, 10, 20, 3, 0), "MERGE", {"numTargetRowsInserted": "2"}),
    ]
    return ["version", "timestamp", "operation", "operationMetrics"], rows


def test_delta_provider_reads_history_with_backtick_quoting():
    adapter = FakeAdapter(history)
    provider = DeltaProvider(adapter)
    version = provider.version("cat.sch.odd`name")
    assert version.token == "8@1792465200000"  # version and commit time
    assert adapter.queries == ["DESCRIBE HISTORY `cat`.`sch`.`odd``name` LIMIT 20"]
    path = provider.path_version("/Volumes/Cat/raw/x")
    assert path.token == "8@1792465200000"
    assert adapter.queries[-1] == "DESCRIBE HISTORY delta.`/Volumes/Cat/raw/x` LIMIT 20"


def test_clock_and_store_executor():
    adapter = FakeAdapter(lambda sql: (["now"], [(1_760_000_000_000,)]))
    assert DeltaProvider(adapter).now_ms() == 1_760_000_000_000
    assert adapter.queries == ["select unix_millis(current_timestamp())"]
    assert store_executor(adapter)("select 1") == [(1_760_000_000_000,)]


UTC = timezone.utc
ICEBERG = ["made_current_at", "snapshot_id", "parent_id", "is_current_ancestor"]


LOG = ["timestamp", "file", "latest_snapshot_id", "latest_schema_id", "latest_sequence_number"]
LOG_ENTRY = (
    datetime(2026, 10, 20, 7, tzinfo=UTC),
    "local:///t/metadata/00001.metadata.json",
    1,
    0,
    1,
)


def iceberg(*rows, log=(LOG_ENTRY,)):
    def answer(sql):
        if "$metadata_log_entries" in sql:
            return LOG, list(log)
        return ICEBERG, list(rows)

    return answer


def fail(sql):
    raise RuntimeError("Table 'c.s.t$history' does not exist")


def test_trino_iceberg_uses_the_latest_current_ancestor():
    adapter = FakeAdapter(
        iceberg(
            (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True),
            (datetime(2026, 10, 20, 5, tzinfo=UTC), 22, 11, True),
            (datetime(2026, 10, 20, 6, tzinfo=UTC), 33, 22, False),
        )
    )
    version = TrinoProvider(adapter).version('cat.sch.odd"name')
    assert version.token.startswith("22@1792472400000+1792479600000:")
    assert version.changed_at_ms == 1792479600000  # the metadata log entry at 07:00
    assert adapter.queries == [
        'select * from "cat"."sch"."odd""name$history"',
        'select * from "cat"."sch"."odd""name$metadata_log_entries"',
    ]


def test_trino_iceberg_rollback_changes_the_token():
    t1, t2 = datetime(2026, 10, 20, 3, tzinfo=UTC), datetime(2026, 10, 20, 4, tzinfo=UTC)
    first, second = 3848394183711388191, 6535619552822973486
    before = iceberg((t1, first, None, True), (t2, second, first, True))
    # a rollback adds no row: it clears is_current_ancestor on the rolled-back snapshot
    after = iceberg((t1, first, None, True), (t2, second, first, False))
    a = TrinoProvider(FakeAdapter(before)).version("c.s.t")
    b = TrinoProvider(FakeAdapter(after)).version("c.s.t")
    assert a.token.startswith(f"{second}@1792468800000+")
    assert b.token.startswith(f"{first}@1792465200000+")
    assert a.token != b.token


def test_trino_iceberg_reads_naive_times_as_utc_and_epoch_numbers():
    naive = TrinoProvider(FakeAdapter(iceberg((datetime(2026, 10, 20, 3), 11, None, True))))
    aware = TrinoProvider(
        FakeAdapter(iceberg((datetime(2026, 10, 20, 3, tzinfo=UTC), 11, 0, True)))
    )
    number = TrinoProvider(FakeAdapter(iceberg((1792465200000, 11, None, True))))
    versions = [p.version("c.s.t") for p in (naive, aware, number)]
    assert versions[0] == versions[1] == versions[2]
    assert versions[0].token.startswith("11@1792465200000+")


@pytest.mark.parametrize("order", [1, -1])
def test_trino_iceberg_tied_current_snapshots_give_unknown(order):
    t = datetime(2026, 10, 20, 3, tzinfo=UTC)
    rows = [(t, 11, None, True), (t, 22, 11, True)][::order]
    provider = TrinoProvider(FakeAdapter(iceberg(*rows)))
    assert provider.version("c.s.t") == UNKNOWN


def test_trino_iceberg_schema_change_moves_the_token():
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    before = (datetime(2026, 10, 20, 7, tzinfo=UTC), "local:///t/metadata/00001.metadata.json")
    after = (datetime(2026, 10, 20, 8, tzinfo=UTC), "local:///t/metadata/00002.metadata.json")
    # ALTER TABLE ADD COLUMN adds no snapshot, only a metadata log entry
    a = TrinoProvider(FakeAdapter(iceberg(row, log=[(*before, 11, 0, 1)]))).version("c.s.t")
    b = TrinoProvider(FakeAdapter(iceberg(row, log=[(*before, 11, 0, 1), (*after, 11, 1, 1)])))
    assert a.token != b.version("c.s.t").token


@pytest.mark.parametrize("order", [1, -1])
def test_trino_iceberg_tied_metadata_log_entries_give_unknown(order):
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    t = datetime(2026, 10, 20, 7, tzinfo=UTC)
    log = [(t, "local:///t/a.metadata.json", 11, 0, 1), (t, "local:///t/b.metadata.json", 11, 1, 1)]
    provider = TrinoProvider(FakeAdapter(iceberg(row, log=log[::order])))
    assert provider.version("c.s.t") == UNKNOWN


def test_trino_iceberg_changed_at_is_the_latest_of_snapshot_and_metadata_commit():
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    # LOG_ENTRY is at 07:00, after the snapshot at 03:00
    assert TrinoProvider(FakeAdapter(iceberg(row))).version("c.s.t").changed_at_ms == 1792479600000
    early = (datetime(2026, 10, 20, 2, tzinfo=UTC), "local:///t/a.metadata.json", 11, 0, 1)
    version = TrinoProvider(FakeAdapter(iceberg(row, log=[early]))).version("c.s.t")
    assert version.changed_at_ms == 1792465200000


def test_trino_iceberg_empty_metadata_log_gives_unknown():
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    assert TrinoProvider(FakeAdapter(iceberg(row, log=()))).version("c.s.t") == UNKNOWN


def test_trino_delta_connector_history():
    def delta(sql):
        return ["version", "timestamp", "user_id", "operation"], [
            (9, datetime(2026, 10, 20, 4, tzinfo=UTC), "u", "OPTIMIZE"),
            (8, datetime(2026, 10, 20, 3, tzinfo=UTC), "u", "MERGE"),
        ]

    # no operationMetrics column: a data operation counts as a change, OPTIMIZE does not
    version = TrinoProvider(FakeAdapter(delta)).version("c.s.t")
    assert version == DataVersion("8@1792465200000", 1792465200000)


@pytest.mark.parametrize(
    "answer",
    [
        lambda sql: (ICEBERG, []),
        lambda sql: (ICEBERG, [(1792465200000, 11, None, False)]),
        lambda sql: (["version", "timestamp", "operation"], []),
        lambda sql: (["something", "else"], [(1, 2)]),
        lambda sql: (ICEBERG, [("not a time", 1, None, True)]),
        lambda sql: (ICEBERG, [(None, 1, None, True)]),
        fail,
    ],
    ids=[
        "empty-iceberg",
        "no-current-snapshot",
        "empty-delta",
        "unknown-columns",
        "bad-time",
        "null-time",
        "query-error",
    ],
)
def test_trino_history_problems_give_unknown(answer):
    messages = []
    provider = TrinoProvider(FakeAdapter(answer), messages.append)
    assert provider.version("c.s.t") == UNKNOWN
    assert messages


def test_trino_view_version_hashes_the_definition():
    adapter = FakeAdapter(lambda sql: (["view_definition"], [("SELECT 1",)]))
    version = TrinoProvider(adapter).version("view:Cat.Sch.V")
    assert version.known
    assert adapter.queries == [
        'select view_definition from "Cat".information_schema.views '
        "where lower(table_schema) = 'Sch' and lower(table_name) = 'V'"
    ]
    changed = TrinoProvider(FakeAdapter(lambda sql: (["view_definition"], [("SELECT 2",)])))
    assert changed.version("view:Cat.Sch.V") != version
    missing = TrinoProvider(FakeAdapter(lambda sql: (["view_definition"], [])))
    assert missing.version("view:Cat.Sch.V") == UNKNOWN


def test_trino_clock_and_session_values():
    adapter = FakeAdapter(lambda sql: (["x"], [(1_760_000_000_000,)]))
    assert TrinoProvider(adapter).now_ms() == 1_760_000_000_000
    assert adapter.queries == ["select cast(to_unixtime(current_timestamp) * 1000 as bigint)"]
    values = FakeAdapter(lambda sql: (["x"], [("2026-10-20", "u", "cat", "sch", "UTC", "450")]))
    assert TrinoProvider(values).volatile_values() == {
        "date": "2026-10-20",
        "session": "u|cat|sch|UTC|450",
    }

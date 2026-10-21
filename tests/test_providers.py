import threading
from contextlib import contextmanager
from datetime import datetime

from dbt_hodor.metadata import UNKNOWN, DataVersion
from dbt_hodor.providers import DeltaProvider, DuckDBProvider, fetch, store_executor


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

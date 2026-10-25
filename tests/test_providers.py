import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from dbt_hodor.metadata import UNKNOWN, DataVersion
from dbt_hodor.providers import (
    DeltaProvider,
    DuckDBProvider,
    SnowflakeProvider,
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


def test_trino_iceberg_reads_naive_times_as_utc():
    naive = TrinoProvider(FakeAdapter(iceberg((datetime(2026, 10, 20, 3), 11, None, True))))
    aware = TrinoProvider(
        FakeAdapter(iceberg((datetime(2026, 10, 20, 3, tzinfo=UTC), 11, 0, True)))
    )
    assert naive.version("c.s.t") == aware.version("c.s.t")


def test_trino_iceberg_schema_change_moves_the_token():
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    before = (datetime(2026, 10, 20, 7, tzinfo=UTC), "local:///t/metadata/00001.metadata.json")
    after = (datetime(2026, 10, 20, 8, tzinfo=UTC), "local:///t/metadata/00002.metadata.json")
    # ALTER TABLE ADD COLUMN adds no snapshot, only a metadata log entry
    a = TrinoProvider(FakeAdapter(iceberg(row, log=[(*before, 11, 0, 1)]))).version("c.s.t")
    b = TrinoProvider(FakeAdapter(iceberg(row, log=[(*before, 11, 0, 1), (*after, 11, 1, 1)])))
    assert a.token != b.version("c.s.t").token


def test_trino_iceberg_changed_at_is_the_latest_of_snapshot_and_metadata_commit():
    row = (datetime(2026, 10, 20, 3, tzinfo=UTC), 11, None, True)
    # LOG_ENTRY is at 07:00, after the snapshot at 03:00
    assert TrinoProvider(FakeAdapter(iceberg(row))).version("c.s.t").changed_at_ms == 1792479600000
    early = (datetime(2026, 10, 20, 2, tzinfo=UTC), "local:///t/a.metadata.json", 11, 0, 1)
    version = TrinoProvider(FakeAdapter(iceberg(row, log=[early]))).version("c.s.t")
    assert version.changed_at_ms == 1792465200000


def test_trino_delta_connector_history():
    def delta(sql):
        return ["version", "timestamp", "user_id", "operation"], [
            (9, datetime(2026, 10, 20, 4, tzinfo=UTC), "u", "OPTIMIZE"),
            (8, datetime(2026, 10, 20, 3, tzinfo=UTC), "u", "MERGE"),
        ]

    # no operationMetrics column: a data operation counts as a change, OPTIMIZE does not
    version = TrinoProvider(FakeAdapter(delta)).version("c.s.t")
    assert version == DataVersion("8@1792465200000", 1792465200000)


T3 = datetime(2026, 10, 20, 3, tzinfo=UTC)
T7 = datetime(2026, 10, 20, 7, tzinfo=UTC)
TIED_FILES = [
    (T7, "local:///t/a.metadata.json", 11, 0, 1),
    (T7, "local:///t/b.metadata.json", 11, 1, 1),
]


@pytest.mark.parametrize(
    "answer",
    [
        lambda sql: (ICEBERG, []),
        lambda sql: (ICEBERG, [(1792465200000, 11, None, False)]),
        lambda sql: (["version", "timestamp", "operation"], []),
        lambda sql: (["something", "else"], [(1, 2)]),
        lambda sql: (ICEBERG, [("not a time", 1, None, True)]),
        lambda sql: (ICEBERG, [(None, 1, None, True)]),
        iceberg((1792465200000, 11, None, True)),
        iceberg((T3, 11, None, True), (T3, 22, 11, True)),
        iceberg((T3, 22, 11, True), (T3, 11, None, True)),
        iceberg((T3, 11, None, True), log=TIED_FILES),
        iceberg((T3, 11, None, True), log=TIED_FILES[::-1]),
        iceberg((T3, 11, None, True), log=()),
        fail,
    ],
    ids=[
        "empty-iceberg",
        "no-current-snapshot",
        "empty-delta",
        "unknown-columns",
        "bad-time",
        "null-time",
        "epoch-number-time",
        "tied-snapshots",
        "tied-snapshots-reversed",
        "tied-log-files",
        "tied-log-files-reversed",
        "empty-metadata-log",
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


def snowflake(info=("DB", "SCH", "T", 100, 200), commit=1792932189385000001, boom=False):
    """Answers the information_schema query, then the SYSTEM$ call."""

    def answer(sql):
        if "SYSTEM$LAST_CHANGE_COMMIT_TIME" in sql:
            if boom:
                raise RuntimeError("Table does not exist")
            return ["c"], [(commit,)]
        return ["a"], [info] if info else []

    return answer


def test_snowflake_token_joins_commit_created_and_last_ddl():
    adapter = FakeAdapter(snowflake())
    assert SnowflakeProvider(adapter).version("db.sch.t").token == "1792932189385000001|100|200"
    assert adapter.queries == [
        "select table_catalog, table_schema, table_name, "
        "date_part(epoch_nanosecond, created), date_part(epoch_nanosecond, last_ddl) "
        'from "DB".information_schema.tables '
        "where lower(table_schema) = 'sch' and lower(table_name) = 't' "
        "and table_type = 'BASE TABLE' "
        "and is_dynamic = 'NO' and is_hybrid = 'NO' and is_iceberg = 'NO'",
        'select SYSTEM$LAST_CHANGE_COMMIT_TIME(\'"DB"."SCH"."T"\')',
    ]


def test_snowflake_asks_for_the_stored_name_of_the_table():
    # a quoted lowercase table is found by lower() and then addressed by its stored name
    adapter = FakeAdapter(snowflake(info=("db", "sch", 'we"ird', 1, 2)))
    SnowflakeProvider(adapter).version('db.sch.we"ird')
    assert adapter.queries[1] == ('select SYSTEM$LAST_CHANGE_COMMIT_TIME(\'"db"."sch"."we""ird"\')')


def test_snowflake_commit_zero_is_a_known_token():
    # an empty table: 0 is a real value, and it differs from a table with rows
    empty = SnowflakeProvider(FakeAdapter(snowflake(commit=0))).version("db.sch.t")
    assert empty.token == "0|100|200"
    assert empty != SnowflakeProvider(FakeAdapter(snowflake())).version("db.sch.t")


def test_snowflake_each_part_moves_the_token():
    base = SnowflakeProvider(FakeAdapter(snowflake())).version("db.sch.t")
    created = SnowflakeProvider(FakeAdapter(snowflake(info=("DB", "SCH", "T", 101, 200))))
    ddl = SnowflakeProvider(FakeAdapter(snowflake(info=("DB", "SCH", "T", 100, 201))))
    commit = SnowflakeProvider(FakeAdapter(snowflake(commit=1792932189385000002)))
    for other in (created, ddl, commit):
        assert other.version("db.sch.t") != base


@pytest.mark.parametrize(
    "answer",
    [
        snowflake(info=None),
        snowflake(info=("DB", "SCH", "T", None, 200)),
        snowflake(info=("DB", "SCH", "T", 100, None)),
        snowflake(commit=None),
        snowflake(commit="abc"),
        snowflake(commit=1.5),
        snowflake(commit=True),
        snowflake(info=("DB", "SCH", "T", "x", 200)),
        snowflake(boom=True),
        lambda sql: (["a"], [("DB", "SCH", "T", 1, 2), ("db", "sch", "t", 1, 2)]),
    ],
    ids=[
        "missing-info-row",
        "null-created",
        "null-last-ddl",
        "null-commit",
        "text-commit",
        "float-commit",
        "bool-commit",
        "text-created",
        "system-function-error",
        "ambiguous-name",
    ],
)
def test_snowflake_problems_give_unknown(answer):
    messages = []
    provider = SnowflakeProvider(FakeAdapter(answer), messages.append)
    assert provider.version("db.sch.t") == UNKNOWN
    assert messages


def test_snowflake_view_version_hashes_the_definition():
    adapter = FakeAdapter(lambda sql: (["view_definition"], [("create view v as select 1",)]))
    version = SnowflakeProvider(adapter).version("view:db.sch.v")
    assert version.known
    assert adapter.queries == [
        'select view_definition from "DB".information_schema.views '
        "where lower(table_schema) = 'sch' and lower(table_name) = 'v'"
    ]
    other = FakeAdapter(lambda sql: (["view_definition"], [("create view v as select 2",)]))
    assert SnowflakeProvider(other).version("view:db.sch.v") != version
    missing = FakeAdapter(lambda sql: (["view_definition"], []))
    assert SnowflakeProvider(missing).version("view:db.sch.v") == UNKNOWN


def test_snowflake_clock_and_session_values():
    adapter = FakeAdapter(lambda sql: (["x"], [(1_760_000_000_000,)]))
    assert SnowflakeProvider(adapter).now_ms() == 1_760_000_000_000
    assert adapter.queries == ["select date_part(epoch_millisecond, current_timestamp())"]
    values = FakeAdapter(lambda sql: (["x"], [("2026-10-20", "u", "r", "w")]))
    assert SnowflakeProvider(values).volatile_values() == {
        "date": "2026-10-20",
        "session": "u|r|w",
    }


def test_snowflake_paths_are_unknown():
    assert SnowflakeProvider(FakeAdapter(snowflake())).path_version("@stage/f") == UNKNOWN


def test_snowflake_query_leaves_out_dynamic_hybrid_and_iceberg_tables():
    adapter = FakeAdapter(snowflake())
    SnowflakeProvider(adapter).version("db.sch.t")
    assert adapter.queries[0].endswith(
        "and table_type = 'BASE TABLE' "
        "and is_dynamic = 'NO' and is_hybrid = 'NO' and is_iceberg = 'NO'"
    )


BS = chr(92)  # a backslash, which Snowflake reads as an escape inside '...'


@pytest.mark.parametrize(
    ("name", "literal"),
    [
        ("o'brien", "'o''brien'"),
        (f"a{BS}b", f"'a{BS}{BS}b'"),
        (f"a{BS}'b", f"'a{BS}{BS}''b'"),
    ],
    ids=["quote", "backslash", "backslash-quote"],
)
def test_snowflake_literals_escape_backslash_and_quote(name, literal):
    adapter = FakeAdapter(snowflake(info=(name, name, name, 1, 2)))
    SnowflakeProvider(adapter).version(f"db.{name}.{name}")
    both = f"lower(table_schema) = {literal} and lower(table_name) = {literal}"
    assert both in adapter.queries[0]
    quoted = '"' + name.replace('"', '""') + '"'
    target = literal_of(f"{quoted}.{quoted}.{quoted}")
    assert adapter.queries[1] == f"select SYSTEM$LAST_CHANGE_COMMIT_TIME({target})"

    view = FakeAdapter(lambda sql: (["view_definition"], [("select 1",)]))
    SnowflakeProvider(view).version(f"view:db.{name}.{name}")
    assert both in view.queries[0]


def literal_of(text):
    return "'" + text.replace(BS, BS + BS).replace("'", "''") + "'"


def two_views(sql):
    return ["view_definition"], [("select 1",), ("select 2",)]


@pytest.mark.parametrize(
    "provider", [SnowflakeProvider, TrinoProvider, DeltaProvider, DuckDBProvider]
)
def test_two_views_matching_one_name_give_unknown(provider):
    # "v" and V differ only in case: hashing one of them could reuse on the wrong definition
    messages = []
    assert provider(FakeAdapter(two_views), messages.append).version("view:c.s.v") == UNKNOWN
    assert messages

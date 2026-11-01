import pytest
import sqlglot
from sqlglot import exp

from dbt_hodor.config import NodeStateConfig
from dbt_hodor.engine import BUILD, NodeFacts, decide
from dbt_hodor.records import COLUMN_NAMES, COLUMNS, TYPE_NAMES, Record
from dbt_hodor.store import StoreError
from dbt_hodor.store.delta import DeltaStore, quote_table, sql_literal, trino_literal
from dbt_hodor.store.trino import TrinoStore

duckdb = pytest.importorskip("duckdb")
from dbt_hodor.store.duckdb import DuckDBStore  # noqa: E402


@pytest.fixture
def store():
    s = DuckDBStore(":memory:")
    yield s
    s.close()


def test_duckdb_load_returns_latest_successful_build_per_node(store, record):
    store.append(
        [
            record(record_id="a", recorded_at_ms=100, built_at_ms=100),
            record(record_id="b", recorded_at_ms=200, built_at_ms=200),  # latest build
            record(record_id="c", recorded_at_ms=300, action="reuse", decision="reuse"),
            record(record_id="d", recorded_at_ms=400, status="error"),
            record(record_id="e", recorded_at_ms=500, target_name="dev"),
            record(record_id="f", recorded_at_ms=600, project_name="other"),
            record(record_id="g", recorded_at_ms=200, node_key="c.s.dim", status="warn"),
            record(record_id="h", recorded_at_ms=50, node_key="c.s.old"),
        ]
    )
    loaded = store.load("proj", "prod", since_ms=100)
    assert set(loaded) == {"c.s.fct", "c.s.dim"}
    assert loaded["c.s.fct"] == record(record_id="b", recorded_at_ms=200, built_at_ms=200)
    assert loaded["c.s.dim"].record_id == "g"


def test_duckdb_carried_copy_counts_as_the_build_but_not_as_an_invocation(store, record):
    store.append(
        [
            record(record_id="a", invocation_id="i1", recorded_at_ms=100, built_at_ms=100),
            record(record_id="b", invocation_id="i2", recorded_at_ms=900, action="reuse"),
            record(record_id="c", invocation_id="i1", recorded_at_ms=900, action="carry"),
        ]
    )
    assert store.load("proj", "prod", since_ms=500)["c.s.fct"].record_id == "c"
    assert [r.record_id for r in store.invocation("proj", "prod", None)] == ["b"]
    assert [r.record_id for r in store.invocation("proj", "prod", "i1")] == ["a"]


def test_duckdb_prune_counts_without_deleting_then_deletes(store, record):
    store.append(
        [
            record(record_id="a", recorded_at_ms=100),
            record(record_id="b", recorded_at_ms=200),
            record(record_id="c", recorded_at_ms=300),
            record(record_id="d", recorded_at_ms=100, target_name="dev"),  # another target
            record(record_id="e", recorded_at_ms=100, project_name="other"),  # another project
        ]
    )
    # a dry run reports the same number it would delete, and leaves everything in place
    assert store.prune("proj", "prod", before_ms=250, delete=False) == 2
    assert len(store.invocation("proj", "prod", "inv-1")) == 3
    assert store.prune("proj", "prod", before_ms=250, delete=True) == 2
    remaining = store.invocation("proj", "prod", "inv-1")
    assert [r.record_id for r in remaining] == ["c"]
    # other projects and targets are untouched
    assert store.prune("proj", "dev", before_ms=250, delete=False) == 1
    assert store.prune("other", "prod", before_ms=250, delete=False) == 1


def test_duckdb_prune_with_nothing_to_delete_is_a_no_op(store, record):
    store.append([record(record_id="a", recorded_at_ms=500)])
    assert store.prune("proj", "prod", before_ms=100, delete=True) == 0
    assert len(store.invocation("proj", "prod", "inv-1")) == 1


def test_duckdb_load_before_excludes_later_builds(store, record):
    store.append(
        [record(record_id="a", recorded_at_ms=100), record(record_id="b", recorded_at_ms=200)]
    )
    assert store.load("proj", "prod", 0, before_ms=200)["c.s.fct"].record_id == "a"
    assert store.load("proj", "prod", 0, before_ms=100) == {}


def test_duckdb_invocation_returns_one_invocation_in_record_order(store, record):
    store.append(
        [
            record(record_id="a", invocation_id="i1", recorded_at_ms=100),
            record(record_id="c", invocation_id="i2", recorded_at_ms=300, node_key="c.s.b"),
            record(record_id="b", invocation_id="i2", recorded_at_ms=200),
            record(record_id="d", invocation_id="i3", recorded_at_ms=400, target_name="dev"),
        ]
    )
    assert [r.record_id for r in store.invocation("proj", "prod", None)] == ["b", "c"]
    assert [r.record_id for r in store.invocation("proj", "prod", "i1")] == ["a"]
    assert store.invocation("proj", "prod", "nope") == []


def test_duckdb_read_only_store_writes_nothing(tmp_path, record):
    path = tmp_path / "state.duckdb"
    with pytest.raises(StoreError):
        DuckDBStore(str(path), read_only=True)  # no file yet: nothing to read, nothing created
    assert not path.exists()
    DuckDBStore(str(path)).close()
    reader = DuckDBStore(str(path), read_only=True)
    assert reader.invocation("proj", "prod", None) == []
    with pytest.raises(StoreError):
        reader.append([record()])
    reader.close()


def test_duckdb_ties_break_on_record_id(store, record):
    store.append([record(record_id="a"), record(record_id="b")])
    assert store.load("proj", "prod", 0)["c.s.fct"].record_id == "b"


def test_duckdb_file_store_creates_parent_and_persists(tmp_path, record):
    path = tmp_path / "nested" / "state.duckdb"
    first = DuckDBStore(str(path))
    first.append([record()])
    first.close()
    second = DuckDBStore(str(path))
    assert second.load("proj", "prod", 0) == {"c.s.fct": record()}
    second.close()


def test_duckdb_store_wraps_errors(store, record):
    store.close()
    with pytest.raises(StoreError):
        store.load("proj", "prod", 0)


def test_duckdb_custom_schema_and_table(tmp_path, record):
    s = DuckDBStore(str(tmp_path / "s.duckdb"), schema="ops", table="my_state")
    s.append([record()])
    assert s._con.execute("select count(*) from ops.my_state").fetchone()[0] == 1
    assert s.load("proj", "prod", 0) == {"c.s.fct": record()}
    s.close()
    with pytest.raises(StoreError):
        DuckDBStore(":memory:", schema='x"; drop table y; --')


def test_quote_table():
    assert quote_table("my-catalog", "ops", "hodor_state") == ("`my-catalog`.`ops`.`hodor_state`")
    for bad in ("t`; drop table x", "a.b", "", "-x", "a b"):
        with pytest.raises(StoreError):
            quote_table("c", "s", bad)


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        (42, "42"),
        ("plain", "'plain'"),
        ("O'Brien", "'O\\'Brien'"),
        ("back\\slash'", "'back\\\\slash\\''"),
    ],
)
def test_sql_literal(value, literal):
    assert sql_literal(value) == literal


def test_sql_literal_rejects_other_types():
    with pytest.raises(TypeError):
        sql_literal(1.5)


class FakeWarehouse:
    def __init__(self, rows=(), fail=False, exists=False, columns=COLUMN_NAMES):
        self.statements = []
        self.rows = rows
        self.fail = fail
        self.exists = exists
        self.columns = columns

    def __call__(self, sql):
        self.statements.append(sql)
        if self.fail:
            raise RuntimeError("warehouse down")
        if sql.startswith("DESCRIBE"):
            if not self.exists:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return [(name, "string") for name in self.columns]
        return self.rows if sql.startswith("SELECT") else []


STORES = [(DeltaStore, "`", "databricks"), (TrinoStore, '"', "trino")]


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_store_quotes_the_table_and_wraps_errors(store_class, quote, dialect):
    warehouse = FakeWarehouse()
    store_class("iceberg", "ops", "hodor_state", warehouse)
    table = ".".join(f"{quote}{p}{quote}" for p in ("iceberg", "ops", "hodor_state"))
    probe, create = warehouse.statements
    assert probe == f"DESCRIBE {table}"
    assert create.startswith(f"CREATE TABLE IF NOT EXISTS {table}")
    assert ("CLUSTER BY" in create) == (store_class is DeltaStore)
    if store_class is TrinoStore:
        assert "record_id varchar" in create
        assert "recorded_at_ms bigint" in create
    with pytest.raises(StoreError):
        store_class("c", "s", f"bad{quote}name", FakeWarehouse())
    with pytest.raises(StoreError):
        store_class("c", "s", "hodor_state", FakeWarehouse(fail=True))


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_store_inserts_in_batches_that_parse_back(store_class, quote, dialect, record):
    warehouse = FakeWarehouse()
    store = store_class("c", "s", "hodor_state", warehouse, batch_size=2)
    tricky = record(
        record_id="x", reason="it's \\ odd", test_result={"message": "a'b\nc\\d \u00e9"}
    )
    store.append([record(record_id="1"), record(record_id="2"), tricky])
    inserts = warehouse.statements[2:]
    assert len(inserts) == 2
    parsed = sqlglot.parse_one(inserts[1], read=dialect)
    values = [
        None if isinstance(v, exp.Null) else v.this if v.is_string else int(v.this)
        for v in parsed.find(exp.Tuple).expressions
    ]
    assert values == list(tricky.to_row())


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_store_reads_with_dialect_literals(store_class, quote, dialect, record):
    warehouse = FakeWarehouse(rows=[record().to_row()])
    store = store_class("c", "s", "hodor_state", warehouse, read_only=True)
    assert warehouse.statements == [f"DESCRIBE {store._table}"]  # read-only: no CREATE
    assert store.load("pro'j", "prod", 5) == {"c.s.fct": record()}
    select = warehouse.statements[-1]
    project = store_class._literal("pro'j")
    assert f"project_name = {project} AND target_name = 'prod' AND recorded_at_ms >= 5" in select
    assert sqlglot.parse_one(select, read=dialect).find(exp.Window) is not None
    assert store.invocation("proj", "prod", None) == [record()]
    assert (
        f"FROM {quote}c{quote}.{quote}s{quote}.{quote}hodor_state{quote}"
        in (warehouse.statements[-1])
    )


def test_delta_store_invocation_and_before(record):
    warehouse = FakeWarehouse(rows=[record().to_row()])
    store = DeltaStore("c", "s", "hodor_state", warehouse, read_only=True)
    assert warehouse.statements == ["DESCRIBE `c`.`s`.`hodor_state`"]  # read-only: no CREATE
    assert store.invocation("proj", "prod", "inv-1") == [record()]
    assert (
        "AND invocation_id = 'inv-1' ORDER BY recorded_at_ms, record_id" in warehouse.statements[-1]
    )
    store.invocation("proj", "prod", None)
    latest = sqlglot.parse_one(warehouse.statements[-1], read="databricks")
    assert latest.find(exp.Subquery) is not None  # the latest invocation, found in SQL
    store.load("proj", "prod", 5, before_ms=9)
    assert "recorded_at_ms >= 5 AND recorded_at_ms < 9" in warehouse.statements[-1]


def test_record_columns_match_store_schema(store):
    cols = [r[0] for r in store._con.execute("describe main.hodor_state").fetchall()]
    assert tuple(cols) == COLUMN_NAMES
    assert Record.__dataclass_fields__.keys() == set(COLUMN_NAMES)


def test_delta_store_skips_create_when_the_table_exists():
    # job principals often have MODIFY but no CREATE TABLE on the schema
    warehouse = FakeWarehouse(exists=True)
    DeltaStore("c", "s", "hodor_state", warehouse)
    assert warehouse.statements == ["DESCRIBE `c`.`s`.`hodor_state`"]


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (7, "7"),
        ("plain", "'plain'"),
        ("O'Brien", "'O''Brien'"),
        ("''", "''''''"),
        ("back\\slash", "'back\\slash'"),
        ("a\\'b", "'a\\''b'"),
        ("line1\nline2\r\n", "'line1\nline2\r\n'"),
        ("caf\u00e9 \u2603 \U0001f600", "'caf\u00e9 \u2603 \U0001f600'"),
    ],
)
def test_trino_literal(value, literal):
    assert trino_literal(value) == literal


def test_trino_literal_rejects_other_types():
    with pytest.raises(TypeError):
        trino_literal(1.5)


class InsertFails(FakeWarehouse):
    """Creating and reading work; every INSERT raises, like a commit conflict."""

    def __call__(self, sql):
        if sql.startswith("INSERT"):
            self.statements.append(sql)
            raise RuntimeError("commit conflict")
        return super().__call__(sql)


@pytest.mark.parametrize("store_class", [DeltaStore, TrinoStore])
def test_failed_insert_is_a_store_error_and_leaves_no_history(store_class, record):
    warehouse = InsertFails()
    store = store_class("c", "s", "hodor_state", warehouse, batch_size=2)
    with pytest.raises(StoreError, match="cannot write hodor state: commit conflict"):
        store.append([record(record_id="1"), record(record_id="2"), record(record_id="3")])
    assert sum(s.startswith("INSERT") for s in warehouse.statements) == 1  # stops at the first
    # nothing was stored, so the next run finds no record and builds the node
    last = store.load("proj", "prod", 0).get("c.s.fct")
    facts = NodeFacts(kind="table", code_hash="h", config_hash="c", hash_version="1")
    decision = decide(facts, last, NodeStateConfig(), 1_000_000)
    assert (decision.action, decision.reason) == (BUILD, "no_history")


def test_duckdb_load_latest_includes_reuse_records_for_selection(store, record):
    # a model's reuse record has status "reused", as dbt-core's RunStatus.Reused
    build = record(record_id="a", recorded_at_ms=100, node_hashes={"body": "old"})
    reuse = record(
        record_id="b",
        recorded_at_ms=200,
        action="reuse",
        status="reused",
        node_hashes={"body": "new"},
    )
    other = record(record_id="c", recorded_at_ms=300, node_key="c.s.x", status="error")
    store.append([build, reuse, other])
    latest = store.load_latest("proj", "prod", 0, reused=True)
    assert latest == {"c.s.fct": reuse}  # the failed record is not a successful one
    # the build fell out of the lookback window: the reuse alone still counts
    assert store.load_latest("proj", "prod", 150, reused=True) == {"c.s.fct": reuse}
    assert store.load_latest("proj", "prod", 201, reused=True) == {}
    assert store.load_latest("proj", "dev", 0, reused=True) == {}
    # defer and clone read builds, carries and clones only
    assert store.load_latest("proj", "prod", 0) == {"c.s.fct": build}


def test_load_still_returns_builds_only(store, record):
    build = record(record_id="a", recorded_at_ms=100)
    store.append([build, record(record_id="b", recorded_at_ms=200, action="reuse")])
    assert store.load("proj", "prod", 0) == {"c.s.fct": build}


OLD_COLUMNS = [name for name in COLUMN_NAMES if name != "node_hashes"]


def _old_table(path, rows=()):
    """A table as 0.3.1 made it: the current columns without node_hashes."""
    con = duckdb.connect(str(path))
    columns = ", ".join(f"{n} {TYPE_NAMES['duckdb'][k]}" for n, k in COLUMNS if n in OLD_COLUMNS)
    con.execute(f"CREATE TABLE hodor_state ({columns})")
    marks = ", ".join("?" for _ in OLD_COLUMNS)
    for row in rows:
        con.execute(f"INSERT INTO hodor_state VALUES ({marks})", list(row.to_row())[:-1])
    con.close()


def test_open_adds_node_hashes_to_an_old_table(tmp_path, record):
    path = tmp_path / "old.duckdb"
    _old_table(path)
    store = DuckDBStore(str(path))
    try:
        original = record(node_hashes={"engine": "v1"})
        store.append([original])
        assert store.load("proj", "prod", 0) == {"c.s.fct": original}
        assert store.load_latest("proj", "prod", 0) == {"c.s.fct": original}
    finally:
        store.close()
    store = DuckDBStore(str(path))  # a second open finds the column and changes nothing
    store.close()


def test_read_only_open_leaves_an_old_table_alone(tmp_path, record):
    path = tmp_path / "old.duckdb"
    _old_table(path, [record(action="reuse")])
    reader = DuckDBStore(str(path), read_only=True)
    try:
        assert reader.invocation("proj", "prod", None)[0].node_hashes is None
        assert reader.load_latest("proj", "prod", 0)["c.s.fct"].node_hashes is None
        assert reader.load("proj", "prod", 0) == {}  # a reuse record is no build
    finally:
        reader.close()
    con = duckdb.connect(str(path), read_only=True)
    names = [r[0] for r in con.execute("DESCRIBE hodor_state").fetchall()]
    con.close()
    assert "node_hashes" not in names


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_adds_node_hashes_to_an_old_table(store_class, quote, dialect):
    warehouse = FakeWarehouse(exists=True, columns=OLD_COLUMNS)
    store_class("c", "s", "hodor_state", warehouse)
    table = ".".join(f"{quote}{p}{quote}" for p in ("c", "s", "hodor_state"))
    assert warehouse.statements[0] == f"DESCRIBE {table}"
    assert warehouse.statements[1].startswith(f"ALTER TABLE {table} ADD COLUMN node_hashes ")
    assert len(warehouse.statements) == 2


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_read_only_does_not_alter_an_old_table(store_class, quote, dialect, record):
    warehouse = FakeWarehouse(rows=[record().to_row()], exists=True, columns=OLD_COLUMNS)
    store = store_class("c", "s", "hodor_state", warehouse, read_only=True)
    store.load("proj", "prod", 0)
    store.load_latest("proj", "prod", 0)
    store.invocation("proj", "prod", None)
    assert not any(s.startswith(("ALTER", "CREATE")) for s in warehouse.statements)
    assert all("NULL AS node_hashes" in s for s in warehouse.statements[1:])


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_wraps_a_failed_alter(store_class, quote, dialect):
    class AlterFails(FakeWarehouse):
        def __call__(self, sql):
            if sql.startswith("ALTER"):
                raise RuntimeError("permission denied")
            return super().__call__(sql)

    with pytest.raises(StoreError, match="node_hashes.*permission denied"):
        store_class("c", "s", "hodor_state", AlterFails(exists=True, columns=OLD_COLUMNS))


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_load_latest_reads_any_action(store_class, quote, dialect, record):
    warehouse = FakeWarehouse(rows=[record(node_hashes={"body": "b"}).to_row()])
    store = store_class("c", "s", "hodor_state", warehouse, read_only=True)
    assert store.load_latest("proj", "prod", 5)["c.s.fct"].node_hashes == {"body": "b"}
    select = warehouse.statements[-1]
    assert "action IN" not in select and "action =" not in select
    assert sqlglot.parse_one(select, read=dialect).find(exp.Window) is not None


def test_duckdb_load_latest_any_status_returns_a_failed_record(store, record):
    build = record(record_id="a", recorded_at_ms=100)
    failed = record(record_id="b", recorded_at_ms=200, status="error")
    store.append([build, failed])
    assert store.load_latest("proj", "prod", 0) == {"c.s.fct": build}
    assert store.load_latest("proj", "prod", 0, any_status=True) == {"c.s.fct": failed}


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_load_latest_any_status_drops_the_status_filter(
    store_class, quote, dialect, record
):
    warehouse = FakeWarehouse(rows=[record(status="error").to_row()])
    store = store_class("c", "s", "hodor_state", warehouse, read_only=True)
    store.load_latest("proj", "prod", 5)
    assert "status IN" in warehouse.statements[-1]
    assert store.load_latest("proj", "prod", 5, any_status=True)["c.s.fct"].status == "error"
    assert "status IN" not in warehouse.statements[-1]


def test_postgres_store_load_latest_any_status_drops_the_status_filter(record):
    from dbt_hodor.store.postgres import PostgresStore

    statements = []
    store = object.__new__(PostgresStore)
    store._table, store._has_hashes = '"s"."hodor_state"', True

    def run(sql, params):
        statements.append((sql, params))
        return [record(status="error").to_row()]

    store._run = run
    store.load_latest("proj", "prod", 5)
    assert "status IN" in statements[-1][0]
    assert store.load_latest("proj", "prod", 5, any_status=True)["c.s.fct"].status == "error"
    assert "status IN" not in statements[-1][0]
    assert statements[-1][1] == ["proj", "prod", 5]


@pytest.mark.parametrize(("store_class", "quote", "dialect"), STORES)
def test_warehouse_store_load_latest_for_selection_reads_reuse_records(
    store_class, quote, dialect, record
):
    warehouse = FakeWarehouse(rows=[record(action="reuse", status="reused").to_row()])
    store = store_class("c", "s", "hodor_state", warehouse, read_only=True)
    assert store.load_latest("proj", "prod", 5, reused=True)["c.s.fct"].status == "reused"
    assert "'reused')" in warehouse.statements[-1]
    store.load_latest("proj", "prod", 5)
    assert "'reused'" not in warehouse.statements[-1]


def test_postgres_store_load_latest_for_selection_reads_reuse_records(record):
    from dbt_hodor.store.postgres import PostgresStore

    statements = []
    store = object.__new__(PostgresStore)
    store._table, store._has_hashes = '"s"."hodor_state"', True

    def run(sql, params):
        statements.append((sql, params))
        return [record(action="reuse", status="reused").to_row()]

    store._run = run
    assert store.load_latest("proj", "prod", 5, reused=True)["c.s.fct"].status == "reused"
    assert "'reused')" in statements[-1][0]
    assert statements[-1][1] == ["proj", "prod", 5]

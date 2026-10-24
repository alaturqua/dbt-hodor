import pytest
import sqlglot
from sqlglot import exp

from dbt_hodor.records import COLUMN_NAMES, Record
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
    def __init__(self, rows=(), fail=False, exists=False):
        self.statements = []
        self.rows = rows
        self.fail = fail
        self.exists = exists

    def __call__(self, sql):
        self.statements.append(sql)
        if self.fail:
            raise RuntimeError("warehouse down")
        if sql.endswith("LIMIT 0") and not self.exists:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        return self.rows if sql.startswith("SELECT") else []


def test_delta_store_inserts_in_batches_that_parse_back(record):
    warehouse = FakeWarehouse()
    store = DeltaStore("c", "s", "hodor_state", warehouse, batch_size=2)
    tricky = record(record_id="x", reason="it's \\ odd", test_result={"message": "a'b\nc"})
    store.append([record(record_id="1"), record(record_id="2"), tricky])
    probe, create, *inserts = warehouse.statements
    assert probe == "SELECT * FROM `c`.`s`.`hodor_state` LIMIT 0"
    assert create.startswith("CREATE TABLE IF NOT EXISTS `c`.`s`.`hodor_state`")
    assert len(inserts) == 2
    parsed = sqlglot.parse_one(inserts[1], read="databricks")
    values = [
        None if isinstance(v, exp.Null) else v.this if v.is_string else int(v.this)
        for v in parsed.find(exp.Tuple).expressions
    ]
    assert values == list(tricky.to_row())


def test_delta_store_load_parses_rows(record):
    warehouse = FakeWarehouse(rows=[record().to_row()])
    store = DeltaStore("c", "s", "hodor_state", warehouse)
    assert store.load("proj", "prod", 5) == {"c.s.fct": record()}
    select = warehouse.statements[-1]
    assert "project_name = 'proj' AND target_name = 'prod' AND recorded_at_ms >= 5" in select


def test_delta_store_invocation_and_before(record):
    warehouse = FakeWarehouse(rows=[record().to_row()])
    store = DeltaStore("c", "s", "hodor_state", warehouse, read_only=True)
    assert warehouse.statements == []  # read-only: no probe, no CREATE
    assert store.invocation("proj", "prod", "inv-1") == [record()]
    assert (
        "AND invocation_id = 'inv-1' ORDER BY recorded_at_ms, record_id" in warehouse.statements[-1]
    )
    store.invocation("proj", "prod", None)
    latest = sqlglot.parse_one(warehouse.statements[-1], read="databricks")
    assert latest.find(exp.Subquery) is not None  # the latest invocation, found in SQL
    store.load("proj", "prod", 5, before_ms=9)
    assert "recorded_at_ms >= 5 AND recorded_at_ms < 9" in warehouse.statements[-1]


def test_delta_store_wraps_errors(record):
    with pytest.raises(StoreError):
        DeltaStore("c", "s", "hodor_state", FakeWarehouse(fail=True))
    with pytest.raises(StoreError):
        DeltaStore("c", "s", "bad`name", FakeWarehouse())


def test_record_columns_match_store_schema(store):
    cols = [r[0] for r in store._con.execute("describe main.hodor_state").fetchall()]
    assert tuple(cols) == COLUMN_NAMES
    assert Record.__dataclass_fields__.keys() == set(COLUMN_NAMES)


def test_delta_store_skips_create_when_the_table_exists():
    # job principals often have MODIFY but no CREATE TABLE on the schema
    warehouse = FakeWarehouse(exists=True)
    DeltaStore("c", "s", "hodor_state", warehouse)
    assert warehouse.statements == ["SELECT * FROM `c`.`s`.`hodor_state` LIMIT 0"]


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


def test_trino_store_quotes_with_double_quotes_and_creates_a_trino_table():
    warehouse = FakeWarehouse()
    TrinoStore("iceberg", "ops", "hodor_state", warehouse)
    probe, create = warehouse.statements
    assert probe == 'SELECT * FROM "iceberg"."ops"."hodor_state" LIMIT 0'
    assert create.startswith('CREATE TABLE IF NOT EXISTS "iceberg"."ops"."hodor_state"')
    assert "record_id varchar" in create
    assert "recorded_at_ms bigint" in create
    assert "CLUSTER BY" not in create
    with pytest.raises(StoreError):
        TrinoStore("c", "s", 'bad"name', FakeWarehouse())
    with pytest.raises(StoreError):
        TrinoStore("c", "s", "hodor_state", FakeWarehouse(fail=True))


def test_trino_store_inserts_parse_back_with_trino_literals(record):
    warehouse = FakeWarehouse()
    store = TrinoStore("c", "s", "hodor_state", warehouse, batch_size=2)
    tricky = record(
        record_id="x", reason="it's \\ odd", test_result={"message": "a'b\nc\\d \u00e9"}
    )
    store.append([record(record_id="1"), record(record_id="2"), tricky])
    inserts = warehouse.statements[2:]
    assert len(inserts) == 2
    parsed = sqlglot.parse_one(inserts[1], read="trino")
    values = [
        None if isinstance(v, exp.Null) else v.this if v.is_string else int(v.this)
        for v in parsed.find(exp.Tuple).expressions
    ]
    assert values == list(tricky.to_row())


def test_trino_store_reads_with_trino_literals(record):
    warehouse = FakeWarehouse(rows=[record().to_row()])
    store = TrinoStore("c", "s", "hodor_state", warehouse, read_only=True)
    assert store.load("pro'j", "prod", 5) == {"c.s.fct": record()}
    select = warehouse.statements[-1]
    assert "project_name = 'pro''j' AND target_name = 'prod'" in select
    assert sqlglot.parse_one(select, read="trino").find(exp.Window) is not None
    assert store.invocation("proj", "prod", None) == [record()]
    assert 'FROM "c"."s"."hodor_state"' in warehouse.statements[-1]


class InsertFails(FakeWarehouse):
    """Creating and reading work; every INSERT raises, like a commit conflict."""

    def __call__(self, sql):
        if sql.startswith("INSERT"):
            self.statements.append(sql)
            raise RuntimeError("commit conflict")
        return super().__call__(sql)


@pytest.mark.parametrize("store_class", [DeltaStore, TrinoStore])
def test_failed_insert_is_a_store_error_and_leaves_no_history(store_class, record):
    from dbt_hodor.config import NodeStateConfig
    from dbt_hodor.engine import BUILD, NodeFacts, decide

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

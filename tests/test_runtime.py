import dataclasses
import threading
from types import SimpleNamespace

import pytest

from dbt_hodor import providers, runtime
from dbt_hodor.runtime import Runtime, model_kind, seed_hash
from dbt_hodor.store.delta import DeltaStore
from dbt_hodor.store.trino import TrinoStore


def node(language="sql", **config):
    return SimpleNamespace(language=language, config=config)


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"materialized": "table"}, "table"),
        ({"materialized": "view"}, "view"),
        ({"materialized": "seed"}, "seed"),
        ({"materialized": "snapshot"}, "snapshot"),
        ({"materialized": "incremental"}, "incremental"),  # adapter default: merge
        ({"materialized": "incremental", "incremental_strategy": "append"}, "incremental"),
        ({"materialized": "incremental", "incremental_strategy": "merge"}, "incremental"),
        ({"materialized": "incremental", "incremental_strategy": "delete+insert"}, "incremental"),
        (
            {"materialized": "incremental", "incremental_strategy": "insert_overwrite"},
            "incremental",
        ),
        ({"materialized": "incremental", "incremental_strategy": "replace_where"}, "incremental"),
        ({"materialized": "incremental", "incremental_strategy": "microbatch"}, "unsupported"),
        ({"materialized": "incremental", "incremental_strategy": "my_custom"}, "unsupported"),
        ({"materialized": "materialized_view"}, "unsupported"),
        ({"materialized": "streaming_table"}, "unsupported"),
        ({"materialized": "metric_view"}, "unsupported"),
        ({"materialized": "dynamic_table"}, "unsupported"),
        ({"materialized": "ephemeral"}, "unsupported"),
    ],
)
def test_model_kind(config, expected):
    assert model_kind(node(**config)) == expected


def test_python_models_are_unsupported():
    assert model_kind(node(language="python", materialized="table")) == "unsupported"


def test_seed_hash_ignores_line_endings_and_sees_content(tmp_path):
    (tmp_path / "seeds").mkdir()
    csv = tmp_path / "seeds" / "countries.csv"
    seed = SimpleNamespace(root_path=str(tmp_path), original_file_path="seeds/countries.csv")
    csv.write_bytes(b"code,name\nde,Germany\n")
    lf = seed_hash(seed)
    csv.write_bytes(b"code,name\r\nde,Germany\r\n")
    assert seed_hash(seed) == lf
    csv.write_bytes(b"code,name\nde,Deutschland\n")
    assert seed_hash(seed) != lf


def _create_with_profile(monkeypatch, adapter_type, **profile):
    created = []
    stores = []

    def init(self, invocation_id, settings, config, adapter, manifest, store, provider, state):
        created.append(provider)

    monkeypatch.setattr(
        runtime, "_profile_settings", lambda config: {f"hodor_{k}": v for k, v in profile.items()}
    )
    monkeypatch.setattr(runtime, "get_flags", lambda: SimpleNamespace())
    monkeypatch.setattr(
        runtime, "open_store", lambda settings, *args, **kwargs: stores.append(settings.store)
    )
    monkeypatch.setattr(runtime.Runtime, "__init__", init)
    adapter = SimpleNamespace(type=lambda: adapter_type)
    result = runtime.Runtime.create(None, adapter, None, "inv")
    return result, created, stores


@pytest.mark.parametrize(
    ("adapter_type", "store", "expected"),
    [
        ("databricks", "delta", providers.DeltaProvider),
        ("databricks", "duckdb", providers.DeltaProvider),
        ("duckdb", "duckdb", providers.DuckDBProvider),
        ("trino", "duckdb", providers.TrinoProvider),
        ("trino", "trino", providers.TrinoProvider),
        ("duckdb", "delta", None),
        ("trino", "delta", None),
        ("databricks", "trino", None),
        ("duckdb", "trino", None),
        ("snowflake", "duckdb", providers.SnowflakeProvider),
        ("snowflake", "postgres", providers.SnowflakeProvider),
        ("snowflake", "delta", None),
        ("snowflake", "trino", None),
        ("bigquery", "duckdb", None),
    ],
)
def test_provider_class_for_adapter_and_store(adapter_type, store, expected):
    assert runtime.provider_class_for(adapter_type, store) is expected


def test_create_returns_none_before_opening_the_store_for_a_disallowed_pair(monkeypatch):
    def no_store(*args, **kwargs):
        raise AssertionError("open_store must not be called")

    warnings = []
    profile = {"hodor_store": "delta", "hodor_database": "c", "hodor_schema": "s"}
    monkeypatch.setattr(runtime, "_profile_settings", lambda config: profile)
    monkeypatch.setattr(runtime, "get_flags", lambda: SimpleNamespace())
    monkeypatch.setattr(runtime, "open_store", no_store)
    monkeypatch.setattr(runtime, "warn", warnings.append)
    adapter = SimpleNamespace(type=lambda: "trino")
    assert runtime.Runtime.create(None, adapter, None, "inv") is None
    assert len(warnings) == 1
    assert "store 'delta' needs the databricks adapter" in warnings[0]


def test_trino_store_on_trino_opens_the_trino_store(monkeypatch):
    result, created, stores = _create_with_profile(
        monkeypatch, "trino", store="trino", database="c", schema="s"
    )
    assert result is not None
    assert type(created[0]) is providers.TrinoProvider
    assert stores == ["trino"]


@pytest.mark.parametrize(("adapter_type", "store"), [("trino", "trino"), ("databricks", "delta")])
def test_unset_store_follows_the_adapter(monkeypatch, adapter_type, store):
    result, _, stores = _create_with_profile(monkeypatch, adapter_type, database="c", schema="s")
    assert result is not None
    assert stores == [store]


def test_explicit_store_wins_over_the_adapter_default(monkeypatch):
    result, _, stores = _create_with_profile(monkeypatch, "trino", store="duckdb", path=":memory:")
    assert result is not None
    assert stores == ["duckdb"]


def test_unset_store_on_duckdb_stays_off(monkeypatch):
    result, created, _ = _create_with_profile(monkeypatch, "duckdb", database="c", schema="s")
    assert result is None
    assert created == []


def _create(monkeypatch, flags):
    """Runtime.create with settings in mode on, flags as given, and a store that opens or raises."""
    opened = []

    def open_store(*args):
        opened.append(args)
        raise runtime.StoreError("stop here")  # fail open after the EMPTY check

    monkeypatch.setattr(runtime, "_profile_settings", lambda config: {})
    monkeypatch.setattr(
        runtime, "load_settings", lambda *a, **k: SimpleNamespace(mode="on", store="duckdb")
    )
    monkeypatch.setattr(runtime, "get_flags", lambda: flags)
    monkeypatch.setattr(runtime, "open_store", open_store)
    monkeypatch.setattr(runtime, "provider_class_for", lambda *a: lambda *b: SimpleNamespace())
    adapter = SimpleNamespace(type=lambda: "duckdb")
    return Runtime.create(SimpleNamespace(), adapter, None, "id"), opened


def test_empty_run_turns_hodor_off_before_any_store_or_provider(monkeypatch):
    monkeypatch.setattr(
        runtime, "provider_class_for", lambda *a: pytest.fail("provider must not be built")
    )
    monkeypatch.setattr(runtime, "open_store", lambda *a: pytest.fail("store must not be opened"))
    monkeypatch.setattr(runtime, "_profile_settings", lambda config: {})
    monkeypatch.setattr(
        runtime, "load_settings", lambda *a, **k: SimpleNamespace(mode="on", store="duckdb")
    )
    monkeypatch.setattr(runtime, "get_flags", lambda: SimpleNamespace(EMPTY=True))
    adapter = SimpleNamespace(type=lambda: "duckdb")
    assert Runtime.create(SimpleNamespace(), adapter, None, "id") is None


def test_flags_without_empty_still_proceed(monkeypatch):
    # dbt test has no EMPTY flag: create goes on to open the store
    created, opened = _create(monkeypatch, SimpleNamespace())
    assert created is None  # the stub store fails open
    assert len(opened) == 1


@pytest.mark.parametrize("store_class", [DeltaStore, TrinoStore])
def test_failed_store_write_warns_and_does_not_raise(monkeypatch, store_class, record):
    def execute(sql):
        if sql.startswith("INSERT"):
            raise RuntimeError("commit conflict")
        return []

    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)
    instance = object.__new__(runtime.Runtime)
    instance.store = store_class("c", "s", "hodor_state", execute)
    instance._lock = threading.Lock()
    instance._warned = set()
    instance._write([record(record_id="1"), record(record_id="2")])  # must not raise
    assert warnings == ["dbt-hodor: cannot write hodor state: commit conflict; 2 records lost"]


def _macro(sql, *calls):
    return SimpleNamespace(macro_sql=sql, depends_on=SimpleNamespace(macros=list(calls)))


MACROS = {"macro.p.outer": _macro("outer", "macro.p.inner"), "macro.p.inner": _macro("inner")}


def _model(**overrides):
    from dbt.artifacts.resources import ModelConfig

    base = {
        "unique_id": "model.p.a",
        "resource_type": "model",
        "relation_name": '"prod"."main"."A"',
        "checksum": SimpleNamespace(name="sha256", checksum="body-1"),
        "config": ModelConfig(materialized="table"),
        "unrendered_config": {"materialized": "table", "schema": "marts"},
        "contract": SimpleNamespace(enforced=False, checksum=None),
        "depends_on": SimpleNamespace(macros=["macro.p.outer"]),
        "description": "first",
        "columns": {},
        "fqn": ["p", "a"],
        "latest_version": None,
        "access": "protected",
        "deprecation_date": None,
    }
    return SimpleNamespace(**(base | overrides))


def _hashes(node, macros=MACROS):
    return runtime.v1_node_hashes(node, SimpleNamespace(macros=macros))


def test_v1_node_hashes_use_the_selection_keys_and_the_exact_relation():
    hashes = _hashes(_model())
    assert set(hashes) == {
        "engine",
        "relation",
        "body",
        "configs",
        "contract",
        "macros",
        "relation_hash",
        "persisted_descriptions",
    }
    assert hashes["engine"] == "v1"
    assert hashes["relation"] == '"prod"."main"."A"'
    assert hashes["body"] == "body-1"
    assert hashes["contract"] is None and hashes["persisted_descriptions"] is None


def test_v1_node_hashes_ignore_what_the_target_renders():
    # dbt compares unrendered configs: another target's schema and relation are no change
    from dbt.artifacts.resources import ModelConfig

    prod = _hashes(_model())
    dev = _hashes(
        _model(
            relation_name='"dev"."dbt_me_marts"."A"',
            config=ModelConfig(materialized="table", schema="dbt_me_marts"),
        )
    )
    assert {k: v for k, v in dev.items() if k != "relation"} == {
        k: v for k, v in prod.items() if k != "relation"
    }


def _with_macro(sql):
    return _hashes(_model(), MACROS | {"macro.p.inner": _macro(sql)})


def _unrendered(**config):
    return _hashes(_model(unrendered_config={"materialized": "table", "schema": "marts"} | config))


@pytest.mark.parametrize(
    ("changed", "keys"),
    [
        (lambda: _hashes(_model(checksum=SimpleNamespace(name="sha256", checksum="2"))), {"body"}),
        (lambda: _unrendered(materialized="view"), {"configs"}),
        # alias, schema and database are the relation, not the configs
        (lambda: _unrendered(schema="x"), {"relation_hash"}),
        (lambda: _unrendered(alias="b"), {"relation_hash"}),
        (lambda: _unrendered(tags="t"), set()),  # dbt leaves tags out of the comparison
        (
            lambda: _hashes(_model(contract=SimpleNamespace(enforced=True, checksum="k"))),
            {"contract"},
        ),
        (lambda: _with_macro("inner v2"), {"macros"}),  # a macro the node's macro calls
        # what dbt's same_fqn and same_ref_representation compare counts as configs
        (lambda: _hashes(_model(fqn=["p", "marts", "a"])), {"configs"}),
        (lambda: _hashes(_model(latest_version=2)), {"configs"}),
        (lambda: _hashes(_model(access="public")), {"configs"}),
        (lambda: _hashes(_model(deprecation_date="2027-01-01")), {"configs"}),
        (lambda: _hashes(_model(description="second")), set()),  # not persisted
    ],
)
def test_v1_node_hashes_change_only_the_compared_part(changed, keys):
    before = _hashes(_model())
    after = changed()
    assert {k for k in before if before[k] != after[k]} == keys


def test_v1_node_hashes_see_persisted_descriptions():
    from dbt.artifacts.resources import ModelConfig

    config = ModelConfig(materialized="table", persist_docs={"relation": True, "columns": True})
    before = _hashes(_model(config=config, columns={"id": SimpleNamespace(description="c")}))
    column = _hashes(_model(config=config, columns={"id": SimpleNamespace(description="d")}))
    relation = _hashes(
        _model(config=config, columns={"id": SimpleNamespace(description="c")}, description="x")
    )
    assert before["persisted_descriptions"] is not None
    assert column["persisted_descriptions"] != before["persisted_descriptions"]
    assert relation["persisted_descriptions"] != before["persisted_descriptions"]


def test_v1_node_hashes_of_a_test_compare_only_its_modifiers():
    from dbt.artifacts.resources import TestConfig

    def test_node(**unrendered):
        return _model(config=TestConfig(), unrendered_config=unrendered, relation_name=None)

    base = _hashes(test_node(severity="warn"))
    assert _hashes(test_node(severity="warn", meta={"a": 1}))["configs"] == base["configs"]
    assert _hashes(test_node(severity="error"))["configs"] != base["configs"]
    assert base["relation"] is None


def _bare_runtime():
    instance = object.__new__(runtime.Runtime)
    instance.invocation_id = "inv"
    instance._clock_offset_ms = 0
    instance.project_name, instance.target_name = "p", "prod"
    instance.settings = SimpleNamespace(mode="on", lookback_days=90)
    instance.manifest = SimpleNamespace(macros=MACROS)
    instance._lock = threading.Lock()
    instance._warned = set()
    instance._pending = []
    instance._view_codes = {}
    instance._parse_macros = {}
    return instance


def _plan(last=None):
    return runtime.Plan("prod.main.a", runtime.Decision("reuse", "up_to_date"), None, 0, last)


def test_records_carry_the_node_hashes():
    instance = _bare_runtime()
    node = _model()
    instance._record(node, _plan(), "reuse", "reused")
    assert instance._pending[0].node_hashes == _hashes(node)


def test_records_hash_the_macros_the_node_had_at_parse_time():
    # compiling a generic test adds macros to its depends_on; state selection hashes the
    # parsed node, so the record must too, or every unchanged test counts as modified
    instance = _bare_runtime()
    parsed = _model()
    instance._parse_macros = {"model.p.a": ("macro.p.outer",)}
    compiled = _model(depends_on=SimpleNamespace(macros=["macro.p.outer", "macro.p.extra"]))
    instance._record(compiled, _plan(), "reuse", "reused")
    assert instance._pending[0].node_hashes == _hashes(parsed)
    assert _hashes(compiled)["macros"] != _hashes(parsed)["macros"]


def test_a_carried_record_holds_the_current_node_hashes(record):
    instance = _bare_runtime()
    node = _model()
    plan = _plan(record(recorded_at_ms=0, node_hashes={"engine": "v1", "body": "old"}))
    instance._record(node, plan, "reuse", "reused")
    instance._carry(plan)
    carried = instance._pending[1]
    assert carried.action == "carry" and carried.node_hashes == _hashes(node)


def test_a_carried_record_never_keeps_stale_hashes(record):
    instance = _bare_runtime()
    instance.manifest = None  # hashing fails
    plan = _plan(record(recorded_at_ms=0, node_hashes={"engine": "v1", "body": "old"}))
    instance._record(_model(), plan, "reuse", "reused")
    instance._carry(plan)
    assert instance._pending[1].action == "carry" and instance._pending[1].node_hashes is None


def test_node_hashes_that_fail_leave_the_record_without_hashes():
    instance = _bare_runtime()
    instance.manifest = None  # the macros cannot be read
    instance._record(_model(), _plan(), "reuse", "reused")
    assert len(instance._pending) == 1  # the record is kept; selection counts it modified
    assert instance._pending[0].node_hashes is None


def _selection(monkeypatch, profile, store, b_config=None):
    from dbt.artifacts.resources import ModelConfig

    monkeypatch.setattr(runtime, "_profile_settings", lambda config: profile)
    monkeypatch.setattr(runtime, "open_store", store)
    monkeypatch.setattr(runtime, "get_adapter", lambda config: SimpleNamespace())
    config = SimpleNamespace(
        project_name="p", target_name="dev", credentials=SimpleNamespace(type="duckdb")
    )
    b = _model(unique_id="model.p.b", config=b_config or ModelConfig(materialized="table"))
    nodes = [("model.p.a", _model()), ("model.p.b", b)]
    return runtime.state_selection(config, SimpleNamespace(macros=MACROS), "modified", nodes)


@pytest.mark.parametrize("node_base", ["prod", None])
def test_state_selection_accepts_nodes_on_the_same_base_target(monkeypatch, node_base):
    from dbt.artifacts.resources import ModelConfig

    class Store:
        def load_latest(self, project, target, since, reused=False):
            return {}

        def close(self):
            pass

    state = {"defer_to_target": node_base} if node_base else {}
    b_config = ModelConfig.from_dict({"materialized": "table", "state": state})
    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    chosen = _selection(monkeypatch, profile, lambda *a, **k: Store(), b_config)
    assert chosen == {"model.p.a", "model.p.b"}


def test_state_selection_refuses_a_node_with_another_base_target(monkeypatch):
    from dbt.artifacts.resources import ModelConfig
    from dbt_common.exceptions import DbtRuntimeError

    def no_store(*args, **kwargs):
        raise AssertionError("the store is not read")

    b_config = ModelConfig.from_dict({"materialized": "table", "state": {"defer_to_target": "qa"}})
    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    with pytest.raises(DbtRuntimeError) as raised:
        _selection(monkeypatch, profile, no_store, b_config)
    assert raised.value.msg == (
        "dbt-hodor cannot compare with prod: node model.p.b sets defer_to_target qa; "
        "state selection uses one base target"
    )


def test_state_selection_without_a_base_target_leaves_it_to_dbt(monkeypatch):
    def no_store(*args, **kwargs):
        raise AssertionError("no store without a base target")

    assert _selection(monkeypatch, {"hodor_store": "duckdb"}, no_store) is None


def test_state_selection_compares_with_the_base_target_records(monkeypatch, record):
    loaded = []

    class Store:
        def load_latest(self, project, target, since, reused=False):
            loaded.append((project, target, reused))
            hashes = _hashes(_model())
            return {"prod.main.a": record(unique_id="model.p.a", node_hashes=hashes)}

        def close(self):
            loaded.append("closed")

    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    chosen = _selection(monkeypatch, profile, lambda *a, **k: Store())
    assert chosen == {"model.p.b"}  # a is unchanged in prod, b has no record there
    assert loaded == [("p", "prod", True), "closed"]  # reuse records count for selection


@pytest.mark.parametrize(
    ("profile", "cause"),
    [
        ({"hodor_store": "duckdb"}, "cannot read hodor state: locked"),
        ({"hodor_store": "duckdb", "hodor_mode": "off"}, "dbt-hodor is off"),
    ],
)
def test_state_selection_that_cannot_compare_fails_the_run(monkeypatch, profile, cause):
    from dbt_common.exceptions import DbtRuntimeError

    def broken(*args, **kwargs):
        raise runtime.StoreError("cannot read hodor state: locked")

    with pytest.raises(DbtRuntimeError) as raised:
        _selection(monkeypatch, profile | {"defer_to_target": "prod"}, broken)
    assert raised.value.msg == f"dbt-hodor cannot compare with prod: {cause}"


@dataclasses.dataclass
class _Node:
    unique_id: str
    database: str = "dev"
    schema: str = "main"
    name: str = "a"
    resource_type: str = "model"
    is_ephemeral: bool = False
    description: str = ""
    meta: dict = dataclasses.field(default_factory=dict)
    tags: list = dataclasses.field(default_factory=list)
    config: object = None
    defer_relation: object = None


def _defer(monkeypatch, profile, store, nodes, selected=()):
    from dbt.adapters.base.relation import BaseRelation

    monkeypatch.setattr(runtime, "_profile_settings", lambda config: profile)
    monkeypatch.setattr(runtime, "open_store", store)
    connections = SimpleNamespace(get_if_exists=lambda: None)
    adapter = SimpleNamespace(Relation=BaseRelation, connections=connections)
    monkeypatch.setattr(runtime, "get_adapter", lambda config: adapter)
    config = SimpleNamespace(
        project_name="p", target_name="dev", credentials=SimpleNamespace(type="duckdb"), quoting={}
    )
    manifest = SimpleNamespace(nodes=dict(nodes))
    return runtime.defer_to_base(config, manifest, selected), manifest.nodes


def _state(base):
    from dbt.artifacts.resources import ModelConfig

    return ModelConfig.from_dict({"materialized": "table", "state": {"defer_to_target": base}})


def test_defer_sets_the_base_relation_on_unselected_nodes(monkeypatch, record):
    from dbt.artifacts.resources import ModelConfig

    loaded = []
    relations = {
        "model.p.a": '"prod"."main"."A"',
        "model.p.b": '"prod"."main"."b"',
        "model.p.c": '"prod"."main"."c"',
        "model.p.d": '"prod"."main"."d',  # does not parse
        "model.p.f": '"prod"."main"."f"',
        "model.p.g": "prod.main.g",  # dbt would render it quoted
        "model.p.h": '"prod"."main"."h"',
    }

    class Store:
        def load_latest(self, project, target, since, any_status=False):
            loaded.append((project, target, any_status))
            out = {
                rel: record(unique_id=uid, node_key=rel, node_hashes={"relation": rel})
                for uid, rel in relations.items()
            }
            if any_status:  # h's newest build, under a new name, failed
                renamed = '"prod"."main"."h2"'
                out[renamed] = record(
                    record_id="r-2",
                    unique_id="model.p.h",
                    node_key=renamed,
                    status="error",
                    recorded_at_ms=2_000_000,
                    node_hashes={"relation": renamed},
                )
            return out

        def close(self):
            loaded.append("closed")

    plain = ModelConfig(materialized="table")
    nodes = {
        "model.p.a": _Node("model.p.a", config=plain),
        "model.p.b": _Node("model.p.b", config=plain),  # selected: dbt builds it
        "model.p.c": _Node("model.p.c", config=_state("qa")),  # another base target
        "model.p.d": _Node("model.p.d", config=plain),
        "model.p.e": _Node("model.p.e", config=_state("prod")),  # no prod build
        "model.p.f": _Node("model.p.f", config=plain, is_ephemeral=True),
        "model.p.g": _Node("model.p.g", config=plain),
        "model.p.h": _Node("model.p.h", config=plain),
    }
    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    done, out = _defer(monkeypatch, profile, lambda *a, **k: Store(), nodes, {"model.p.b"})
    assert done
    assert loaded == [("p", "prod", False), ("p", "prod", True), "closed"]
    deferred = out["model.p.a"].defer_relation
    assert (deferred.database, deferred.schema, deferred.alias) == ("prod", "main", "A")
    assert deferred.relation_name == '"prod"."main"."A"' and deferred.compiled_code is None
    assert [uid for uid, node in out.items() if node.defer_relation] == ["model.p.a"]


@pytest.mark.parametrize(
    "profile", [{"hodor_store": "duckdb"}, {"hodor_store": "duckdb", "defer_to_target": "dev"}]
)
def test_defer_without_another_base_target_leaves_it_to_dbt(monkeypatch, profile):
    def no_store(*args, **kwargs):
        raise AssertionError("the store is not read")

    done, _ = _defer(monkeypatch, profile, no_store, {"model.p.a": _Node("model.p.a")})
    assert done is False


@pytest.mark.parametrize(
    ("extra", "cause"),
    [({}, "cannot read hodor state: locked"), ({"hodor_mode": "off"}, "dbt-hodor is off")],
)
def test_defer_that_cannot_read_the_base_warns_and_leaves_it_to_dbt(monkeypatch, extra, cause):
    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)

    def broken(*args, **kwargs):
        raise runtime.StoreError("cannot read hodor state: locked")

    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"} | extra
    done, out = _defer(monkeypatch, profile, broken, {"model.p.a": _Node("model.p.a")})
    assert done is False
    assert out["model.p.a"].defer_relation is None
    assert warnings == [
        f"dbt-hodor could not defer to prod: {cause}; dbt fails the run without --state"
    ]


@pytest.mark.parametrize(
    ("relations", "message"),
    [
        ({}, "dbt-hodor found no records for prod to defer to"),
        (
            {"model.p.a": '"prod"."main"."a'},
            "dbt-hodor deferred none of 1 unselected nodes to prod",
        ),
    ],
)
def test_defer_that_defers_nothing_warns_once(monkeypatch, record, relations, message):
    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)

    class Store:
        def load_latest(self, project, target, since, any_status=False):
            return {
                rel: record(unique_id=uid, node_key=rel, node_hashes={"relation": rel})
                for uid, rel in relations.items()
            }

        def close(self):
            pass

    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    nodes = {"model.p.a": _Node("model.p.a")}
    done, _ = _defer(monkeypatch, profile, lambda *a, **k: Store(), nodes)
    assert done
    assert warnings == [message]


def test_defer_patch_fallback_names_the_target(monkeypatch):
    from dbt_hodor import patches

    warnings = []
    monkeypatch.setattr(patches, "warn", warnings.append)
    monkeypatch.setitem(patches.ORIGINALS, "GraphRunnableTask._get_deferred_manifest", lambda t: 7)
    profile = {"hodor_store": "duckdb", "defer_to_target": "prod"}
    monkeypatch.setattr(runtime, "_profile_settings", lambda config: profile)
    task = SimpleNamespace(
        args=SimpleNamespace(defer=True, state=None, defer_state=None),
        config=SimpleNamespace(target_name="dev", credentials=SimpleNamespace(type="duckdb")),
        manifest=None,
        job_queue=None,  # the selection is missing
    )
    assert patches.get_deferred_manifest(task) == 7
    assert patches.get_deferred_manifest(task) == 7  # decided once
    assert len(warnings) == 1
    assert warnings[0].startswith("dbt-hodor could not defer to prod: ")


class _NoStore:
    def __getattr__(self, name):
        raise AssertionError(f"the store was read: {name}")


def _clone_runtime(store, mode="on", base="base", allow=True):
    instance = _bare_runtime()
    instance.store = store
    instance.settings = SimpleNamespace(
        mode=mode, lookback_days=90, defer_to_target=base, allow_clones=allow, pre_clone=False
    )
    instance.relations = {}
    instance._bases = {}
    instance._base_lock = threading.Lock()
    return instance


def _build_plan():
    from dbt_hodor.config import NodeStateConfig
    from dbt_hodor.engine import NodeFacts

    facts = NodeFacts(kind="table", code_hash="c", config_hash="k", hash_version=1)
    plan = runtime.Plan(
        "prod.main.a", runtime.Decision("build", "no_history"), NodeStateConfig(), 0, None, facts
    )
    plan.analysis = runtime.SqlAnalysis(code_hash="c", select_star=False)
    return plan


@pytest.mark.parametrize(
    "mode, base, allow",
    [
        ("on", None, True),  # no base target
        ("on", "prod", True),  # the base target is this target
        ("on", "base", False),  # clones off
        ("shadow", "base", True),  # shadow mode never clones
    ],
)
def test_no_clone_check_reads_nothing(mode, base, allow):
    instance = _clone_runtime(_NoStore(), mode, base, allow)
    assert instance._clone_choice(_build_plan(), "model.p.a", "prod.main.a", lambda: True) is None


def test_base_records_that_cannot_be_read_clone_nothing_and_warn_once(monkeypatch):
    class Broken:
        calls = 0

        def load(self, *args):
            Broken.calls += 1
            raise RuntimeError("no store")

    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)
    instance = _clone_runtime(Broken())
    for _ in range(2):
        assert (
            instance._clone_choice(_build_plan(), "model.p.a", "prod.main.a", lambda: True) is None
        )
    assert Broken.calls == 1  # read once per run
    assert warnings == ["dbt-hodor: cannot read the records of base, nothing is cloned: no store"]


def _clone_ready(store=None, provider=None, kind="table"):
    """A runtime whose clone choice is made: (CLONED, base record, base relation)."""
    from conftest import make_record

    from dbt_hodor.clone import CLONED

    instance = _clone_runtime(store or _NoStore())
    instance.provider = provider or _NoStore()
    instance.decisions = {}
    instance.errors = 0
    plan = _build_plan()
    plan.facts = dataclasses.replace(plan.facts, kind=kind)
    base = make_record(data_version="7")
    instance._can_clone = lambda runner, model: True
    instance._clone_choice = lambda *args: (
        runtime.Decision("build", CLONED),
        base,
        '"db"."base"."a"',
    )
    return instance, plan, base


class _Adapter:
    """Records what the clone did; `fail_drop` makes drop_relation raise."""

    class Relation:
        quote_character = '"'

        @staticmethod
        def create(**parts):
            return SimpleNamespace(**parts)

    def __init__(self, fail_drop=False):
        self.calls = []
        self.fail_drop = fail_drop

    def get_relation(self, *parts):
        return SimpleNamespace(is_table=True, parts=parts)

    def drop_relation(self, relation):
        self.calls.append("drop")
        if self.fail_drop:
            raise RuntimeError("cannot drop")

    def cache_added(self, relation):
        self.calls.append("cache")

    def execute(self, sql, **kwargs):
        self.calls.append(sql)


def _runner(adapter):
    compiled = []
    compiler = SimpleNamespace(
        compile_node=lambda model, manifest, extra: compiled.append(1) or model
    )
    return SimpleNamespace(adapter=adapter, compiler=compiler, config=None), compiled


def _context():
    this = SimpleNamespace(incorporate=lambda **kw: "this")
    return {
        "this": this,
        "create_or_replace_clone": lambda this, base: "CLONE SQL",
        "should_revoke": lambda existing, full_refresh_mode: False,
        "apply_grants": lambda *args, **kwargs: None,
        "persist_docs": lambda *args: None,
    }


class _Reads:
    """Lists every attribute read; a clone check must read none."""

    def __init__(self):
        self.read = []

    def __getattr__(self, name):
        self.read.append(name)
        raise AttributeError(name)


def test_without_can_clone_table_no_store_or_metadata_is_read():
    store, provider = _Reads(), _Reads()
    instance = _clone_runtime(store)
    instance.provider = provider
    instance.decisions = {}
    instance._can_clone = lambda runner, model: False
    model = _model(unique_id="model.p.a")
    assert instance._try_clone(None, model, None, _build_plan(), "prod.main.a") is None
    assert store.read == provider.read == []


def _clone_model():
    return _model(database="prod", schema="main", alias="a")


class _Versions:
    """The base table's version: `first` when read before the clone, `then` after it."""

    def __init__(self, first, then):
        self.first, self.then = first, then

    def version(self, relation):
        return runtime.DataVersion(self.first)

    def refresh(self, relation):
        return runtime.DataVersion(self.then)


def test_a_base_table_written_during_the_clone_drops_the_clone_and_builds(monkeypatch):
    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)
    instance, plan, _ = _clone_ready(provider=_Versions("7", "8"))
    instance._clone_context = lambda runner, model: _context()
    adapter = _Adapter()
    runner, compiled = _runner(adapter)
    model = _clone_model()
    assert instance._try_clone(runner, model, None, plan, "prod.main.a") is model
    assert adapter.calls == ["cache", "CLONE SQL", "drop"]
    assert compiled == [1]  # compiled again for the build
    assert plan.decision == runtime.Decision("build", "no_history")  # builds as decided
    assert warnings and "changed during the clone" in warnings[0]


def test_a_table_whose_clone_cannot_be_dropped_builds(monkeypatch):
    warnings = []
    monkeypatch.setattr(runtime, "warn", warnings.append)
    instance, plan, _ = _clone_ready(provider=_Versions("7", "8"))
    instance._clone_context = lambda runner, model: _context()
    runner, _ = _runner(_Adapter(fail_drop=True))
    model = _clone_model()
    assert instance._try_clone(runner, model, None, plan, "prod.main.a") is model
    assert any("cannot drop" in w for w in warnings), warnings
    assert instance._pending == []  # the build writes the record


def test_an_incremental_whose_clone_cannot_be_dropped_fails_with_a_record(monkeypatch):
    monkeypatch.setattr(runtime, "warn", lambda msg: None)
    instance, plan, _ = _clone_ready(provider=_Versions("7", "8"), kind="incremental")
    instance._clone_context = lambda runner, model: _context()
    runner, _ = _runner(_Adapter(fail_drop=True))
    with pytest.raises(runtime.DbtRuntimeError, match="partial clone"):
        instance._try_clone(runner, _clone_model(), None, plan, "prod.main.a")
    (record,) = instance._pending
    assert (record.action, record.status) == ("build", "error")

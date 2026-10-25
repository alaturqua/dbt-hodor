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

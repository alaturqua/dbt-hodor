import threading
from types import SimpleNamespace

import pytest

from dbt_hodor import providers, runtime
from dbt_hodor.runtime import model_kind, seed_hash
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


def _create(monkeypatch, adapter_type, **profile):
    created = []
    stores = []

    def init(self, invocation_id, settings, config, adapter, manifest, store, provider, state):
        created.append(provider)

    monkeypatch.setattr(
        runtime, "_profile_settings", lambda config: {f"hodor_{k}": v for k, v in profile.items()}
    )
    monkeypatch.setattr(
        runtime, "open_store", lambda settings, *args, **kwargs: stores.append(settings.store)
    )
    monkeypatch.setattr(runtime.Runtime, "__init__", init)
    adapter = SimpleNamespace(type=lambda: adapter_type)
    result = runtime.Runtime.create(None, adapter, None, "inv")
    return result, created, stores


@pytest.mark.parametrize(
    ("adapter_type", "provider_class"),
    [
        ("databricks", providers.DeltaProvider),
        ("duckdb", providers.DuckDBProvider),
        ("trino", providers.TrinoProvider),
    ],
)
def test_each_supported_adapter_gets_its_provider(monkeypatch, adapter_type, provider_class):
    result, created, _ = _create(monkeypatch, adapter_type, store="duckdb", path=":memory:")
    assert result is not None
    assert type(created[0]) is provider_class


def test_unsupported_adapter_turns_hodor_off(monkeypatch):
    result, created, _ = _create(monkeypatch, "snowflake", store="duckdb", path=":memory:")
    assert result is None
    assert created == []


@pytest.mark.parametrize("adapter_type", ["duckdb", "trino"])
def test_delta_store_needs_the_databricks_adapter(monkeypatch, adapter_type):
    result, created, _ = _create(monkeypatch, adapter_type, store="delta", database="c", schema="s")
    assert result is None
    assert created == []


def test_delta_store_is_allowed_on_databricks(monkeypatch):
    result, created, _ = _create(monkeypatch, "databricks", store="delta", database="c", schema="s")
    assert result is not None
    assert type(created[0]) is providers.DeltaProvider


def test_trino_store_needs_the_trino_adapter(monkeypatch):
    for adapter_type in ("databricks", "duckdb"):
        result, created, _ = _create(
            monkeypatch, adapter_type, store="trino", database="c", schema="s"
        )
        assert result is None
        assert created == []
    result, created, stores = _create(monkeypatch, "trino", store="trino", database="c", schema="s")
    assert result is not None
    assert type(created[0]) is providers.TrinoProvider
    assert stores == ["trino"]


@pytest.mark.parametrize(("adapter_type", "store"), [("trino", "trino"), ("databricks", "delta")])
def test_unset_store_follows_the_adapter(monkeypatch, adapter_type, store):
    result, _, stores = _create(monkeypatch, adapter_type, database="c", schema="s")
    assert result is not None
    assert stores == [store]


def test_explicit_store_wins_over_the_adapter_default(monkeypatch):
    result, _, stores = _create(monkeypatch, "trino", store="duckdb", path=":memory:")
    assert result is not None
    assert stores == ["duckdb"]


def test_unset_store_on_duckdb_stays_off(monkeypatch):
    result, created, _ = _create(monkeypatch, "duckdb", database="c", schema="s")
    assert result is None
    assert created == []


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

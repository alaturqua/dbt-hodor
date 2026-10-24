from types import SimpleNamespace

import pytest

from dbt_hodor import providers, runtime
from dbt_hodor.runtime import model_kind, seed_hash


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

    def init(self, invocation_id, settings, config, adapter, manifest, store, provider, state):
        created.append(provider)

    monkeypatch.setattr(
        runtime, "_profile_settings", lambda config: {f"hodor_{k}": v for k, v in profile.items()}
    )
    monkeypatch.setattr(runtime, "open_store", lambda *args, **kwargs: object())
    monkeypatch.setattr(runtime.Runtime, "__init__", init)
    adapter = SimpleNamespace(type=lambda: adapter_type)
    result = runtime.Runtime.create(None, adapter, None, "inv")
    return result, created


@pytest.mark.parametrize(
    ("adapter_type", "provider_class"),
    [
        ("databricks", providers.DeltaProvider),
        ("duckdb", providers.DuckDBProvider),
        ("trino", providers.TrinoProvider),
    ],
)
def test_each_supported_adapter_gets_its_provider(monkeypatch, adapter_type, provider_class):
    result, created = _create(monkeypatch, adapter_type, store="duckdb", path=":memory:")
    assert result is not None
    assert type(created[0]) is provider_class


def test_unsupported_adapter_turns_hodor_off(monkeypatch):
    result, created = _create(monkeypatch, "snowflake", store="duckdb", path=":memory:")
    assert result is None
    assert created == []


@pytest.mark.parametrize("adapter_type", ["duckdb", "trino"])
def test_delta_store_needs_the_databricks_adapter(monkeypatch, adapter_type):
    result, created = _create(monkeypatch, adapter_type, store="delta", database="c", schema="s")
    assert result is None
    assert created == []


def test_delta_store_is_allowed_on_databricks(monkeypatch):
    result, created = _create(monkeypatch, "databricks", store="delta", database="c", schema="s")
    assert result is not None
    assert type(created[0]) is providers.DeltaProvider

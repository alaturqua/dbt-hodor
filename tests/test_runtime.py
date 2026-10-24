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


@pytest.mark.parametrize(
    ("adapter_type", "store", "expected"),
    [
        ("databricks", "delta", providers.DeltaProvider),
        ("databricks", "duckdb", providers.DeltaProvider),
        ("duckdb", "duckdb", providers.DuckDBProvider),
        ("trino", "duckdb", providers.TrinoProvider),
        ("duckdb", "delta", None),
        ("trino", "delta", None),
        ("snowflake", "duckdb", None),
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
    monkeypatch.setattr(runtime, "open_store", no_store)
    monkeypatch.setattr(runtime, "warn", warnings.append)
    adapter = SimpleNamespace(type=lambda: "trino")
    assert runtime.Runtime.create(None, adapter, None, "inv") is None
    assert len(warnings) == 1
    assert "store 'delta' needs the databricks adapter" in warnings[0]

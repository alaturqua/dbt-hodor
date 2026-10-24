from types import SimpleNamespace

import pytest

from dbt_hodor import runtime
from dbt_hodor.runtime import Runtime, model_kind, seed_hash


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


def _create(monkeypatch, flags):
    """Runtime.create with settings in mode on, flags as given, and a store that opens or raises."""
    opened = []

    def open_store(*args):
        opened.append(args)
        raise runtime.StoreError("stop here")  # fail open after the EMPTY check

    monkeypatch.setattr(runtime, "_profile_settings", lambda config: {})
    monkeypatch.setattr(runtime, "load_settings", lambda *a: SimpleNamespace(mode="on"))
    monkeypatch.setattr(runtime, "get_flags", lambda: flags)
    monkeypatch.setattr(runtime, "open_store", open_store)
    monkeypatch.setattr(runtime, "DuckDBProvider", lambda *a: SimpleNamespace())
    adapter = SimpleNamespace(type=lambda: "duckdb")
    return Runtime.create(SimpleNamespace(), adapter, None, "id"), opened


def test_empty_run_turns_hodor_off_before_any_store_or_provider(monkeypatch):
    monkeypatch.setattr(
        runtime, "DuckDBProvider", lambda *a: pytest.fail("provider must not be built")
    )
    monkeypatch.setattr(runtime, "open_store", lambda *a: pytest.fail("store must not be opened"))
    monkeypatch.setattr(runtime, "_profile_settings", lambda config: {})
    monkeypatch.setattr(runtime, "load_settings", lambda *a: SimpleNamespace(mode="on"))
    monkeypatch.setattr(runtime, "get_flags", lambda: SimpleNamespace(EMPTY=True))
    adapter = SimpleNamespace(type=lambda: "duckdb")
    assert Runtime.create(SimpleNamespace(), adapter, None, "id") is None


def test_flags_without_empty_still_proceed(monkeypatch):
    # dbt test has no EMPTY flag: create goes on to open the store
    created, opened = _create(monkeypatch, SimpleNamespace())
    assert created is None  # the stub store fails open
    assert len(opened) == 1

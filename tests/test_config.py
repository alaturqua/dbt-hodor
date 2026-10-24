import pytest

from dbt_hodor.config import (
    DEFAULT_LAG_TOLERANCE_S,
    NodeStateConfig,
    Settings,
    load_settings,
    node_state_config,
    parse_duration,
)


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("0s", 0),
        ("45m", 2700),
        ("45 minutes", 2700),
        ("4h", 14400),
        ("1d", 86400),
        ("2w", 1209600),
        ("1 Day", 86400),
        (3600, 3600),
        ({"count": 4, "period": "hour"}, 14400),
        ({"count": 1, "period": "day"}, 86400),
    ],
)
def test_parse_duration_accepts_documented_forms(value, seconds):
    assert parse_duration(value) == seconds


@pytest.mark.parametrize(
    "value",
    ["", "45", "4x", "-1h", -5, True, 1.5, None, {"count": 4}, {"count": "4", "period": "hour"}],
)
def test_parse_duration_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        parse_duration(value)


def test_node_state_config_defaults_when_missing():
    assert node_state_config(None, warn=pytest.fail) == NodeStateConfig()
    assert NodeStateConfig().lag_tolerance_s == DEFAULT_LAG_TOLERANCE_S == 2700


def test_node_state_config_reads_all_supported_keys():
    cfg = node_state_config(
        {
            "lag_tolerance": "4h",
            "require_fresh_data_from": "ALL",
            "compare_unrendered_code": "true",
            "execute_hooks_on_any_reuse": True,
            "evaluate_volatile_sql": "false",
        },
        warn=pytest.fail,
    )
    assert cfg == NodeStateConfig(
        lag_tolerance_s=14400,
        require_fresh_data_from="all",
        compare_unrendered_code=True,
        execute_hooks_on_reuse=True,
        evaluate_volatile_sql=False,
    )
    assert NodeStateConfig().evaluate_volatile_sql is None  # unset: the node kind decides


def test_node_state_config_accepts_dbt_core_hook_spelling():
    cfg = node_state_config({"execute_hooks_on_reuse": True}, warn=pytest.fail)
    assert cfg.execute_hooks_on_reuse is True


def test_node_state_config_warns_and_keeps_defaults_for_bad_values():
    warnings = []
    cfg = node_state_config(
        {
            "lag_tolerance": "soon",
            "require_fresh_data_from": "some",
            "pre_clone": "always",
            "colour": "blue",
        },
        warn=warnings.append,
    )
    assert cfg == NodeStateConfig()
    assert len(warnings) == 4
    assert any("pre_clone" in w and "not supported" in w for w in warnings)
    assert any("colour" in w for w in warnings)


def test_delta_without_schema_turns_off():
    warnings = []
    settings = load_settings({"catalog": "my_catalog"}, {}, warnings.append)
    assert settings.mode == "off"
    assert settings.store == "delta"
    assert settings.database == "my_catalog"
    assert warnings == ["store 'delta' needs hodor_schema; off for this run"]


def test_delta_without_database_or_schema_names_both():
    warnings = []
    assert load_settings({}, {}, warnings.append).mode == "off"
    assert "hodor_database and hodor_schema" in warnings[0]


def test_settings_from_profile_keys():
    settings = load_settings(
        {
            "catalog": "my_catalog",
            "hodor_mode": "on",
            "hodor_store": "delta",
            "hodor_schema": "ops",
            "hodor_metadata_threads": 16,
            "hodor_lookback_days": 30,
        },
        {},
        pytest.fail,
    )
    assert settings == Settings(
        mode="on",
        store="delta",
        database="my_catalog",
        schema="ops",
        table="hodor_state",
        metadata_threads=16,
        lookback_days=30,
    )


def test_database_schema_and_table_override_the_defaults():
    settings = load_settings(
        {
            "catalog": "my_catalog",
            "hodor_database": "ops-catalog",
            "hodor_schema": "state",
            "hodor_table": "runs",
        },
        {},
        pytest.fail,
    )
    assert (settings.database, settings.schema, settings.table) == ("ops-catalog", "state", "runs")


def test_target_database_key_is_the_fallback():
    settings = load_settings({"database": "pgdb", "hodor_schema": "s"}, {}, pytest.fail)
    assert settings.database == "pgdb"


def test_duckdb_store_defaults_schema_to_main():
    settings = load_settings({"hodor_store": "duckdb"}, {}, pytest.fail)
    assert (settings.mode, settings.schema, settings.table) == ("shadow", "main", "hodor_state")


def test_env_vars_override_profile_keys():
    settings = load_settings(
        {"hodor_mode": "on", "hodor_store": "delta", "hodor_database": "a", "hodor_schema": "b"},
        {
            "HODOR_MODE": "shadow",
            "HODOR_STORE": "duckdb",
            "HODOR_PATH": "tmp/state.duckdb",
            "HODOR_SCHEMA": "dev",
            "HODOR_TABLE": "t2",
        },
        pytest.fail,
    )
    assert settings.mode == "shadow"
    assert settings.store == "duckdb"
    assert settings.path == "tmp/state.duckdb"
    assert (settings.schema, settings.table) == ("dev", "t2")


@pytest.mark.parametrize(("value", "mode"), [(False, "off"), (True, "on")])
def test_yaml_booleans_for_on_and_off(value, mode):
    settings = load_settings({"hodor_store": "duckdb", "hodor_mode": value}, {}, pytest.fail)
    assert settings.mode == mode


def test_empty_env_var_does_not_override():
    settings = load_settings(
        {"hodor_mode": "on", "hodor_store": "duckdb"}, {"HODOR_MODE": ""}, pytest.fail
    )
    assert settings.mode == "on"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("hodor_mode", "loud"),
        ("hodor_store", "parquet"),
        ("hodor_metadata_threads", "0"),
        ("hodor_metadata_threads", "many"),
        ("hodor_lookback_days", "0"),
    ],
)
def test_invalid_settings_warn_and_use_defaults(key, value):
    warnings = []
    settings = load_settings({"hodor_store": "duckdb", key: value}, {}, warnings.append)
    assert warnings
    assert settings.mode in ("shadow", "off")
    assert settings.metadata_threads == 8
    assert settings.lookback_days == 90


@pytest.mark.parametrize("key", ["hodor_database", "hodor_schema"])
def test_invalid_name_parts_are_rejected(key):
    warnings = []
    raw = {"hodor_mode": "on", "hodor_database": "c", "hodor_schema": "s"}
    settings = load_settings({**raw, key: "x`; drop table y"}, {}, warnings.append)
    assert settings.mode == "off"
    assert any(f"invalid {key}" in w for w in warnings)


def test_invalid_table_falls_back_to_default():
    warnings = []
    settings = load_settings({"hodor_store": "duckdb", "hodor_table": "a.b"}, {}, warnings.append)
    assert settings.table == "hodor_state"
    assert any("invalid hodor_table" in w for w in warnings)


@pytest.mark.parametrize(
    ("adapter_type", "store"),
    [
        (None, "delta"),
        ("databricks", "delta"),
        ("duckdb", "delta"),  # kept: the runtime turns delta off on a non-databricks adapter
        ("trino", "trino"),
    ],
)
def test_unset_store_follows_the_adapter(adapter_type, store):
    profile = {"hodor_schema": "s", "database": "c"}
    settings = load_settings(profile, {}, pytest.fail, adapter_type)
    assert (settings.store, settings.database, settings.mode) == (store, "c", "shadow")


@pytest.mark.parametrize("adapter_type", ["databricks", "duckdb", "trino"])
def test_explicit_store_wins_over_the_adapter(adapter_type):
    settings = load_settings({"hodor_store": "duckdb"}, {}, pytest.fail, adapter_type)
    assert settings.store == "duckdb"
    profile = {"hodor_store": "delta", "hodor_schema": "s", "database": "c"}
    assert load_settings(profile, {}, pytest.fail, adapter_type).store == "delta"
    profile = {"hodor_schema": "s", "database": "c"}
    env = {"HODOR_STORE": "trino"}
    assert load_settings(profile, env, pytest.fail, adapter_type).store == "trino"


def test_invalid_store_falls_back_to_the_adapter_default():
    warnings = []
    profile = {"hodor_store": "parquet", "hodor_schema": "s", "database": "c"}
    settings = load_settings(profile, {}, warnings.append, "trino")
    assert settings.store == "trino"
    assert warnings == ["invalid hodor_store 'parquet'; using 'trino'"]


def test_trino_store_uses_the_target_catalog_and_needs_a_schema():
    warnings = []
    settings = load_settings({"database": "iceberg"}, {}, warnings.append, "trino")
    assert (settings.store, settings.database, settings.mode) == ("trino", "iceberg", "off")
    assert warnings == ["store 'trino' needs hodor_schema; off for this run"]
    settings = load_settings({"hodor_schema": "ops"}, {}, warnings.append, "trino")
    assert settings.mode == "off"
    assert warnings[-1] == "store 'trino' needs hodor_database; off for this run"
    profile = {"database": "iceberg", "hodor_schema": "ops", "hodor_database": "state"}
    settings = load_settings(profile, {}, pytest.fail, "trino")
    assert (settings.database, settings.schema) == ("state", "ops")

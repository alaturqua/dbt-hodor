"""Node-level `state:` configs and hodor settings."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass

DEFAULT_LAG_TOLERANCE_S = 45 * 60

_UNIT_SECONDS = {
    "s": 1,
    "second": 1,
    "seconds": 1,
    "m": 60,
    "minute": 60,
    "minutes": 60,
    "h": 3600,
    "hour": 3600,
    "hours": 3600,
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "w": 604800,
    "week": 604800,
    "weeks": 604800,
}
_DURATION_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$")

IGNORED_STATE_KEYS = frozenset({"pre_clone", "allow_clones", "defer_to_target"})
_HOOK_KEYS = ("execute_hooks_on_any_reuse", "execute_hooks_on_reuse")

MODES = ("off", "shadow", "on")
STORES = ("delta", "duckdb", "postgres", "trino")
WAREHOUSE_STORES = ("delta", "trino")  # stores in the target's own catalog, read through dbt
# The store when hodor_store is unset, by adapter type. Other adapters get delta.
_ADAPTER_STORES = {"trino": "trino"}
_STORE_ALIASES = {"lakebase": "postgres"}  # Lakebase is Postgres; the store code is the same
# One name part (database, schema or table). Stores always quote it.
IDENT_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
# A Postgres host name, or a Lakebase endpoint resource path.
_HOST_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_ENDPOINT_RE = re.compile(r"^projects/[\w-]+/branches/[\w-]+/endpoints/[\w-]+$")
_SETTING_NAMES = (
    "mode",
    "store",
    "database",
    "schema",
    "table",
    "path",
    "metadata_threads",
    "lookback_days",
    "host",
    "port",
    "user",
    "password",
    "sslmode",
    "endpoint",
)

Warn = Callable[[str], None]


def parse_duration(value: object) -> int:
    """Return seconds for '45m', 3600 or {'count': 4, 'period': 'hour'}."""
    if isinstance(value, bool):
        raise ValueError(f"invalid duration: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"invalid duration: {value!r}")
        return value
    if isinstance(value, str):
        match = _DURATION_RE.match(value)
        if match and match.group(2).lower() in _UNIT_SECONDS:
            return int(match.group(1)) * _UNIT_SECONDS[match.group(2).lower()]
        raise ValueError(f"invalid duration: {value!r}")
    if isinstance(value, Mapping) and set(value) == {"count", "period"}:
        count, period = value["count"], value["period"]
        if (
            isinstance(count, int)
            and not isinstance(count, bool)
            and count >= 0
            and isinstance(period, str)
            and period.lower() in _UNIT_SECONDS
        ):
            return count * _UNIT_SECONDS[period.lower()]
    raise ValueError(f"invalid duration: {value!r}")


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"invalid boolean: {value!r}")


@dataclass(frozen=True)
class NodeStateConfig:
    lag_tolerance_s: int = DEFAULT_LAG_TOLERANCE_S
    require_fresh_data_from: str = "any"
    compare_unrendered_code: bool = False
    execute_hooks_on_reuse: bool = False
    # None (auto): volatile values count unless only in stamp columns. True: all. False: none.
    evaluate_volatile_sql: bool | None = None


def node_state_config(raw: Mapping[str, object] | None, warn: Warn) -> NodeStateConfig:
    """Read a node's `state:` config. Invalid values warn and keep the default."""
    if not raw:
        return NodeStateConfig()
    values: dict[str, object] = {}
    for key, value in raw.items():
        try:
            if key == "lag_tolerance":
                values["lag_tolerance_s"] = parse_duration(value)
            elif key == "require_fresh_data_from":
                if not isinstance(value, str) or value.lower() not in ("any", "all"):
                    raise ValueError(f"invalid value {value!r}, expected 'any' or 'all'")
                values["require_fresh_data_from"] = value.lower()
            elif key == "compare_unrendered_code":
                values["compare_unrendered_code"] = _parse_bool(value)
            elif key in _HOOK_KEYS:
                values["execute_hooks_on_reuse"] = _parse_bool(value)
            elif key == "evaluate_volatile_sql":
                values["evaluate_volatile_sql"] = _parse_bool(value)
            elif key in IGNORED_STATE_KEYS:
                warn(f"state.{key} is not supported in v1 and is ignored")
            else:
                warn(f"unknown state config key {key!r} is ignored")
        except ValueError as exc:
            warn(f"state.{key}: {exc}; using the default")
    return NodeStateConfig(**values)  # type: ignore[arg-type]


@dataclass(frozen=True)
class Settings:
    mode: str = "shadow"
    store: str = "delta"
    database: str | None = None  # Databricks catalog or Postgres database; unused by duckdb
    schema: str | None = None
    table: str = "hodor_state"
    path: str = ".hodor/state.duckdb"  # duckdb store file
    metadata_threads: int = 8
    lookback_days: int = 90
    # postgres store. endpoint is a Lakebase endpoint path: it supplies host, user and the
    # OAuth token, so host, user and password stay empty for Lakebase.
    host: str | None = None
    port: int = 5432
    user: str | None = None
    password: str | None = None
    sslmode: str = "require"
    endpoint: str | None = None


def _int_setting(
    raw: Mapping[str, object], name: str, default: int, low: int, high: int, warn: Warn
) -> int:
    if name not in raw:
        return default
    try:
        value = int(str(raw[name]))
    except ValueError:
        value = low - 1
    if low <= value <= high:
        return value
    warn(f"invalid hodor_{name} {raw[name]!r}; expected {low}-{high}, using {default}")
    return default


def _ident_setting(raw: Mapping[str, object], name: str, warn: Warn) -> str | None:
    if name not in raw:
        return None
    value = str(raw[name])
    if IDENT_RE.match(value):
        return value
    warn(f"invalid hodor_{name} {value!r}; expected letters, digits, '_' or '-'")
    return None


def _pattern_setting(
    raw: Mapping[str, object], name: str, pattern: re.Pattern[str], expected: str, warn: Warn
) -> str | None:
    if name not in raw:
        return None
    value = str(raw[name])
    if pattern.match(value):
        return value
    warn(f"invalid hodor_{name} {value!r}; expected {expected}")
    return None


def load_settings(
    profile_target: Mapping[str, object],
    environ: Mapping[str, str],
    warn: Warn,
    *,
    adapter_type: str | None = None,
) -> Settings:
    """Read `hodor_<name>` settings (profile keys, overlaid with `--vars` by the caller);
    `HODOR_<NAME>` env vars override them.

    hodor_store defaults by adapter type: trino gets trino, any other adapter delta.
    hodor_database defaults to the target's own catalog (Databricks, Trino) or database.
    """
    raw: dict[str, object] = {}
    for name in _SETTING_NAMES:
        env_value = environ.get(f"HODOR_{name.upper()}")
        value = env_value if env_value else profile_target.get(f"hodor_{name}")
        if value is not None:
            raw[name] = value

    defaults = Settings()
    mode_value = raw.get("mode", defaults.mode)
    if isinstance(mode_value, bool):  # YAML 1.1 reads unquoted on/off as booleans
        mode_value = "on" if mode_value else "off"
    mode = str(mode_value).lower()
    if mode not in MODES:
        warn(f"invalid hodor_mode {mode!r}; using {defaults.mode!r}")
        mode = defaults.mode
    default_store = _ADAPTER_STORES.get(adapter_type or "", defaults.store)
    store = str(raw.get("store", default_store)).lower()
    store = _STORE_ALIASES.get(store, store)
    if store not in STORES:
        warn(f"invalid hodor_store {store!r}; using {default_store!r}")
        store = default_store
    target_database = profile_target.get("catalog") or profile_target.get("database")
    # Only delta and trino live in the target's own catalog. A Postgres database is unrelated.
    if store in WAREHOUSE_STORES and "database" not in raw and target_database:
        raw["database"] = target_database
    database = _ident_setting(raw, "database", warn)
    schema = _ident_setting(raw, "schema", warn)
    table = _ident_setting(raw, "table", warn) or defaults.table
    host = _pattern_setting(raw, "host", _HOST_RE, "a host name", warn)
    endpoint = _pattern_setting(
        raw, "endpoint", _ENDPOINT_RE, "projects/*/branches/*/endpoints/*", warn
    )
    if store == "duckdb":
        schema = schema or "main"
    elif mode != "off" and (database is None or schema is None):
        # No default schema: hodor writes nothing to a catalog until someone names one.
        missing = " and ".join(
            k for k, v in (("hodor_database", database), ("hodor_schema", schema)) if v is None
        )
        warn(f"store {store!r} needs {missing}; off for this run")
        mode = "off"
    elif store == "postgres" and mode != "off" and not (endpoint or host):
        warn("store 'postgres' needs hodor_endpoint (Lakebase) or hodor_host; off for this run")
        mode = "off"
    return Settings(
        mode=mode,
        store=store,
        database=database,
        schema=schema,
        table=table,
        path=str(raw.get("path", defaults.path)),
        metadata_threads=_int_setting(
            raw, "metadata_threads", defaults.metadata_threads, 1, 64, warn
        ),
        lookback_days=_int_setting(raw, "lookback_days", defaults.lookback_days, 1, 3650, warn),
        host=host,
        port=_int_setting(raw, "port", defaults.port, 1, 65535, warn),
        user=str(raw["user"]) if raw.get("user") else None,
        password=str(raw["password"]) if raw.get("password") else None,
        sslmode=str(raw.get("sslmode", defaults.sslmode)),
        endpoint=endpoint,
    )

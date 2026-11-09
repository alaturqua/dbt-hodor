"""What hodor reads from the running dbt session: its log, the hodor settings of the active
profile target and the state store they name."""

from __future__ import annotations

from typing import Any

from dbt.flags import get_flags
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

from dbt_hodor.config import WAREHOUSE_STORES, Settings
from dbt_hodor.providers import store_executor
from dbt_hodor.store import StateStore, open_local_store


def info(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.INFO)


def warn(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.WARN)


def debug(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.DEBUG)


_BASE_TARGET_KEYS = ("defer_to_target", "allow_clones", "pre_clone")


def _profile_settings(config: Any) -> dict[str, object]:
    """hodor_* keys and the base target keys of the active target. dbt drops unknown keys
    from credentials, so read profiles.yml again and render the keys the way dbt renders
    the profile."""
    from dbt.config.profile import read_profile
    from dbt.config.renderer import ProfileRenderer

    raw = read_profile(str(get_flags().PROFILES_DIR))
    outputs = (raw.get(config.profile_name) or {}).get("outputs") or {}
    target = outputs.get(config.target_name) or {}
    keys = {
        k: v
        for k, v in target.items()
        if isinstance(k, str) and (k.startswith("hodor_") or k in _BASE_TARGET_KEYS)
    }
    rendered: dict[str, object] = ProfileRenderer(config.cli_vars).render_data(keys)
    # `--vars '{hodor_mode: off}'` overrides the profile for one invocation (env vars still win)
    rendered.update(
        {k: v for k, v in (config.cli_vars or {}).items() if str(k).startswith("hodor_")}
    )
    rendered["database"] = config.credentials.database
    return rendered


def open_store(
    settings: Settings, config: Any, adapter: Any, read_only: bool = False
) -> StateStore:
    if settings.store not in WAREHOUSE_STORES:
        return open_local_store(settings, config.project_root, read_only)
    from dbt_hodor.store.delta import DeltaStore
    from dbt_hodor.store.trino import TrinoStore

    assert settings.database and settings.schema  # load_settings turns hodor off otherwise
    store_class = TrinoStore if settings.store == "trino" else DeltaStore
    return store_class(
        settings.database,
        settings.schema,
        settings.table,
        store_executor(adapter),
        read_only=read_only,
    )

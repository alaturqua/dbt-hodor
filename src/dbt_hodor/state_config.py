# Copyright 2026 Fivetran, Inc.
# Copyright 2026 The dbt-hodor Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Derived from dbt-state clients/dbt_state/src/dbt_state/plugin.py (commit b0f3030).
# This module holds the only upstream-derived logic in dbt-hodor: teaching dbt to merge the
# `state:` config key instead of replacing it. Modified for dbt-hodor: guarded so it installs
# once, and the merge keeps the outer level's keys.
"""Make dbt merge the `state:` config key across project, folder and model levels.

dbt merges only the keys listed in `BaseConfig.mergebehavior["update"]`, which is a fixed
list with no registration hook, so `state` has to be appended to it and `update_from`
wrapped.
"""

from __future__ import annotations


def install_state_config_support() -> None:
    """Merge the `state:` config key by key across project, folder and model levels."""
    from dbt_common.contracts.config.base import BaseConfig

    update_keys = BaseConfig.mergebehavior["update"]
    if "state" not in update_keys:
        update_keys.append("state")

    if getattr(BaseConfig, "_hodor_state_patch_installed", False):
        return

    original_update_from = BaseConfig.update_from

    def patched_update_from(self, data, config_cls, validate=True):  # type: ignore[no-untyped-def]
        if "state" not in data:
            return original_update_from(self, data, config_cls, validate)
        state = data.pop("state")
        # The original call does not merge "state"; merge it here and put it back.
        updated = original_update_from(self, data, config_cls, validate)
        old_state = self.get("state")
        if isinstance(old_state, dict) and isinstance(state, dict):
            updated["state"] = {**old_state, **state}
        else:
            updated["state"] = state
        return updated

    BaseConfig.update_from = patched_update_from  # type: ignore[method-assign]
    BaseConfig._hodor_state_patch_installed = True  # type: ignore[attr-defined]

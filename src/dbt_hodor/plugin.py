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
# Modified for dbt-hodor: no gRPC, auth, telemetry, git selector, deferral or clone
# patches; only run, build, test, seed and snapshot are patched; hodor settings and
# dbt_state guard.
"""dbt plugin entry point: install hodor's patches for run, build, test, seed and snapshot."""

from __future__ import annotations

import importlib.util
import logging
import os
from collections.abc import Callable
from typing import Any

from dbt.flags import get_flags
from dbt.plugins import dbtPlugin
from dbt.task.run import ModelRunner
from dbt.task.runnable import GraphRunnableTask
from dbt.task.seed import SeedRunner
from dbt.task.snapshot import SnapshotRunner
from dbt.task.test import TestRunner

# Captured at import, so a second plugin registration (dbtRunner in tests) never wraps
# hodor's own patches as the "originals".
ORIGINALS: dict[str, Callable[..., Any]] = {
    "ModelRunner.execute": ModelRunner.execute,
    "ModelRunner.print_result_line": ModelRunner.print_result_line,
    "SeedRunner.print_result_line": SeedRunner.print_result_line,
    "SnapshotRunner.print_result_line": SnapshotRunner.print_result_line,
    "TestRunner.execute": TestRunner.execute,
    "TestRunner.print_result_line": TestRunner.print_result_line,
    "GraphRunnableTask.execute_with_hooks": GraphRunnableTask.execute_with_hooks,
}
ENABLED_FOR_COMMANDS = ("run", "build", "test", "seed", "snapshot")


def install_patches() -> None:
    from dbt_hodor import patches

    ModelRunner.execute = patches.model_execute  # type: ignore[method-assign]
    ModelRunner.print_result_line = patches.model_print_result_line  # type: ignore[method-assign]
    SeedRunner.print_result_line = patches.seed_print_result_line  # type: ignore[method-assign]
    SnapshotRunner.print_result_line = patches.snapshot_print_result_line  # type: ignore[method-assign]
    TestRunner.execute = patches.test_execute  # type: ignore[method-assign]
    TestRunner.print_result_line = patches.test_print_result_line  # type: ignore[method-assign]
    GraphRunnableTask.execute_with_hooks = patches.execute_with_hooks  # type: ignore[method-assign]


def remove_patches() -> None:
    ModelRunner.execute = ORIGINALS["ModelRunner.execute"]  # type: ignore[method-assign]
    ModelRunner.print_result_line = ORIGINALS["ModelRunner.print_result_line"]  # type: ignore[method-assign]
    SeedRunner.print_result_line = ORIGINALS["SeedRunner.print_result_line"]  # type: ignore[method-assign]
    SnapshotRunner.print_result_line = ORIGINALS["SnapshotRunner.print_result_line"]  # type: ignore[method-assign]
    TestRunner.execute = ORIGINALS["TestRunner.execute"]  # type: ignore[method-assign]
    TestRunner.print_result_line = ORIGINALS["TestRunner.print_result_line"]  # type: ignore[method-assign]
    GraphRunnableTask.execute_with_hooks = ORIGINALS["GraphRunnableTask.execute_with_hooks"]  # type: ignore[method-assign]


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


class HodorPlugin(dbtPlugin):
    def initialize(self) -> None:
        from dbt_hodor.runtime import info, warn

        logging.getLogger("sqlglot").setLevel(logging.ERROR)
        install_state_config_support()

        if importlib.util.find_spec("dbt_state") is not None:
            remove_patches()
            warn("dbt-hodor is off: dbt-state is installed too, and both patch the same methods")
            return
        if os.environ.get("HODOR_MODE", "").lower() == "off":
            remove_patches()
            return
        command = getattr(get_flags(), "WHICH", None)
        if command not in ENABLED_FOR_COMMANDS:
            remove_patches()
            return
        install_patches()
        from dbt_hodor import __version__

        info(f"dbt-hodor {__version__} is enabled for dbt {command}")

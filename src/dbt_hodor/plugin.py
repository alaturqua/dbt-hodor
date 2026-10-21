from __future__ import annotations

import logging
import os
import sys
import typing as t
import signal
import types

from aenum import extend_enum


from dbt.events.types import EndOfRunSummary
from dbt.flags import get_flags
from dbt.graph.selector_methods import MethodName, MethodManager
from dbt.plugins import dbtPlugin
from dbt.task import test as dbt_test
from dbt.task.compile import CompileRunner, CompileTask
from dbt.task.run import ModelRunner, RunTask
from dbt.task.runnable import GraphRunnableTask
from dbt.task.seed import SeedRunner
from dbt.task.test import TestRunner

if t.TYPE_CHECKING:
    from dbt.config.runtime import RuntimeConfig
    from dbt.contracts.graph.manifest import Manifest
    from dbt.contracts.graph.nodes import ManifestNode
    from dbt.contracts.results import RunResult

    from dbt_state._typing import ModelOrSnapshotNode

# Capturing the original method references here on means that if the plugin is registered multiple times,
# it doesnt use its monkeypatched methods from the previous initialization as the "originals"
# this prevents stale references to things like database connections from causing problems
ORIGINALS: t.Dict[str, t.Callable] = {
    "CompileRunner.compile": CompileRunner.compile,
    "ModelRunner.compile": ModelRunner.compile,
    "ModelRunner.execute": ModelRunner.execute,
    "SeedRunner.compile": SeedRunner.compile,
    "SeedRunner.execute": SeedRunner.execute,
    "TestRunner.print_result_line": TestRunner.print_result_line,
    "EndOfRunSummary.message": EndOfRunSummary.message,
    "dbt_test.generate_runtime_model_context": dbt_test.generate_runtime_model_context,
    "GraphRunnableTask.defer_to_manifest": GraphRunnableTask.defer_to_manifest,
    "CompileTask.defer_to_manifest": CompileTask.defer_to_manifest,
    "RunTask.defer_to_manifest": RunTask.defer_to_manifest,
}

SIGNAL_HANDLER_ORIGINALS: t.Dict[str, t.Union[t.Optional[t.Callable], int]] = {
    "SIGINT": signal.getsignal(signal.SIGINT),
    "SIGTERM": signal.getsignal(signal.SIGTERM),
}

ENABLED_FOR_COMMANDS = ["run", "build", "seed", "snapshot", "compile", "test", "show"]

_TRUE_VALUES = frozenset({"true", "1", "t", "y", "yes", "on"})


def is_supported_command() -> bool:
    from dbt_state.utils import get_dbt_command_name

    command_name = get_dbt_command_name(get_flags())
    if command_name is not None and command_name not in ENABLED_FOR_COMMANDS:
        return False
    return True


def set_runner_overrides() -> None:
    from dbt_state.runner import RunnerOverride
    from query_cache_common.models.services.client_telemetry_service_models import ClientResult

    runner_override = RunnerOverride(
        ORIGINALS["ModelRunner.execute"],
        ORIGINALS["dbt_test.generate_runtime_model_context"],
        ORIGINALS["EndOfRunSummary.message"],
    )

    def compile_override(self: CompileRunner, manifest: Manifest) -> t.Any:
        return runner_override.compile_override(self, manifest)

    def execute_override(
        self: ModelRunner, node: ModelOrSnapshotNode, manifest: Manifest
    ) -> RunResult:
        return runner_override.execute_override(self, node, manifest)

    def print_result_line(self: TestRunner, result: RunResult) -> None:
        return runner_override.print_result_line_override(self, result)

    def generate_runtime_model_context_override(
        model: ManifestNode, config: RuntimeConfig, manifest: Manifest
    ) -> t.Dict[str, t.Any]:
        return runner_override.generate_runtime_model_context_override(model, config, manifest)

    def end_of_run_summary_message(self: EndOfRunSummary) -> str:
        return runner_override.end_of_run_summary_message_override(self)

    def defer_to_manifest_override(self: GraphRunnableTask, *args: t.Any) -> None:
        runner_override.defer_to_manifest_override(self)
        if self.previous_defer_state or self.previous_state:
            class_name = self.__class__.__name__
            if f"{class_name}.defer_to_manifest" in ORIGINALS:
                ORIGINALS[f"{class_name}.defer_to_manifest"](self, *args)
            else:
                ORIGINALS["GraphRunnableTask.defer_to_manifest"](self, *args)

    CompileRunner.compile = compile_override  # ty: ignore[invalid-assignment]
    ModelRunner.compile = compile_override  # ty: ignore[invalid-assignment]
    ModelRunner.execute = execute_override  # ty: ignore[invalid-assignment]
    SeedRunner.compile = compile_override  # ty: ignore[invalid-assignment]
    SeedRunner.execute = execute_override  # ty: ignore[invalid-assignment]
    TestRunner.print_result_line = print_result_line  # ty: ignore[invalid-assignment]
    EndOfRunSummary.message = end_of_run_summary_message  # ty: ignore[invalid-assignment]
    dbt_test.generate_runtime_model_context = generate_runtime_model_context_override  # ty: ignore[invalid-assignment]
    GraphRunnableTask.defer_to_manifest = defer_to_manifest_override  # ty: ignore[invalid-assignment]
    CompileTask.defer_to_manifest = defer_to_manifest_override  # ty: ignore[invalid-assignment]
    RunTask.defer_to_manifest = defer_to_manifest_override  # ty: ignore[invalid-assignment]

    cancelled = False

    def on_user_cancel(signum: int, frame: t.Optional[types.FrameType]) -> None:
        nonlocal cancelled

        # called when SIGINT (ctrl+c) or SIGTERM (kill <pid>) is sent
        signal_type = "SIGINT" if signum == signal.SIGINT else "SIGTERM"

        if session_manager := runner_override._session:
            if not cancelled:
                session_manager.end(
                    result=ClientResult.CANCELLED,
                    description=f"Cancelled by user: {signal_type}",
                    metrics={},
                )

                # ensure telemetry is flushed
                # this shutdown handler is normally called by an `atexit` handler registered in SessionManager.start(),
                # but atexit handlers only run on normal program termination, not termination via OS signals
                session_manager._close()

                # prevent a user hammering ctrl+c from triggering duplicate session end events
                cancelled = True

        if original_handler := SIGNAL_HANDLER_ORIGINALS.get(signal_type):
            if isinstance(original_handler, int):
                # Restore original handler and re-raise the signal so that the original termination behavior is preserved.
                signal.signal(signum, original_handler)
                os.kill(os.getpid(), signum)
            else:
                original_handler(signum, frame)

    # Signal handlers must be registered on the main thread
    # so we cannot register them within the RunCache constructor
    signal.signal(signal.SIGINT, on_user_cancel)
    signal.signal(signal.SIGTERM, on_user_cancel)


def remove_runner_overrides() -> None:
    CompileRunner.compile = ORIGINALS["CompileRunner.compile"]  # ty: ignore[invalid-assignment]
    ModelRunner.compile = ORIGINALS["ModelRunner.compile"]  # ty: ignore[invalid-assignment]
    ModelRunner.execute = ORIGINALS["ModelRunner.execute"]  # ty: ignore[invalid-assignment]
    SeedRunner.compile = ORIGINALS["SeedRunner.compile"]  # ty: ignore[invalid-assignment]
    SeedRunner.execute = ORIGINALS["SeedRunner.execute"]  # ty: ignore[invalid-assignment]
    TestRunner.print_result_line = ORIGINALS["TestRunner.print_result_line"]  # ty: ignore[invalid-assignment]
    EndOfRunSummary.message = ORIGINALS["EndOfRunSummary.message"]  # ty: ignore[invalid-assignment]
    dbt_test.generate_runtime_model_context = ORIGINALS["dbt_test.generate_runtime_model_context"]  # ty: ignore[invalid-assignment]
    GraphRunnableTask.defer_to_manifest = ORIGINALS["GraphRunnableTask.defer_to_manifest"]  # ty: ignore[invalid-assignment]
    CompileTask.defer_to_manifest = ORIGINALS["CompileTask.defer_to_manifest"]  # ty: ignore[invalid-assignment]
    RunTask.defer_to_manifest = ORIGINALS["RunTask.defer_to_manifest"]  # ty: ignore[invalid-assignment]


def install_state_config_support() -> None:
    """Monkeypatch support for a "state" config key, so it can be used in the following locations:

    -- dbt_project.yml --
    models:
      +state: ... (all models)
      <resource path>:
       +state: ... (all models under path)

    -- models/schema.yml --
    models:
      - name: my_model
        config:
          state: ...

    -- models/my_model.sql --
    {{ config(materialized="table", state={...}) }}
    select ..."""

    try:
        from dbt_common.contracts.config.base import BaseConfig
    except ImportError:
        # dbt 1.7
        from dbt.contracts.graph.model_config import BaseConfig

    update_keys = BaseConfig.mergebehavior["update"]
    if "state" not in update_keys:
        update_keys.append("state")

    if getattr(BaseConfig, "_run_cache_state_patch_installed", False):
        return

    original_update_from = BaseConfig.update_from
    TBaseConfig = t.TypeVar("TBaseConfig", bound=BaseConfig)

    def patched_update_from(
        self: TBaseConfig,
        data: t.Dict[str, t.Any],
        config_cls: t.Type[BaseConfig],
        validate: bool = True,
    ) -> TBaseConfig:
        if "state" not in data:
            return original_update_from(self, data, config_cls, validate)

        state = data.pop("state")

        # since the original call doesnt know about the "state" key and doesnt handle it correctly,
        # we remove the "state" key, make the original call, merge the state data ourselves and then inject it into the result
        updated = original_update_from(self, data, config_cls, validate)

        old_state = self.get("state")
        if isinstance(old_state, dict) and isinstance(state, dict):
            updated["state"] = {**old_state, **state}
        else:
            updated["state"] = state

        return updated

    BaseConfig.update_from = patched_update_from  # ty: ignore[invalid-assignment]
    setattr(BaseConfig, "_run_cache_state_patch_installed", True)


_LEGACY_PACKAGES: t.Tuple[str, ...] = ("run-cache", "dbt-run-cache")


def _check_for_legacy_packages() -> None:
    import importlib.metadata

    try:
        from dbt_common.exceptions import DbtRuntimeError
    except ImportError:
        # dbt 1.7
        from dbt.exceptions import DbtRuntimeError

    installed = []
    for name in _LEGACY_PACKAGES:
        try:
            importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            continue
        installed.append(name)

    if not installed:
        return

    uninstall_cmd = "pip uninstall -y " + " ".join(installed)
    raise DbtRuntimeError(
        "dbt-state cannot run while the legacy package(s) {pkgs} are installed. "
        "Please uninstall them manually and try again, e.g.: `{cmd}`.".format(
            pkgs=", ".join(installed),
            cmd=uninstall_cmd,
        )
    )


class RunCachePlugin(dbtPlugin):
    def initialize(self) -> None:
        """
        Initialize the dbt run cache plugin.
        """
        logging.getLogger("sqlglot").setLevel(logging.ERROR)

        if "DBT_ENGINE_MANAGE_STATE" in os.environ:
            explicitly_disabled = (
                os.getenv("DBT_ENGINE_MANAGE_STATE", "").lower() not in _TRUE_VALUES
            )
        else:
            disabled_value = (
                os.getenv("RUN_CACHE_DISABLED") or os.getenv("DBT_RUN_CACHE_DISABLED") or ""
            )
            explicitly_disabled = disabled_value.lower() in _TRUE_VALUES
        if explicitly_disabled:
            # the plugin may have been previously registered with run_cache_enabled meaning
            # that the global method overrides may already be in place
            remove_runner_overrides()
            try:
                from dbt_state import events
                from dbt_state.version import __version__

                events.fire_info_event("dbt-state v{} is disabled", __version__)
            except Exception:
                try:
                    from dbt.adapters.events.types import AdapterEventInfo
                    from dbt_common.events.functions import fire_event

                    fire_event(
                        AdapterEventInfo(name="RunCache", base_msg="dbt State is disabled", args=[])
                    )
                except Exception:
                    pass
            return

        _check_for_legacy_packages()

        # monkeypatch support for the "state" config key
        install_state_config_support()

        from dbt_state import events
        from dbt_state.version import __version__
        from dbt_state.selector import GitSelectorMethod

        # the plugin can be registered multiple times, particularly using dbtRunner in tests
        # so ensure we only monkeypatch this enum once
        if "git" not in {m.value for m in MethodName}:
            extend_enum(MethodName, "Git", "git")
            MethodManager.SELECTOR_METHODS[MethodName.Git] = GitSelectorMethod  # type: ignore

        _set_branch_env_vars()

        supported_command = is_supported_command()
        if supported_command:
            status = "enabled"
        else:
            status = "installed"
        events.fire_info_event(
            "dbt-state v{} is {}",
            __version__,
            status,
        )
        if not supported_command:
            # the plugin may have been previously registered with run_cache_enabled meaning
            # that the global method overrides may already be in place
            remove_runner_overrides()
            return

        # some sql queries are horrendous
        # make sure to change this number in the server as well
        sys.setrecursionlimit(10000)

        set_runner_overrides()


def _set_branch_env_vars() -> None:
    from dbt_state import events
    from dbt_state.git import GitClient
    from dbt_state.utils import sanitize_name, set_invocation_context

    try:
        git_client = GitClient(".")
        current_branch = git_client.get_current_branch()
        os.environ["DBT_CLOUD_GIT_BRANCH"] = current_branch
        os.environ["DBT_RUN_CACHE_GIT_BRANCH"] = current_branch
        os.environ["DBT_RUN_CACHE_GIT_BRANCH_SANITIZED"] = sanitize_name(current_branch)
        set_invocation_context()
    except Exception as e:
        os.environ.pop("DBT_CLOUD_GIT_BRANCH", None)
        os.environ.pop("DBT_RUN_CACHE_GIT_BRANCH", None)
        os.environ.pop("DBT_RUN_CACHE_GIT_BRANCH_SANITIZED", None)
        if isinstance(e, FileNotFoundError):
            events.fire_debug_event("Git branch detection skipped: git is not installed")
        elif "not a git repository" in str(e).lower():
            events.fire_debug_event("Git branch detection skipped: not a git repository")
        else:
            events.fire_warn_event("Git branch detection skipped: {}", str(e))

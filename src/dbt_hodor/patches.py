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
# Derived from dbt-state clients/dbt_state/src/dbt_state/runner.py (commit b0f3030).
# Modified for dbt-hodor: decisions come from dbt_hodor.runtime instead of the gRPC
# service; tests are replayed through dbt's own result builder; records flush at run end;
# seed and snapshot result lines carry the reason too.
"""The patched dbt methods. Each one hands over to the runtime or to the original."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

from dbt.contracts.graph.nodes import GenericTestNode
from dbt.events.types import LogModelResult as DbtLogModelResult
from dbt.events.types import LogTestResult as DbtLogTestResult
from dbt_common.events.base_types import EventLevel
from dbt_common.events.format import format_fancy_output_line
from dbt_common.events.functions import fire_event
from dbt_common.ui import green, red, yellow

from dbt_hodor.engine import REUSE
from dbt_hodor.plugin import ORIGINALS
from dbt_hodor.runtime import (
    REPLAY_PREFIX,
    current_runtime,
    finish_runtime,
    get_runtime,
    start_runtime,
    warn,
)


def model_execute(self: Any, model: Any, manifest: Any) -> Any:
    original = ORIGINALS["ModelRunner.execute"]
    runtime = get_runtime(self.config, self.adapter, manifest)
    if runtime is None:
        return original(self, model, manifest)
    return runtime.execute_model(self, model, manifest, original)


def test_execute(self: Any, test: Any, manifest: Any) -> Any:
    original = ORIGINALS["TestRunner.execute"]
    runtime = get_runtime(self.config, self.adapter, manifest)
    if runtime is None:
        return original(self, test, manifest)
    return runtime.execute_test(self, test, manifest, original)


def execute_with_hooks(self: Any, selected_uids: Any) -> Any:
    try:
        start_runtime(self, selected_uids)
    except Exception as exc:  # noqa: BLE001 - nodes create the runtime lazily instead
        warn(f"dbt-hodor: run start failed: {exc}")
    try:
        return ORIGINALS["GraphRunnableTask.execute_with_hooks"](self, selected_uids)
    finally:
        try:
            finish_runtime()  # also after errors and Ctrl+C: missing records cost extra builds
        except Exception as exc:  # noqa: BLE001
            warn(f"dbt-hodor: writing the last state records failed: {exc}")


def _result_line(original_key: str) -> Callable[[Any, Any], None]:
    """The result line with hodor's reason, for one runner class of the ModelRunner family."""

    def print_result_line(self: Any, result: Any) -> None:
        original = ORIGINALS[original_key]
        runtime = current_runtime()
        decision = runtime.decisions.get(self.node.unique_id) if runtime else None
        status = str(getattr(result.status, "value", result.status))
        if runtime is None or decision is None or status == "error":
            return original(self, result)
        if status != "reused":
            message = result.message or "OK"
            if runtime.settings.mode == "shadow" and decision.action == REUSE:
                message = f"{message} (shadow, could reuse: {decision.reason})"
            else:
                message = f"{message} ({decision.reason})"
            return original(self, replace(result, message=message))
        from dbt.task import group_lookup

        event = LogModelResult(
            description=self.describe_node(),
            status=result.message,
            index=self.node_index,
            total=self.num_nodes,
            execution_time=result.execution_time,
            node_info=self.node.node_info,
            group=group_lookup.get(self.node.unique_id),
        )
        fire_event(event, level=EventLevel.INFO)
        return None

    return print_result_line


model_print_result_line = _result_line("ModelRunner.print_result_line")
seed_print_result_line = _result_line("SeedRunner.print_result_line")
snapshot_print_result_line = _result_line("SnapshotRunner.print_result_line")


def test_print_result_line(self: Any, result: Any) -> None:
    message = (result.adapter_response or {}).get("_message") or ""
    if not str(message).startswith(REPLAY_PREFIX):
        return ORIGINALS["TestRunner.print_result_line"](self, result)
    from dbt.task import group_lookup

    model = result.node
    event = LogTestResult(
        name=self.describe_node_name(),
        status=str(result.status),
        index=self.node_index,
        num_models=self.num_nodes,
        execution_time=result.execution_time,
        node_info=model.node_info,
        num_failures=result.failures,
        group=group_lookup.get(model.unique_id),
        attached_node=model.attached_node if isinstance(model, GenericTestNode) else None,
    )
    fire_event(event, level=LogTestResult.status_to_level(str(result.status)))
    return None


class LogModelResult(DbtLogModelResult):
    """A model result line that starts with REUSED instead of 'OK created' for reused nodes.

    The class name must stay LogModelResult: it has to match dbt's protobuf message type.
    """

    def message(self) -> str:
        if not self.status.startswith("REUSED"):
            return super().message()
        return format_fancy_output_line(
            msg=f"REUSED {self.description}",
            status=green(self.status),
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


class LogTestResult(DbtLogTestResult):
    """A test result line that says the result was replayed.

    The class name must stay LogTestResult: it has to match dbt's protobuf message type.
    """

    def message(self) -> str:
        if self.status == "error":
            info, style = "ERROR", red
        elif self.status == "pass":
            info, style = "PASS", green
        elif self.status == "warn":
            info, style = f"WARN {self.num_failures}", yellow
        else:
            info, style = f"FAIL {self.num_failures}", red
        return format_fancy_output_line(
            msg=f"{info} {self.name}",
            status=style(f"{info} (replayed)"),
            index=self.index,
            total=self.num_models,
            execution_time=self.execution_time,
        )

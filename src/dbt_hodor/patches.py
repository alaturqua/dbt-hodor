from __future__ import annotations

import time
import typing as t
import threading

from dataclasses import replace

from dbt.adapters.factory import get_adapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.results import RunResult
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import (
    GenericTestNode,
    ModelNode,
    SeedNode,
    ManifestNode,
    SingularTestNode,
    SnapshotNode,
)
from dbt.events.types import EndOfRunSummary, LogTestResult as DbtLogTestResult, green, red, yellow
from dbt_state.errors import AuthenticationError, UnsupportedClientVersionError

try:
    from dbt_common.events.format import format_fancy_output_line
except ImportError:
    from dbt.events.format import format_fancy_output_line  # type: ignore

try:
    from dbt_common.exceptions import DbtRuntimeError
except ImportError:
    # dbt 1.7
    from dbt.exceptions import DbtRuntimeError

from dbt_state import events
from dbt_state.adapters import ADAPTER_EXTENSION_MAPPING
from dbt_state.auth import sso_auth
from dbt_state.config import RunCacheConfig, DBT_RUN_CACHE_PATH
from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state.dispatcher import AsyncTelemetryDispatcher
from dbt_state.system_info import get_system_user_id, get_os_name
from dbt_state.run_cache import RunCache, NoRunResult
from dbt_state.session import SessionManager
from dbt_state.utils import (
    is_ci_environment,
    is_non_interactive_environment,
    format_time_saved,
    DBT_VERSION,
)
from dbt_state.version import __version__
from query_cache_common.constants import NO_OP_STATUS

if t.TYPE_CHECKING:
    from dbt.adapters.sql import SQLAdapter
    from dbt.contracts.graph.nodes import ManifestNode
    from dbt.task.compile import CompileRunner
    from dbt.task.run import ModelRunner
    from dbt.task.runnable import GraphRunnableTask
    from dbt.task.test import TestRunner

    from dbt_state._typing import ModelOrSnapshotNode


class RunnerOverride:
    def __init__(
        self,
        original_execute: t.Callable[[ModelRunner, ModelOrSnapshotNode, Manifest], RunResult],
        original_generate_runtime_model_context: t.Callable[
            [ManifestNode, RuntimeConfig, Manifest], t.Dict[str, t.Any]
        ],
        original_end_of_run_summary_message: t.Callable[[EndOfRunSummary], str],
        telemetry_dispatcher: AsyncTelemetryDispatcher | None = None,
        query_cache_client: QueryCacheGrpcClient | None = None,
    ) -> None:
        self._original_execute = original_execute
        self._original_generate_runtime_model_context = original_generate_runtime_model_context
        self._original_end_of_run_summary_message = original_end_of_run_summary_message
        self._query_cache_client = query_cache_client
        self._run_cache: RunCache | None = None
        self._run_cache_lock = threading.Lock()
        self._disabled = False
        self._telemetry_dispatcher = telemetry_dispatcher
        self._session: t.Optional[SessionManager] = None

    def defer_to_manifest_override(self, task: GraphRunnableTask) -> None:
        """Sets defer_relation on unselected manifest nodes to enable dbt's native deferral."""
        manifest = task.manifest
        if manifest is None:
            return

        adapter = get_adapter(task.config)
        run_cache = self._run_cache_get_or_create(task.config, adapter, manifest)
        if run_cache is None:
            return

        run_cache.prewarm_connections()

        any_deferred = False
        for unique_id, node in manifest.nodes.items():
            if not isinstance(node, (ModelNode, SeedNode, SnapshotNode)):
                continue
            defer_rel = run_cache.get_defer_relation(node)
            if defer_rel is not None:
                attrs_to_replace: t.Dict[str, t.Any] = dict(defer_relation=defer_rel)
                if DBT_VERSION < (1, 8, 0) and self._should_defer(node, task.config, adapter):
                    # In dbt 1.7.* the manifest node is replaced in place, while in later versions
                    # setting defer_relation on the node is sufficient
                    attrs_to_replace.update(
                        dict(
                            database=defer_rel.database,
                            schema=defer_rel.schema,
                            alias=defer_rel.alias,
                            deferred=True,
                        )
                    )
                manifest.nodes[unique_id] = replace(node, **attrs_to_replace)
                any_deferred = True

        if any_deferred:
            # Flags is a frozen dataclass, so we use object.__setattr__
            object.__setattr__(task.config.args, "defer", True)

    def execute_override(
        self, runner: ModelRunner, node: ModelOrSnapshotNode, manifest: Manifest
    ) -> RunResult:
        try:
            run_cache = self._run_cache_get_or_create(runner.config, runner.adapter, manifest)
            if run_cache is None:
                return self._original_execute(runner, node, manifest)

            try:
                on_execute_result = run_cache.on_execute(node)
            except Exception as e:
                events.fire_warn_event_with_cache_bypass(
                    "execute: dbt State failed for node {}:\n{}",
                    node.unique_id,
                    str(e),
                )
                return self._original_execute(runner, node, manifest)

            if isinstance(on_execute_result, RunResult):
                return on_execute_result

            execute_start_ts = time.perf_counter()
            result = self._original_execute(runner, node, manifest)

            run_cache.on_run_result(node, result)

            if isinstance(on_execute_result, NoRunResult):
                try:
                    elapsed_ms = int((time.perf_counter() - execute_start_ts) * 1000)
                    run_cache.confirm_execution(
                        node,
                        on_execute_result.request_id,
                        on_execute_result.failed_to_clone,
                        execution_runtime_ms=elapsed_ms,
                    )
                except Exception as e:
                    events.fire_warn_event_with_cache_bypass(
                        "execute: request confirmation failed for node {}:\n{}",
                        node.unique_id,
                        str(e),
                    )
            return result
        finally:
            self.flush_logger(node.name)

    def compile_override(self, runner: CompileRunner, manifest: Manifest) -> t.Any:
        run_cache = self._run_cache_get_or_create(runner.config, runner.adapter, manifest)
        if run_cache is not None:
            try:
                run_cache._decision_logger.log_node_start(runner.node)
                run_cache.on_compile(runner.node)
                for dep_id in getattr(runner.node.depends_on, "nodes", []):
                    dep_node = manifest.nodes.get(dep_id)
                    if dep_node and (defer_rel := getattr(dep_node, "defer_relation", None)):
                        run_cache._decision_logger.log_deferral(
                            node_name=runner.node.name,
                            relation_name=dep_node.name,
                            deferred_to_fqn=defer_rel.relation_name,
                        )
            except Exception as e:
                events.fire_warn_event_with_cache_bypass(
                    "compile: dbt State failed for node {}:\n{}",
                    runner.node.unique_id,
                    str(e),
                )

        if isinstance(runner.node, SeedNode):
            return runner.node
        compiler = runner.compiler if hasattr(runner, "compiler") else runner.adapter.get_compiler()
        compiled_node = compiler.compile_node(runner.node, manifest, {})

        if run_cache is not None:
            try:
                run_cache.cache_compiled_view_sql(compiled_node)
            except Exception as e:
                events.fire_debug_event(
                    "compile: failed to cache view SQL for node {}:\n{}",
                    runner.node.unique_id,
                    str(e),
                )

        return compiled_node

    def print_result_line_override(self, runner: TestRunner, result: RunResult) -> None:
        """Override for printing test results to include NO-OP status."""
        try:
            model = result.node
            kwargs: t.Dict[str, t.Any] = {}
            try:
                from dbt.task import group_lookup

                kwargs["group"] = group_lookup.get(model.unique_id)
                kwargs["attached_node"] = (
                    result.node.attached_node if isinstance(result.node, GenericTestNode) else None
                )
            except ImportError:
                pass

            try:
                name = runner.describe_node_name()
            except Exception:
                name = model.name

            log_test_result = LogTestResult(
                name=name,
                status=str(result.status),
                index=runner.node_index,
                num_models=runner.num_nodes,
                execution_time=result.execution_time,
                node_info=model.node_info,
                num_failures=result.failures,
                **kwargs,
            )
            log_test_result.set_is_no_op(
                (result.adapter_response.get("_message") or "").startswith(NO_OP_STATUS)
            )
            events.dbt_fire_event(
                log_test_result, level=LogTestResult.status_to_level(str(result.status))
            )
        finally:
            self.flush_logger(result.node.name)

    def end_of_run_summary_message_override(self, summary: EndOfRunSummary) -> str:
        message = self._original_end_of_run_summary_message(summary)
        if (
            self._run_cache
            and self._run_cache.total_cache_hits
            and "Completed successfully" in message
        ):
            tolerance = format_time_saved(
                self._run_cache.run_cache_config.freshness_tolerance * 1000
            )
            message += green(
                f". Total cache hits: {self._run_cache.total_cache_hits}."
                f" Estimated time saved: {format_time_saved(self._run_cache.total_time_saved_ms)}."
                f" Freshness tolerance: {tolerance}."
            )
        return message

    def generate_runtime_model_context_override(
        self, node: ManifestNode, config: RuntimeConfig, manifest: Manifest
    ) -> t.Dict[str, t.Any]:
        context = self._original_generate_runtime_model_context(node, config, manifest)
        # Only override for test nodes. Other kinds of nodes handled by compile_override, so we don't want to interfere with them here.
        if not isinstance(node, (GenericTestNode, SingularTestNode)):
            return context

        adapter = context["adapter"]._adapter
        run_cache = self._run_cache_get_or_create(config, adapter, manifest)
        if run_cache is None or not run_cache.run_cache_config.enable_data_tests:
            return context

        try:
            run_cache._decision_logger.log_node_start(node)
            context["adapter"]._adapter = run_cache.data_test_adapter_proxy(node)
        except Exception as e:
            events.fire_warn_event_with_cache_bypass(
                "generate_runtime_model_context: dbt State failed for node {}:\n{}",
                node.unique_id,
                str(e),
            )
        return context

    def flush_logger(self, node_name: str) -> None:
        if rc := self._run_cache:
            try:
                rc._decision_logger.log_node_end(node_name)
            except Exception as e:
                events.fire_debug_event(
                    "Failed to flush explainer log entry for {}: {}", node_name, str(e)
                )

    def _get_or_create_client(self, run_cache_config: RunCacheConfig) -> QueryCacheGrpcClient:
        if self._query_cache_client is None:
            self._session = SessionManager()
            self._query_cache_client = QueryCacheGrpcClient.create(
                run_cache_config=run_cache_config,
                session_id=self._session._session_id,
                system_user_id=get_system_user_id(DBT_RUN_CACHE_PATH),
                os_name=get_os_name(),
            )
            self._telemetry_dispatcher = AsyncTelemetryDispatcher(
                query_cache_client=self._query_cache_client,
            )
            self._session.start(
                run_cache_config=run_cache_config, telemetry_dispatcher=self._telemetry_dispatcher
            )
        return self._query_cache_client

    def _run_cache_get_or_create(
        self, config: RuntimeConfig, adapter: SQLAdapter, manifest: Manifest
    ) -> RunCache | None:
        if not self._disabled and self._run_cache is None:
            with self._run_cache_lock:
                if self._run_cache is None and not self._disabled:
                    adapter_type = adapter.type()
                    if adapter_type not in ADAPTER_EXTENSION_MAPPING:
                        events.fire_warn_event(
                            "dbt State disabled: target type '{}' is not supported. "
                            "Supported target types are: {}.",
                            adapter_type,
                            ", ".join(sorted(ADAPTER_EXTENSION_MAPPING.keys())),
                        )
                        self._disabled = True
                        return None

                    run_cache_config = RunCacheConfig.from_runtime_config(config)
                    if is_ci_environment() or is_non_interactive_environment():
                        sso = sso_auth(org_id=run_cache_config.org_id)
                        if not sso.is_logged_in() and not run_cache_config.oauth_client_secret:
                            events.fire_warn_event(
                                "dbt State disabled: not authenticated and no OAuth client credentials configured. "
                                "To authenticate, either configure OAuth credentials via environment variables (RUN_CACHE_OAUTH_CLIENT_ID and RUN_CACHE_OAUTH_CLIENT_SECRET), "
                                "in profiles.yml (run_cache_oauth_client_id and run_cache_oauth_client_secret), "
                                "or run `dbt-state auth login` in an attached terminal."
                            )
                            self._disabled = True
                            return None

                    try:
                        client = self._get_or_create_client(run_cache_config)
                    except AuthenticationError as e:
                        # Don't print a stack trace to console for AuthenticationError
                        # Raising it as a DbtRuntimeError prints just the message and halts execution
                        raise DbtRuntimeError(str(e)) from e

                    try:
                        client_version_is_supported = client.is_client_version_supported()
                    except Exception as e:
                        events.fire_warn_event("Failed to validate client version: %s", str(e))
                    else:
                        if not client_version_is_supported:
                            raise UnsupportedClientVersionError(
                                "Client version %s is not supported", __version__
                            )

                    assert self._telemetry_dispatcher is not None
                    assert self._session is not None
                    self._run_cache = RunCache.create(
                        run_cache_config,
                        config,
                        adapter,
                        manifest,
                        client,
                        self._telemetry_dispatcher,
                        self._session,
                    )
        return self._run_cache

    def _should_defer(self, node: ManifestNode, config: RuntimeConfig, adapter: SQLAdapter) -> bool:
        # Check whetehr the original relation already exists and the user did not opt into favoring the state
        return self._favor_state(config) or not adapter.get_relation(
            node.database, node.schema, node.identifier
        )

    def _favor_state(self, config: RuntimeConfig) -> bool:
        return getattr(config.args, "favor_state", False)


class LogTestResult(DbtLogTestResult):
    """The override for the LogTestResult event to modify the status to include the NO-OP status.

    NOTE: Don't change the name of the class. It must be exactly LogTestResult to match the corresponding protobuf message type.
    """

    def set_is_no_op(self, is_no_op: bool) -> None:
        # This is a hack to workaround the fact that the base class overrides __settattr__
        object.__setattr__(self, "_is_no_op", is_no_op)

    def get_is_no_op(self) -> bool:
        return self.__getattribute__("_is_no_op")

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR"
            style = red
        elif self.status == "pass":
            info = "PASS"
            style = green
        elif self.status == "warn":
            info = f"WARN {self.num_failures}"
            style = yellow
        else:  # self.status == "fail":
            info = f"FAIL {self.num_failures}"
            style = red

        msg = f"{info} {self.name}"

        # This is what the whole struggle is about
        if self.get_is_no_op():
            info += f" ({NO_OP_STATUS})"
        status = style(info)

        result = format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.num_models,
            execution_time=self.execution_time,
        )
        return result

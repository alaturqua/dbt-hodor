"""One hodor run: settings, store, records and per-node decisions. Every failure fails open."""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.flags import get_flags
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from dbt_common.invocation import get_invocation_id

from dbt_hodor import __version__
from dbt_hodor.config import NodeStateConfig, Settings, load_settings, node_state_config
from dbt_hodor.engine import (
    BUILD,
    REUSE,
    VOLATILE_PREFIX,
    Decision,
    NodeFacts,
    decide,
    upstream_versions,
)
from dbt_hodor.hashing import (
    HASH_VERSION,
    SqlAnalysis,
    analyze_node_sql,
    config_hash,
    unrendered_hash,
)
from dbt_hodor.lineage import GraphNode, Lineage, external_inputs, normalize_relation
from dbt_hodor.metadata import UNKNOWN, DataVersion
from dbt_hodor.providers import DeltaProvider, DuckDBProvider, Provider, store_executor
from dbt_hodor.records import Record
from dbt_hodor.store import StateStore, StoreError

SUPPORTED_ADAPTERS = ("databricks", "duckdb")
# None is the adapter default (merge on dbt-databricks).
SUPPORTED_STRATEGIES = (
    None,
    "append",
    "merge",
    "delete+insert",
    "insert_overwrite",
    "replace_where",
)
OWN_DATA_KINDS = ("table", "incremental", "seed", "snapshot")
NODE_TYPES = ("model", "seed", "snapshot")  # the ModelRunner family
FLUSH_EVERY = 200
REPLAY_PREFIX = "dbt-hodor: replayed"
OPAQUE_GROUP = "<opaque inputs>"
DAY_MS = 86_400_000


def info(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.INFO)


def warn(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.WARN)


def debug(msg: str) -> None:
    fire_event(Note(msg=msg), level=EventLevel.DEBUG)


def _value(enum_or_str: Any) -> str:
    return str(getattr(enum_or_str, "value", enum_or_str))


def _relation(node: Any) -> str | None:
    name = getattr(node, "relation_name", None)
    return normalize_relation(name) if name else None


def build_graph(manifest: Any) -> dict[str, GraphNode]:
    graph: dict[str, GraphNode] = {}
    for uid, node in manifest.nodes.items():
        resource_type = _value(node.resource_type)
        materialized = (
            node.config.get("materialized") if resource_type == "model" else resource_type
        )
        parents = tuple(getattr(getattr(node, "depends_on", None), "nodes", None) or ())
        graph[uid] = GraphNode(uid, resource_type, materialized, _relation(node), parents)
    for uid, source in manifest.sources.items():
        graph[uid] = GraphNode(uid, "source", None, _relation(source))
    return graph


def model_kind(model: Any) -> str:
    if _value(getattr(model, "language", "sql")) != "sql":
        return "unsupported"
    materialized = model.config.get("materialized")
    if materialized in ("table", "view", "seed", "snapshot"):
        return materialized
    if materialized == "incremental":
        strategy = model.config.get("incremental_strategy")
        return "incremental" if strategy in SUPPORTED_STRATEGIES else "unsupported"
    return "unsupported"


def seed_hash(seed: Any) -> str:
    """sha256 of the seed file. dbt's own checksum is only the path for large seeds."""
    with open(os.path.join(seed.root_path, seed.original_file_path), "rb") as f:
        data = f.read().replace(b"\r\n", b"\n")  # same hash on Windows and Linux checkouts
    return hashlib.sha256(data).hexdigest()


def full_refresh(node: Any) -> bool:
    """dbt's rule: the node's full_refresh config, when set, wins over the flag."""
    configured = node.config.get("full_refresh")
    if configured is not None:
        return bool(configured)
    return bool(getattr(get_flags(), "FULL_REFRESH", False))


def _profile_settings(config: Any) -> dict[str, object]:
    """hodor_* keys of the active target. dbt drops unknown keys from credentials, so read
    profiles.yml again and render the keys the way dbt renders the profile."""
    from dbt.config.profile import read_profile
    from dbt.config.renderer import ProfileRenderer

    raw = read_profile(str(get_flags().PROFILES_DIR))
    outputs = (raw.get(config.profile_name) or {}).get("outputs") or {}
    target = outputs.get(config.target_name) or {}
    keys = {k: v for k, v in target.items() if isinstance(k, str) and k.startswith("hodor_")}
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
    if settings.store == "duckdb":
        from dbt_hodor.store.duckdb import DuckDBStore

        path = settings.path
        if path != ":memory:" and not os.path.isabs(path):
            path = os.path.join(config.project_root, path)
        return DuckDBStore(path, settings.schema or "main", settings.table, read_only)
    if settings.store == "postgres":
        from dbt_hodor.store.postgres import PostgresStore, lakebase_target

        assert settings.database and settings.schema  # load_settings turns hodor off otherwise
        host, user, credential = settings.host, settings.user, None
        if settings.endpoint:  # Lakebase: the endpoint supplies host, user and the token
            host, user, credential = lakebase_target(settings.endpoint)
        if credential is None:
            password = settings.password or ""
            credential = lambda: password  # noqa: E731 - a plain Postgres password
        assert host  # load_settings turns hodor off without a host or an endpoint
        return PostgresStore(
            settings.database,
            settings.schema,
            settings.table,
            host,
            settings.port,
            user or "",
            credential,
            settings.sslmode,
            read_only,
        )
    from dbt_hodor.store.delta import DeltaStore

    assert settings.database and settings.schema  # load_settings turns hodor off otherwise
    return DeltaStore(
        settings.database,
        settings.schema,
        settings.table,
        store_executor(adapter),
        read_only=read_only,
    )


@dataclass
class Plan:
    node_key: str
    decision: Decision
    cfg: NodeStateConfig
    started_ms: int
    last: Record | None
    facts: NodeFacts | None = None
    text_hash: str | None = None
    analysis: SqlAnalysis | None = None


class Runtime:
    def __init__(
        self,
        invocation_id: str,
        settings: Settings,
        config: Any,
        adapter: Any,
        manifest: Any,
        store: StateStore,
        provider: Provider,
    ) -> None:
        self.invocation_id = invocation_id
        self.settings = settings
        self.project_name = config.project_name
        self.target_name = config.target_name
        self.adapter = adapter
        self.dialect = adapter.type()  # sqlglot dialect names match: duckdb, databricks
        self.manifest = manifest
        self.store = store
        self.provider = provider
        self.graph = build_graph(manifest)
        self.lineage = Lineage(self.graph)
        self._lock = threading.Lock()
        self._pending: list[Record] = []
        self._warned: set[str] = set()
        self._macro_cache: dict[str, dict[str, str]] = {}
        self._volatile: dict[str, DataVersion] | None = None
        try:
            from dbt.adapters.factory import get_adapter_package_names

            self._internal_packages = frozenset(get_adapter_package_names(self.dialect))
        except Exception:  # noqa: BLE001 - the plugin is loaded in a real run
            self._internal_packages = frozenset({"dbt", f"dbt_{self.dialect}"})
        self.reused = self.built = self.errors = 0
        self.finished = False
        self.decisions: dict[str, Decision] = {}  # for the result line

        try:
            offset = provider.now_ms() - int(time.time() * 1000)
        except Exception as exc:  # noqa: BLE001 - the local clock only affects lag_tolerance
            warn(f"dbt-hodor: cannot read the warehouse clock, using the local clock: {exc}")
            offset = 0
        self._clock_offset_ms = offset

        from dbt import tracking

        started = getattr(getattr(tracking, "active_user", None), "run_started_at", None)
        self.run_started_at = str(started) if started else None

        since = self.now_ms() - settings.lookback_days * DAY_MS
        try:
            self.records = store.load(self.project_name, self.target_name, since)
        except StoreError as exc:
            warn(f"dbt-hodor: {exc}; every node builds this run")
            self.records = {}
        info(
            f"dbt-hodor {__version__}: mode {settings.mode}, {len(self.records)} build records "
            f"for {self.project_name}/{self.target_name}"
        )

    @classmethod
    def create(cls, config: Any, adapter: Any, manifest: Any, invocation_id: str) -> Runtime | None:
        settings = load_settings(
            _profile_settings(config), os.environ, lambda m: warn(f"dbt-hodor: {m}")
        )
        if settings.mode == "off":
            info("dbt-hodor is off")
            return None
        adapter_type = adapter.type()
        if adapter_type not in SUPPORTED_ADAPTERS:
            warn(f"dbt-hodor is off: adapter {adapter_type!r} is not supported")
            return None
        provider = (DuckDBProvider if adapter_type == "duckdb" else DeltaProvider)(adapter, debug)
        try:
            store = open_store(settings, config, adapter)
        except StoreError as exc:
            warn(f"dbt-hodor is off for this run: {exc}")
            return None
        return cls(invocation_id, settings, config, adapter, manifest, store, provider)

    def prefetch(self, selected_uids: Any) -> None:
        """Data versions of the selected nodes' targets and upstream tables, in parallel.

        External inputs found in compiled SQL are read later, per node: nothing is
        compiled yet.
        """
        tables: set[str] = set()
        for uid in selected_uids:
            node = self.graph.get(uid)
            if node is None or node.resource_type not in (*NODE_TYPES, "test"):
                continue
            if node.relation and node.materialized in OWN_DATA_KINDS:
                tables.add(node.relation)
            for parent in node.parents:
                tables |= self.lineage.base_tables(parent) or frozenset()
        started = time.monotonic()
        self.provider.prefetch(sorted(tables), self.settings.metadata_threads)
        debug(f"dbt-hodor: read {len(tables)} data versions in {time.monotonic() - started:.1f}s")

    # -- helpers -------------------------------------------------------------------------

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self._clock_offset_ms

    def warn_once(self, msg: str) -> None:
        with self._lock:
            if msg in self._warned:
                return
            self._warned.add(msg)
        warn(f"dbt-hodor: {msg}")

    def _macro_sql(self, macro_ids: Iterable[str]) -> dict[str, str]:
        """macro_sql of the given macros and every macro they call, recursively."""
        out: dict[str, str] = {}
        stack = list(macro_ids)
        while stack:
            macro_id = stack.pop()
            macro = self.manifest.macros.get(macro_id)
            if macro_id in out or macro is None:
                continue
            out[macro_id] = macro.macro_sql
            stack.extend(macro.depends_on.macros)
        return out

    def _execution_macros(self, node: Any) -> dict[str, str]:
        """Project and package macros that the materialization or the hooks run.

        Their changes (for example an override of create_table_as that adds a column) do not
        show in the compiled SQL. dbt's and the adapter's own macros are left out, so an
        adapter upgrade does not rebuild every node.
        """
        roots: list[str] = []
        materialized = node.config.get("materialized")
        if materialized:
            macro = self.manifest.find_materialization_macro_by_name(
                self.project_name, materialized, self.dialect
            )
            if macro is not None:
                roots.append(macro.unique_id)
        for hook in [*getattr(node.config, "pre_hook", ()), *getattr(node.config, "post_hook", ())]:
            roots.extend(self._hook_macros(hook.sql))
        key = "|".join(sorted(set(roots)))
        cached = self._macro_cache.get(key)
        if cached is None:
            cached = {
                macro_id: sql
                for macro_id, sql in self._macro_sql(roots).items()
                if self.manifest.macros[macro_id].package_name not in self._internal_packages
            }
            self._macro_cache[key] = cached
        return cached

    def _hook_macros(self, sql: str) -> list[str]:
        from dbt.clients.jinja_static import statically_extract_macro_calls

        found = []
        for call in statically_extract_macro_calls(sql, {}, None):
            package, _, name = call.rpartition(".")
            macro = self.manifest.find_macro_by_name(name, self.project_name, package or None)
            if macro is not None:
                found.append(macro.unique_id)
        return found

    def _config_hash(self, node: Any) -> str:
        columns = {name: col.to_dict() for name, col in (node.columns or {}).items()}
        constraints = [c.to_dict() for c in getattr(node, "constraints", None) or ()]
        return config_hash(
            node.config.to_dict(omit_none=True), columns, constraints, self._execution_macros(node)
        )

    def _volatile_groups(
        self, cfg: NodeStateConfig, analysis: SqlAnalysis
    ) -> dict[str, Mapping[str, DataVersion]]:
        """Volatile values as upstreams. Unset config: stamp columns do not count (auto)."""
        if cfg.evaluate_volatile_sql is False:
            return {}
        classes = analysis.volatile
        if cfg.evaluate_volatile_sql:
            classes = classes | analysis.volatile_stamps
        if not classes:
            return {}
        values = {"always": DataVersion(self.invocation_id), **self._volatile_values()}
        return {
            f"{VOLATILE_PREFIX}{cls}": {f"{VOLATILE_PREFIX}{cls}": values[cls]}
            for cls in sorted(classes)
        }

    def _volatile_values(self) -> dict[str, DataVersion]:
        with self._lock:
            cached = self._volatile
        if cached is not None:
            return cached
        try:
            values = {k: DataVersion(v) for k, v in self.provider.volatile_values().items()}
        except Exception as exc:  # noqa: BLE001 - unknown values: the nodes build
            self.warn_once(
                f"cannot read the current date and user, nodes that use them build: {exc}"
            )
            values = {"date": UNKNOWN, "session": UNKNOWN}
        with self._lock:
            if self._volatile is None:
                self._volatile = values
            return self._volatile

    def _analyze(self, node: Any, last: Record | None) -> tuple[str, SqlAnalysis]:
        return analyze_node_sql(
            node.compiled_code or "",
            self.invocation_id,
            self.run_started_at,
            last.text_hash if last else None,
            last.sql_facts if last else None,
            self.dialect,
        )

    def _versions(self, tables: frozenset[str]) -> dict[str, DataVersion]:
        return {table: self.provider.version(table) for table in sorted(tables)}

    def _upstream_groups(
        self, unique_id: str, own: str | None, analysis: SqlAnalysis
    ) -> dict[str, Mapping[str, DataVersion] | None]:
        groups: dict[str, Mapping[str, DataVersion] | None] = {}
        known: set[str] = {own} if own else set()
        for parent, tables in self.lineage.upstream_groups(unique_id).items():
            node = self.graph.get(parent)
            if node is not None and node.relation:
                known.add(node.relation)
            if tables is None:
                groups[parent] = None
                continue
            known |= tables
            groups[parent] = self._versions(tables)
        ext = external_inputs(analysis, known)
        for table in sorted(ext.tables):
            groups[table] = {table: self.provider.version(table)}
        for path in sorted(ext.delta_paths):
            groups[f"delta:{path}"] = {f"delta:{path}": self.provider.path_version(path)}
        if ext.unknown:
            groups[OPAQUE_GROUP] = None
        return groups

    def _plan(
        self,
        node: Any,
        node_key: str,
        facts_fn: Callable[[NodeStateConfig, Record | None], tuple[NodeFacts, str, SqlAnalysis]],
    ) -> Plan:
        started = self.now_ms()
        last = self.records.get(node_key)
        cfg = NodeStateConfig()
        try:
            cfg = node_state_config(node.config.get("state"), self.warn_once)
            facts, text_hash, analysis = facts_fn(cfg, last)
            decision = decide(facts, last, cfg, started)
        except Exception as exc:  # noqa: BLE001 - fail open
            warn(f"dbt-hodor: deciding {node.unique_id} failed, building it: {exc}")
            with self._lock:
                self.errors += 1
            return Plan(node_key, Decision(BUILD, "hodor_error"), cfg, started, last)
        return Plan(node_key, decision, cfg, started, last, facts, text_hash, analysis)

    def _record(self, node: Any, plan: Plan, action: str, status: str, **extra: Any) -> None:
        facts = plan.facts
        try:
            record = Record(
                record_id=str(uuid.uuid4()),
                invocation_id=self.invocation_id,
                recorded_at_ms=self.now_ms(),
                project_name=self.project_name,
                target_name=self.target_name,
                node_key=plan.node_key,
                unique_id=node.unique_id,
                resource_type=_value(node.resource_type),
                mode=self.settings.mode,
                decision=plan.decision.action,
                reason=plan.decision.reason,
                action=action,
                status=status,
                text_hash=plan.text_hash,
                code_hash=facts.code_hash if facts else None,
                unrendered_hash=facts.unrendered_hash if facts else None,
                config_hash=facts.config_hash if facts else None,
                hash_version=HASH_VERSION if facts else None,
                sql_facts=plan.analysis.to_json() if plan.analysis else None,
                upstreams=upstream_versions(facts) if facts else {},
                hodor_version=__version__,
                **extra,
            )
        except Exception as exc:  # noqa: BLE001 - a lost record only causes an extra build
            self.warn_once(f"cannot build a state record: {exc}")
            return
        with self._lock:
            self._pending.append(record)
            batch = self._pending if len(self._pending) >= FLUSH_EVERY else None
            if batch is not None:
                self._pending = []
        if batch is not None:
            self._write(batch)

    def _write(self, batch: list[Record]) -> None:
        try:
            self.store.append(batch)
        except Exception as exc:  # noqa: BLE001 - the next run builds these nodes again
            self.warn_once(f"{exc}; {len(batch)} records lost")

    def _count(self, plan: Plan) -> None:
        """Count by decision, so shadow mode reports what `on` would have reused."""
        with self._lock:
            if plan.decision.action == REUSE:
                self.reused += 1
            else:
                self.built += 1

    # -- models --------------------------------------------------------------------------

    def _model_facts(
        self, runner: Any, model: Any, cfg: NodeStateConfig, last: Record | None
    ) -> tuple[NodeFacts, str, SqlAnalysis]:
        kind = model_kind(model)
        if kind == "seed":
            analysis = SqlAnalysis(code_hash=seed_hash(model), select_star=False)
            text_hash = analysis.code_hash
        else:
            text_hash, analysis = self._analyze(model, last)
        relation = _relation(model)
        exists = runner.adapter.get_relation(model.database, model.schema, model.alias) is not None
        target = UNKNOWN
        if kind in OWN_DATA_KINDS and exists and relation:
            target = self.provider.version(relation)
        groups = {}
        if kind in OWN_DATA_KINDS:  # a view computes volatile values when it is read
            groups = self._upstream_groups(model.unique_id, relation, analysis)
            groups |= self._volatile_groups(cfg, analysis)
        unrendered = None
        if cfg.compare_unrendered_code:
            unrendered = unrendered_hash(
                model.raw_code, self._macro_sql(model.depends_on.macros), model.unrendered_config
            )
        facts = NodeFacts(
            kind=kind,
            code_hash=analysis.code_hash,
            config_hash=self._config_hash(model),
            hash_version=HASH_VERSION,
            unrendered_hash=unrendered,
            full_refresh=full_refresh(model),
            relation_exists=exists,
            target_data_version=target,
            select_star_view=kind == "view" and analysis.select_star,
            upstream_groups=groups,
        )
        return facts, text_hash, analysis

    def _run_hooks(self, runner: Any, model: Any, manifest: Any) -> None:
        from dbt.clients.jinja import get_rendered
        from dbt.context.providers import generate_runtime_model_context

        context = generate_runtime_model_context(model, runner.config, manifest)
        for hook in list(context["pre_hooks"] or []) + list(context["post_hooks"] or []):
            sql = get_rendered(hook.get("sql") or "", context, model).strip()
            if sql:
                runner.adapter.execute(sql, auto_begin=False)

    def execute_model(self, runner: Any, model: Any, manifest: Any, original: Callable) -> Any:
        resource_type = _value(model.resource_type)
        if (
            resource_type not in NODE_TYPES
            or model.config.get("incremental_strategy") == "microbatch"
            # `dbt seed --show` prints each result's agate table, which a reuse does not load
            or (resource_type == "seed" and getattr(get_flags(), "SHOW", False))
        ):
            return original(runner, model, manifest)
        relation = _relation(model)
        plan = self._plan(
            model,
            relation or model.unique_id,
            lambda cfg, last: self._model_facts(runner, model, cfg, last),
        )
        self.decisions[model.unique_id] = plan.decision
        if plan.decision.action == REUSE and self.settings.mode == "on":
            if plan.cfg.execute_hooks_on_reuse:
                self._run_hooks(runner, model, manifest)
            self._record(model, plan, REUSE, RunStatus.Reused.value)
            self._count(plan)
            return RunResult.from_node(model, RunStatus.Reused, f"REUSED ({plan.decision.reason})")
        try:
            result = original(runner, model, manifest)
        except BaseException:
            self._record(model, plan, BUILD, "error", built_at_ms=plan.started_ms)
            raise
        self._count(plan)
        version = UNKNOWN
        if plan.facts is not None and plan.facts.kind in OWN_DATA_KINDS and relation:
            version = self.provider.refresh(relation)
        self._record(
            model,
            plan,
            BUILD,
            _value(result.status),
            built_at_ms=plan.started_ms,
            data_version=version.token,
            data_changed_at_ms=version.changed_at_ms,
        )
        return result

    # -- tests ---------------------------------------------------------------------------

    def test_key(self, test: Any) -> str:
        parents = test.depends_on.nodes
        tested = sorted((self.graph[p].relation or p) if p in self.graph else p for p in parents)
        digest = hashlib.sha256("|".join(tested).encode("utf-8")).hexdigest()[:16]
        return f"{test.unique_id}#{digest}"

    def _test_facts(
        self, test: Any, cfg: NodeStateConfig, last: Record | None
    ) -> tuple[NodeFacts, str, SqlAnalysis]:
        text_hash, analysis = self._analyze(test, last)
        facts = NodeFacts(
            kind="test",
            code_hash=analysis.code_hash,
            config_hash=self._config_hash(test),
            hash_version=HASH_VERSION,
            full_refresh=full_refresh(test),
            upstream_groups=self._upstream_groups(test.unique_id, None, analysis)
            | self._volatile_groups(cfg, analysis),
        )
        return facts, text_hash, analysis

    def execute_test(self, runner: Any, test: Any, manifest: Any, original: Callable) -> Any:
        from dbt.contracts.graph.nodes import GenericTestNode, SingularTestNode
        from dbt.task.test import TestResultData

        if not isinstance(test, (GenericTestNode, SingularTestNode)):
            return original(runner, test, manifest)
        plan = self._plan(
            test, self.test_key(test), lambda cfg, last: self._test_facts(test, cfg, last)
        )
        if plan.decision.action == REUSE and self.settings.mode == "on" and plan.last:
            recorded = plan.last.test_result or {}
            data = TestResultData(
                failures=int(recorded.get("failures") or 0),  # type: ignore[call-overload]
                should_warn=bool(recorded.get("should_warn")),
                should_error=bool(recorded.get("should_error")),
                adapter_response={"_message": f"{REPLAY_PREFIX} ({plan.decision.reason})"},
            )
            result = runner.build_test_run_result(test, data)
            self._record(test, plan, REUSE, _value(result.status))
            self._count(plan)
            return result
        try:
            data = runner.execute_data_test(test, manifest)
        except BaseException:
            self._record(test, plan, BUILD, "error", built_at_ms=plan.started_ms)
            raise
        result = runner.build_test_run_result(test, data)
        self._count(plan)
        self._record(
            test,
            plan,
            BUILD,
            _value(result.status),
            built_at_ms=plan.started_ms,
            test_result={
                "failures": data.failures,
                "should_warn": data.should_warn,
                "should_error": data.should_error,
            },
        )
        return result

    # -- end of run ----------------------------------------------------------------------

    def finish(self) -> None:
        if self.finished:
            return
        self.finished = True
        with self._lock:
            batch, self._pending = self._pending, []
        if batch:
            self._write(batch)
        try:
            self.store.close()
        except Exception as exc:  # noqa: BLE001
            debug(f"dbt-hodor: closing the store failed: {exc}")
        if self.settings.mode == "shadow":
            summary = (
                f"{self.reused} could be reused, {self.built} need a build (shadow: all built)"
            )
        else:
            summary = f"{self.reused} reused, {self.built} built"
        info(f"dbt-hodor: {summary}, {self.errors} fail-open errors")


_slot: tuple[str, Runtime | None] | None = None
_slot_lock = threading.Lock()


def get_runtime(config: Any, adapter: Any, manifest: Any) -> Runtime | None:
    """The runtime of the current dbt invocation, created on the first node."""
    global _slot
    invocation = get_invocation_id()
    slot = _slot
    if slot is not None and slot[0] == invocation:
        return slot[1]
    with _slot_lock:
        if _slot is None or _slot[0] != invocation:
            try:
                runtime = Runtime.create(config, adapter, manifest, invocation)
            except Exception as exc:  # noqa: BLE001 - fail open for the whole run
                warn(f"dbt-hodor is off for this run: {exc}")
                runtime = None
            _slot = (invocation, runtime)
        return _slot[1]


def start_runtime(task: Any, selected_uids: Any) -> None:
    """Create the runtime on the main thread before any node runs, then prefetch."""
    from dbt.adapters.factory import get_adapter

    adapter = get_adapter(task.config)
    with adapter.connection_named("hodor"):
        runtime = get_runtime(task.config, adapter, task.manifest)
    if runtime is not None:
        runtime.prefetch(selected_uids)


def current_runtime() -> Runtime | None:
    """The runtime of the current invocation, without creating one."""
    slot = _slot
    return slot[1] if slot is not None and slot[0] == get_invocation_id() else None


def finish_runtime() -> None:
    slot = _slot
    if slot is not None and slot[0] == get_invocation_id() and slot[1] is not None:
        slot[1].finish()

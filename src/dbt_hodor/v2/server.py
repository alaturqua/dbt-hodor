"""The local gRPC state server that dbt v2 asks whether to run each node.

Fail open: any error, a store that cannot open or a slow store answers "build", no defer
and no clone. Records are read again for each dbt run (each `ValidateClientVersion`), and a
store that failed to open is opened again then. One wait budget, `STORE_WAIT_S`, covers the
records of a project and target for a whole run: the first decision that needs them starts
the clock, later ones wait only until the same deadline, and no single request waits
longer than the budget. `ValidateClientVersion` never touches the store: dbt stops the run
when that call times out.

The one exception is `GetStateSelection`: a `state:` selection that cannot be compared fails
the run with a gRPC error, as dbt-core does without a comparison manifest. A guess could run
the wrong set of models.
"""

from __future__ import annotations

import dataclasses
import logging
import queue
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any

import grpc
from google.protobuf import json_format, message_factory

from .. import __version__
from ..core.clone import CLONE, CLONED, may_clone, node_upstreams, relation_ids
from ..core.config import DAY_MS, BaseTarget, NodeStateConfig
from ..core.defer import deferred_relations
from ..core.engine import BUILD, REUSE, Decision, NodeFacts, decide, upstream_versions
from ..core.hashing import SqlAnalysis, normalize_runtime_values, text_hash
from ..core.records import CARRY, REUSED, SUCCESS_STATUSES, Record
from ..core.records import by_unique_id as _by_unique_id
from ..core.records import newer as _newer
from ..core.selection import SUB_CRITERIA, select
from ..store import StateStore
from . import SUPPORTED_CLIENT_VERSIONS
from .mapping import (
    CloneAnswer,
    answer,
    clone_epoch,
    clone_sql,
    criterion,
    node_facts,
    normalize,
    seed_facts,
    v2_node_hashes,
)
from .proto.query_cache_protobuf.query_cache import shared_pb2
from .proto.query_cache_protobuf.query_cache.services import (
    client_telemetry_service_pb2,
    client_validation_service_pb2,
    clone_service_pb2,
    execution_service_pb2,
    explain_service_pb2,
    health_service_pb2,
    selector_service_pb2,
    sql_service_pb2,
)

log = logging.getLogger("dbt_hodor.v2")

_MODULES = (
    client_telemetry_service_pb2,
    client_validation_service_pb2,
    clone_service_pb2,
    execution_service_pb2,
    explain_service_pb2,
    health_service_pb2,
    selector_service_pb2,
    sql_service_pb2,
)
# The one wait budget, below the ~60 s dbt waits for an answer. A decision waits for the
# records of its project and target until a deadline set by the first decision of the dbt
# run that needs them (and never longer than this for one request). A state selection and
# a defer wait this long, selection never answering from less than all the records.
STORE_WAIT_S = 30.0
# statuses of the records state selection compares with: a model's reuse counts
_SELECTION_STATUSES = (*SUCCESS_STATUSES, REUSED)
# Telemetry sessions kept for their defer target: the latest few dbt invocations.
KEEP_SESSIONS = 64
_INVOCATION_HEADER = "x-dbt-invocation-id"
# How long a decision is kept for its ConfirmExecution and for GetExplainMessages.
KEEP_MS = 6 * 3600 * 1000
# Action of a record that marks a seed's table as written outside its builds.
STALE = "stale"
_STOP = object()
_FLUSH_TIMEOUT_S = 10.0


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Records:
    """The records of one project and target, loaded in the background once per dbt run."""

    ready: threading.Event = field(default_factory=threading.Event)
    ok: bool = False
    stale: bool = False  # a new dbt run began: `_entry` replaces it with an empty one, reloaded
    error: str | None = None  # why the load failed
    version: int = 0  # changes with `builds`
    # (version, latest build per unique_id): clone looks up its base record by id
    builds_by_id: tuple[int, dict[str, Record]] = (-1, {})
    builds: dict[str, Record] = field(default_factory=dict)  # latest build or carry
    latest: dict[str, Record] = field(default_factory=dict)  # latest of any action


class _Abort(Exception):  # noqa: N818 - a gRPC status for dbt, not a server error
    """A gRPC status that ends the call (only state selection fails a run)."""

    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class _Session:
    """What one dbt invocation told the server: its defer target and its own target."""

    defer_to: str | None = None
    target: str | None = None
    started_ms: int = 0  # when ValidateClientVersion arrived; 0 when it never did


@dataclass(frozen=True)
class _Pending:
    """A decision that answered execute, waiting for its ConfirmExecution."""

    facts: NodeFacts
    analysis: SqlAnalysis | None
    node_hashes: Mapping[str, object]
    decision: Decision
    started_ms: int
    invocation: str
    project: str
    target: str
    node_key: str
    unique_id: str
    resource_type: str
    text_hash: str | None
    clone: bool = False  # answered ready_to_clone


def _invocation_id(context: Any) -> str:
    for key, value in context.invocation_metadata() or ():
        if key == _INVOCATION_HEADER:
            return value
    return ""


def _unable_to_clone(req: Any, reason: str, out_cls: Any) -> Any:
    unable = clone_service_pb2.UnableToCloneResponse(
        clone_source=req.clone_source_table,
        clone_target=req.target_table,
        explained_decision=shared_pb2.ExplainedDecision(
            decision=shared_pb2.READY_TO_EXECUTE, decision_description=reason
        ),
    )
    return out_cls(unable_to_clone=unable)


def _explain_lines(
    facts: NodeFacts, last: Record | None, node_key: str, decision: Decision
) -> list[Any]:
    """One line per input the decision compared with the last build."""
    line = explain_service_pb2.ExplainLine
    if decision.reason in ("store_slow", "hodor_error"):
        return [line(text=f"records not loaded: {decision.reason}")]  # nothing was compared
    if last is None:
        return [line(text="no build record for this node")]

    def same(now: object, then: object) -> str:
        return "same as" if now == then else "differs from"

    target = facts.target_data_version.token if facts.target_data_version.known else None
    lines = [
        line(text=f"code hash {same(facts.code_hash, last.code_hash)} the last build"),
        line(text=f"config hash {same(facts.config_hash, last.config_hash)} the last build"),
        line(text=f"{node_key}: {target or 'unknown'} (last build: {last.data_version})"),
    ]
    for table, token in sorted(upstream_versions(facts).items()):
        then = last.upstreams.get(table)
        lines.append(line(text=f"{table}: {token or 'unknown'} (last build: {then})"))
    return lines


def _convert(src: Any, out_cls: Any) -> Any:
    """Copy the fields two messages share by name (a recorded execution to a submit)."""
    data = json_format.MessageToDict(src, preserving_proto_field_name=True)
    names = set(out_cls.DESCRIPTOR.fields_by_name)
    return json_format.ParseDict({k: v for k, v in data.items() if k in names}, out_cls())


class StateServer:
    def __init__(
        self,
        store_factory: Callable[[], StateStore],
        project_name: str,
        project_id: str | None,
        port: int,
        lookback_days: int = 90,
        allow_clones: bool = False,
    ) -> None:
        self.project_name = project_name
        self.project_id = project_id
        self.port = port
        self.lookback_days = lookback_days
        # dbt State's clone settings live on the profiles.yml target, which the server
        # cannot see. Clones are off unless `serve --allow-clones`: a dbt v2 shallow clone of
        # an auto-clustered Databricks table makes its next rebuild fail. pre_clone has no
        # v2 answer
        self.allow_clones = allow_clones
        self._store_error: str | None = None
        self._sessions: dict[str, _Session] = {}  # x-dbt-invocation-id: session
        self._store_factory = store_factory
        self._store: StateStore | None = None
        self._store_ready = threading.Event()
        self._lock = threading.Lock()
        self._loads: dict[tuple[str, str], _Records] = {}
        # (project, target): when decisions stop waiting for the records, this dbt run
        self._deadlines: dict[tuple[str, str], float] = {}
        # record_id: records this server wrote that the writer has not stored yet. A load or a
        # selection that reads the store adds them to what it read, so no record is missed
        self._unflushed: dict[str, Record] = {}
        self._pending: dict[str, _Pending] = {}
        self._explained: dict[str, tuple[int, Any]] = {}  # id: (kept at ms, entry)
        self._queue: queue.Queue[Any] = queue.Queue()
        self._invocation = str(uuid.uuid4())
        self._server: Any = None
        self._threads: list[threading.Thread] = []
        # rpc name -> handler(request, out_cls, context); RPCs without one answer `_default`
        self._handlers: dict[str, Callable[[Any, Any, Any], Any]] = {
            "ValidateClientVersion": self._validate,
            "SubmitEnrichedSQLSpeculative": lambda req, out, ctx: out(
                undecided=sql_service_pb2.UndecidedResponse()
            ),
            "SubmitEnrichedSQL": self._submit_sql,
            "SubmitValues": self._submit_values,
            "ConfirmExecution": self._confirm,
            "RecordExecutions": self._record_executions,
            "GetExplainMessages": self._explain,
            "SubmitTelemetryBatch": self._telemetry,
            "RegisterSessionStart": self._session_start,
            "GetStateSelection": self._select,
            "ResolveDeferredRelations": self._defer,
            "RegisterClone": self._register_clone,
        }

    # -- lifecycle -----------------------------------------------------------------------

    def start(self) -> int:
        """Open the store in the background, serve, and return the bound port."""
        self._threads = [
            threading.Thread(target=self._open_store, name="hodor-store-open", daemon=True),
            threading.Thread(target=self._write_loop, name="hodor-writer", daemon=True),
        ]
        for thread in self._threads:
            thread.start()
        self._executor = futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="hodor-rpc")
        server = grpc.server(self._executor)
        server.add_generic_rpc_handlers(tuple(self._generic_handlers()))
        bound = server.add_insecure_port(f"127.0.0.1:{self.port}")
        if not bound:
            raise RuntimeError(f"cannot listen on 127.0.0.1:{self.port}")
        server.start()
        self._server = server
        return bound

    def stop(self) -> None:
        """Stop serving, write the queued records and close the store."""
        if self._server is not None:
            self._server.stop(grace=1).wait()
            self._server = None
            with self._lock:  # requests still waiting for records answer build, not 30 s later
                for entry in self._loads.values():
                    entry.ready.set()
            self._executor.shutdown(wait=True)
        self._queue.put(_STOP)
        with self._lock:
            threads = list(self._threads)
        for thread in threads:  # the writer flushes the queue; loaders finish their read
            thread.join(_FLUSH_TIMEOUT_S)
            if thread.is_alive():
                log.warning("dbt-hodor: the store did not answer in time; records may be lost")
                return
        if self._store is not None:
            try:
                self._store.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("dbt-hodor: closing the store failed: %s", exc)
            self._store = None

    def _reopen_store(self) -> None:
        """Open a store that failed to open again, in the background. A run is the unit of
        retry: a store that stays down costs the decisions their wait once per run."""
        with self._lock:
            if self._store is not None or not self._store_ready.is_set():
                return  # open, or still opening
            self._store_ready.clear()
            opener = threading.Thread(target=self._open_store, name="hodor-store-open", daemon=True)
            self._threads = [t for t in self._threads if t.is_alive()] + [opener]
        opener.start()

    def _open_store(self) -> None:
        try:
            self._store = self._store_factory()
        except Exception as exc:  # noqa: BLE001 - the server runs on and every node builds
            log.warning("dbt-hodor: cannot open the state store, every node builds: %s", exc)
            self._store_error = f"cannot open the state store: {exc}"
        finally:
            self._store_ready.set()

    def _write_loop(self) -> None:
        """The one writer: dbt sends requests in parallel, the store gets one append at a time."""
        while True:
            batch = [self._queue.get()]
            self._store_ready.wait()  # the store may be opening again
            store = self._store
            while True:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            records = [r for r in batch if r is not _STOP]
            if records and store is None:
                log.warning("dbt-hodor: no state store, %d records lost", len(records))
            elif records and store is not None:
                try:
                    store.append(records)
                except Exception as exc:  # noqa: BLE001 - the next run builds these nodes
                    log.warning("dbt-hodor: %s; %d records lost", exc, len(records))
            with self._lock:  # stored or lost, they are no longer waiting
                for record in records:
                    self._unflushed.pop(record.record_id, None)
            if len(records) < len(batch):
                return

    # -- handlers from the service descriptors -------------------------------------------

    def _generic_handlers(self) -> list[Any]:
        handlers = []
        for module in _MODULES:
            for service in module.DESCRIPTOR.services_by_name.values():
                methods = {}
                for method in service.methods:
                    if method.client_streaming or method.server_streaming:
                        continue  # Health/Watch: dbt does not call it
                    in_cls = message_factory.GetMessageClass(method.input_type)
                    out_cls = message_factory.GetMessageClass(method.output_type)
                    methods[method.name] = grpc.unary_unary_rpc_method_handler(
                        self._wrap(method.name, out_cls),
                        request_deserializer=in_cls.FromString,
                        response_serializer=out_cls.SerializeToString,
                    )
                handlers.append(grpc.method_handlers_generic_handler(service.full_name, methods))
        return handlers

    def _wrap(self, name: str, out_cls: Any) -> Callable[[Any, Any], Any]:
        handler = self._handlers.get(name)

        def unary(request: Any, context: Any) -> Any:
            if handler is not None:
                try:
                    return handler(request, out_cls, context)
                except _Abort as status:
                    context.abort(status.code, status.message)
                except Exception as exc:  # noqa: BLE001 - fail open
                    log.warning("dbt-hodor: %s failed, answering build: %r", name, exc)
            return self._default(name, request, out_cls, context)

        return unary

    def _default(self, name: str, request: Any, out_cls: Any, context: Any) -> Any:
        """The safe answer of each rpc: build, unsupported, no clone, accepted."""
        if name in ("SubmitEnrichedSQL", "SubmitValues"):
            return answer(Decision(BUILD, "hodor_error"), str(uuid.uuid4()), None, out_cls)
        if name == "SubmitEnrichedSQLSpeculative":
            return out_cls(undecided=sql_service_pb2.UndecidedResponse())
        if name == "ValidateClientVersion":
            return out_cls(is_supported=False)
        if name == "Check":
            return out_cls(status=health_service_pb2.HealthCheckResponse.SERVING)
        if name == "ConfirmExecution":
            return out_cls(success=True, request_id=request.request_id)
        if name in ("SubmitTelemetryBatch", "RegisterSessionStart"):
            return out_cls(success=True)
        if name == "RegisterClone":
            return _unable_to_clone(request, "hodor_error", out_cls)
        if name == "GetStateSelection":
            # a state: selection that cannot be compared fails the run, as in dbt-core
            context.abort(
                grpc.StatusCode.INTERNAL,
                "dbt-hodor cannot answer the state: selector; see the dbt-hodor serve log",
            )
        return out_cls()  # ResolveDeferredRelations: nothing deferred

    # -- records -------------------------------------------------------------------------

    def _entry(self, key: tuple[str, str]) -> _Records:
        with self._lock:
            return self._entry_locked(key)

    def _entry_locked(self, key: tuple[str, str]) -> _Records:
        """The records of a key for this run; a missing or stale entry is replaced by an
        empty one that is loaded in the background (`ok` is False until then). The lock is held."""
        entry = self._loads.get(key)
        if entry is None or entry.stale:
            entry = self._loads[key] = _Records()
            loader = threading.Thread(
                target=self._load, args=(key, entry), name="hodor-load", daemon=True
            )
            self._threads = [t for t in self._threads if t.is_alive()] + [loader]
            loader.start()
        return entry

    @staticmethod
    def _apply(entry: _Records, record: Record) -> None:
        """Keep a record in the entry when it is the newest of its kind. The lock is held."""
        if record.status in SUCCESS_STATUSES and _newer(entry.latest.get(record.node_key), record):
            entry.latest[record.node_key] = record
        if (
            record.action in (BUILD, CARRY)
            and record.status in SUCCESS_STATUSES
            and _newer(entry.builds.get(record.node_key), record)
        ):
            entry.builds[record.node_key] = record
            entry.version += 1

    def _load(self, key: tuple[str, str], entry: _Records) -> None:
        self._store_ready.wait()
        store = self._store
        try:
            if store is None:
                # every decision builds; the entry stays until the next run replaces it, so a
                # store that is down starts one loader, not one per request
                entry.error = self._store_error or "no state store"
                return
            since = now_ms() - self.lookback_days * DAY_MS
            with self._lock:  # before the read: a record flushed after it is in the snapshot
                waiting = [
                    r for r in self._unflushed.values() if (r.project_name, r.target_name) == key
                ]
            builds = store.load(key[0], key[1], since)
            latest = store.load_latest(key[0], key[1], since)
            with self._lock:  # the store's records, and what it may not hold yet
                # the store's rows are already the latest build and the latest record
                for target, loaded in ((entry.builds, builds), (entry.latest, latest)):
                    for node_key, record in loaded.items():
                        if _newer(target.get(node_key), record):
                            target[node_key] = record
                for record in waiting:
                    self._apply(entry, record)
                entry.version += 1
                entry.ok = True
        except Exception as exc:  # noqa: BLE001 - this decision builds, the next one retries
            log.warning("dbt-hodor: cannot load records of %s/%s: %s", *key, exc)
            entry.error = f"cannot read records: {exc}"
            with self._lock:
                if self._loads.get(key) is entry:
                    del self._loads[key]
        finally:
            entry.ready.set()

    def _records(
        self, key: tuple[str, str], asked: float | None = None
    ) -> tuple[_Records | None, Decision | None]:
        """The loaded records, or the build decision when they are not there in time.

        The wait ends at the run's deadline for this key, set by the first request that
        needed it, and never later than `asked` (the request's start) plus the budget."""
        asked = time.monotonic() if asked is None else asked
        entry = self._entry(key)
        with self._lock:
            deadline = self._deadlines.setdefault(key, time.monotonic() + STORE_WAIT_S)
        deadline = min(deadline, asked + STORE_WAIT_S)
        if not entry.ready.wait(max(0.0, deadline - time.monotonic())):
            return None, Decision(BUILD, "store_slow")
        with self._lock:
            ok = entry.ok
        return (entry, None) if ok else (None, Decision(BUILD, "hodor_error"))

    def _base_build(self, entry: _Records, unique_id: str) -> Record | None:
        """The latest build of `unique_id` in a loaded entry."""
        with self._lock:
            version, by_id = entry.builds_by_id
            if version != entry.version:
                by_id = _by_unique_id(entry.builds.values())
                entry.builds_by_id = (entry.version, by_id)
            return by_id.get(unique_id)

    def _session(self, invocation: str) -> _Session:
        with self._lock:
            return dataclasses.replace(self._sessions.get(invocation) or _Session())

    def _update_session(self, invocation: str, **values: Any) -> None:
        with self._lock:
            session = self._sessions.pop(invocation, None) or _Session()  # newest goes last
            for name, value in values.items():
                setattr(session, name, value)
            self._sessions[invocation] = session
            while len(self._sessions) > KEEP_SESSIONS:
                del self._sessions[next(iter(self._sessions))]

    def _write(self, record: Record) -> None:
        """Queue a record, and keep it for this server's next decisions."""
        key = (record.project_name, record.target_name)
        with self._lock:  # one section: the entry cannot be replaced between look-up and use
            self._apply(self._entry_locked(key), record)
            self._unflushed[record.record_id] = record
        self._queue.put(record)

    def _record(self, p: _Pending, action: str, status: str, **extra: Any) -> Record:
        facts = p.facts
        return Record(
            record_id=str(uuid.uuid4()),
            invocation_id=p.invocation,
            recorded_at_ms=now_ms(),
            project_name=p.project,
            target_name=p.target,
            node_key=p.node_key,
            unique_id=p.unique_id,
            resource_type=p.resource_type,
            mode="on",
            decision=p.decision.action,
            reason=p.decision.reason,
            action=action,
            status=status,
            text_hash=p.text_hash,
            code_hash=facts.code_hash,
            config_hash=facts.config_hash,
            hash_version=facts.hash_version,
            sql_facts=p.analysis.to_json() if p.analysis else None,
            upstreams=upstream_versions(facts),
            hodor_version=__version__,
            node_hashes=dict(p.node_hashes),
            **extra,
        )

    def _keep(self, store: dict[str, Any], key: str, value: Any, kept_ms: int) -> None:
        with self._lock:
            store[key] = value
            # insertion order is age order: drop from the front while too old
            for old in list(store):
                item = store[old]
                if kept_ms - (item.started_ms if isinstance(item, _Pending) else item[0]) < KEEP_MS:
                    break
                del store[old]

    # -- rpcs ----------------------------------------------------------------------------

    def _validate(self, req: Any, out_cls: Any, context: Any) -> Any:
        self._invocation = str(uuid.uuid4())  # dbt validates once per run
        self._update_session(_invocation_id(context), started_ms=now_ms())
        self._reopen_store()
        with self._lock:  # a new run reads the records again; the first decision loads them
            for entry in self._loads.values():
                entry.stale = True
            self._deadlines.clear()
        return out_cls(is_supported=req.dbt_run_cache_version in SUPPORTED_CLIENT_VERSIONS)

    def _submit_sql(self, req: Any, out_cls: Any, context: Any) -> Any:
        return self._submit(req, out_cls, context, seed=False)

    def _submit_values(self, req: Any, out_cls: Any, context: Any) -> Any:
        return self._submit(req, out_cls, context, seed=True)

    def _submit(self, req: Any, out_cls: Any, context: Any, seed: bool) -> Any:
        request_id = str(uuid.uuid4())
        started = now_ms()
        asked = time.monotonic()
        state = req.dbt_node_state if req.HasField("dbt_node_state") else None
        if state is None or not req.target_table:
            unsupported = Decision(BUILD, "unsupported")
            self._remember_explain(request_id, unsupported, [], started)
            return answer(unsupported, request_id, None, out_cls)
        key = (state.project_name or self.project_name, state.target_name)
        node_key = normalize(req.target_table)
        dbt_invocation = _invocation_id(context)
        invocation = dbt_invocation or self._invocation
        self._update_session(dbt_invocation, target=key[1])
        entry, decision = self._records(key, asked)
        with self._lock:
            last = entry.builds.get(node_key) if entry else None
            latest = entry.latest.get(node_key) if entry else None
        analysis: SqlAnalysis | None = None
        if seed:
            facts = seed_facts(req, last)
            if decision is None and latest is not None and latest.action == STALE:
                decision = Decision(BUILD, "target_modified")
        else:
            facts, analysis = node_facts(req, last, {})
        if decision is None:
            # dbt's freshness tolerance is the lag tolerance; 0 (or unset) means none
            cfg = (
                NodeStateConfig()
                if seed
                else NodeStateConfig(lag_tolerance_s=max(0, req.freshness_tolerance_seconds))
            )
            decision = decide(facts, last, cfg, started)
        clone: CloneAnswer | None = None
        if not seed and entry is not None:
            self._mark_stale_seeds(req, entry, node_key, invocation)
            try:
                found = self._clone(
                    req, key, entry, facts, analysis, decision, dbt_invocation, asked
                )
            except Exception as exc:  # noqa: BLE001 - the node builds as decided
                log.warning("dbt-hodor: clone check failed, building: %r", exc)
                found = None
            if found is not None:
                decision, clone = found
        pending = _Pending(
            facts=facts,
            analysis=analysis,
            node_hashes=v2_node_hashes(state, req.target_table),
            decision=decision,
            started_ms=started,
            invocation=invocation,
            project=key[0],
            target=key[1],
            node_key=node_key,
            unique_id=state.node_unique_id,
            resource_type=state.resource_type or ("seed" if seed else "model"),
            text_hash=None if seed else text_hash(normalize_runtime_values(req.sql)),
            clone=clone is not None,
        )
        if decision.action == REUSE:
            self._write(self._record(pending, REUSE, REUSED))
        else:
            self._keep(self._pending, request_id, pending, started)
        lines = _explain_lines(facts, last, node_key, decision)
        self._remember_explain(request_id, decision, lines, started, clone is not None)
        return answer(decision, request_id, clone, out_cls)

    def _clone(
        self,
        req: Any,
        key: tuple[str, str],
        entry: _Records,
        facts: NodeFacts,
        analysis: SqlAnalysis | None,
        decision: Decision,
        dbt_invocation: str,
        asked: float,
    ) -> tuple[Decision, CloneAnswer] | None:
        """The clone decision and answer when the base target's table may be cloned."""
        if decision.action != BUILD or analysis is None:
            return None
        base_target = self._session(dbt_invocation).defer_to
        if not base_target or base_target == key[1]:
            return None
        base_entry, _ = self._records((key[0], base_target), asked)
        if base_entry is None:
            return None
        base = self._base_build(base_entry, req.dbt_node_state.node_unique_id)
        if base is None:
            return None
        current_up = upstream_versions(facts)
        # Upstreams compare by node id. A name maps to a node only through the records of
        # the base and the current target; one that maps to no node, or to two, blocks the
        # clone (sources have no records, so a model that reads one is never cloned).
        names = {normalize(n) for n in (*current_up, *base.upstreams)}
        with self._lock:  # relation_ids takes {unique_id: record}
            known = [
                _by_unique_id(records[n] for n in names if n in records)
                for records in (base_entry.latest, entry.latest)
            ]
        ids = relation_ids(*known)
        cloned = may_clone(
            facts,
            decision,
            base,
            BaseTarget(base_target, self.allow_clones, pre_clone=False),
            node_upstreams(current_up, ids),
            node_upstreams(base.upstreams, ids),
            base_is_current=False,
            volatile_stamps=analysis.volatile_stamps,
            volatile=analysis.volatile,
        )
        # PRE_CLONED (clone, then run on top) has no v2 answer: the node runs as decided
        if cloned is None or cloned.reason != CLONED:
            return None
        epoch = clone_epoch(base.data_version)
        hashes = base.node_hashes or {}
        source = hashes.get("relation") if isinstance(hashes, Mapping) else None
        if epoch is None or not isinstance(source, str) or not source:
            return None
        if normalize(source) == normalize(req.target_table):
            return None  # two target names that write to one schema: nothing to clone
        sql = clone_sql(req.dialect, req.target_table, source)
        if sql is None:
            return None
        return cloned, CloneAnswer((sql,), source, req.target_table, epoch)

    def _mark_stale_seeds(self, req: Any, entry: _Records, node_key: str, invocation: str) -> None:
        """A seed table read with another epoch than at the seed's last build was written
        outside dbt: mark the seed stale, so its next run builds it."""
        for info in req.tables:
            name = normalize(info.name)
            if name == node_key or not info.HasField("last_modified_epoch"):
                continue
            with self._lock:
                build = entry.builds.get(name)
                latest = entry.latest.get(name)
            if build is None or build.resource_type != "seed":
                continue
            if latest is not None and latest.action == STALE:
                continue
            if str(info.last_modified_epoch) == build.data_version:
                continue
            self._write(
                dataclasses.replace(
                    build,
                    record_id=str(uuid.uuid4()),
                    invocation_id=invocation,
                    recorded_at_ms=now_ms(),
                    decision=BUILD,
                    reason="target_modified",
                    action=STALE,
                    status="success",
                )
            )

    def _remember_explain(
        self,
        request_id: str,
        decision: Decision,
        lines: list[Any],
        kept_ms: int,
        cloned: bool = False,
    ) -> None:
        if decision.action == REUSE:
            result = shared_pb2.SKIP_EXECUTION
        else:
            result = shared_pb2.READY_TO_CLONE if cloned else shared_pb2.READY_TO_EXECUTE
        entry = explain_service_pb2.ExplainMessageEntry(
            execution_decision_id=request_id,
            decision=result,
            decision_description=decision.reason,
            explain_lines=lines,
        )
        self._keep(self._explained, request_id, (kept_ms, entry), kept_ms)

    def _confirm(self, req: Any, out_cls: Any, context: Any) -> Any:
        with self._lock:
            pending = self._pending.pop(req.request_id, None)
        # an unknown id: a decision of another server, or too old. A failed clone writes
        # nothing: dbt builds instead and the node has no record of it
        if pending is not None and not (pending.clone and req.failed_to_clone):
            epoch = req.last_modified_epoch if req.HasField("last_modified_epoch") else None
            self._write(
                self._record(
                    pending,
                    CLONE if pending.clone else BUILD,
                    "success",
                    data_version=str(epoch) if epoch is not None else None,
                    data_changed_at_ms=epoch,
                    built_at_ms=pending.started_ms,
                )
            )
        return out_cls(success=True, request_id=req.request_id)

    def _record_executions(self, req: Any, out_cls: Any, context: Any) -> Any:
        """Builds dbt ran without asking (incremental models under --full-refresh)."""
        stored = 0
        dbt_invocation = _invocation_id(context)
        started = self._session(dbt_invocation).started_ms
        for item in req.records:
            try:
                record = self._executed(item, started, dbt_invocation or self._invocation)
            except Exception as exc:  # noqa: BLE001 - the other entries are still recorded
                log.warning("dbt-hodor: cannot record an execution: %r", exc)
                continue
            if record is not None:
                self._write(record)
                stored += 1
        return out_cls(records_stored=stored)

    def _executed(self, item: Any, started: int, invocation: str) -> Record | None:
        """The build record of one RecordExecutions entry; None when it names no node.

        dbt may report an execution long after it ran, so its start is taken as the start of
        the invocation: never later than the real one. When no invocation was seen the start
        is 0, which makes the same-second check build the next time.
        """
        outcome = item.outcome
        if item.HasField("enriched_sql"):
            sub = _convert(item.enriched_sql, sql_service_pb2.SubmitEnrichedSQLRequest)
            facts, analysis = node_facts(sub, None, {})
            sql_hash = text_hash(normalize_runtime_values(sub.sql))
            seed = False
        elif item.HasField("values"):
            sub = _convert(item.values, sql_service_pb2.SubmitValuesRequest)
            facts, analysis, sql_hash, seed = seed_facts(sub, None), None, None, True
        else:
            return None
        if not sub.HasField("dbt_node_state") or not sub.target_table:
            return None
        state = sub.dbt_node_state
        pending = _Pending(
            facts=facts,
            analysis=analysis,
            node_hashes=v2_node_hashes(state, sub.target_table),
            decision=Decision(BUILD, "full_refresh"),
            started_ms=started,
            invocation=invocation,
            project=state.project_name or self.project_name,
            target=state.target_name,
            node_key=normalize(sub.target_table),
            unique_id=state.node_unique_id,
            resource_type=state.resource_type or ("seed" if seed else "model"),
            text_hash=sql_hash,
        )
        epoch = outcome.last_modified_epoch if outcome.HasField("last_modified_epoch") else None
        return self._record(
            pending,
            BUILD,
            "success",
            data_version=str(epoch) if epoch is not None else None,
            data_changed_at_ms=epoch,
            built_at_ms=started,
        )

    def _explain(self, req: Any, out_cls: Any, context: Any) -> Any:
        with self._lock:
            found = [self._explained.get(i) for i in req.execution_decision_ids]
        return out_cls(messages=[item[1] for item in found if item is not None])

    def _telemetry(self, req: Any, out_cls: Any, context: Any) -> Any:
        for event in req.events:
            if event.HasField("session_start"):
                self._session_started(event.session_start, context)
        return out_cls(success=True)

    def _session_start(self, req: Any, out_cls: Any, context: Any) -> Any:
        self._session_started(req, context)
        return out_cls(success=True)

    def _session_started(self, start: Any, context: Any) -> None:
        """dbt's sessionStart config names the run's defer target (`defer_to`)."""
        fields = start.config.fields
        value = fields["defer_to"] if "defer_to" in fields else None
        defer_to = value.string_value if value is not None else ""
        self._update_session(_invocation_id(context), defer_to=defer_to or None)

    def _select(self, req: Any, out_cls: Any, context: Any) -> Any:
        """The nodes matching a `state:` criterion, compared with the base target's latest
        records. Anything that stops the comparison fails the run with a gRPC status."""
        failed = grpc.StatusCode.FAILED_PRECONDITION
        if self.project_id is not None and req.project_id and req.project_id != self.project_id:
            raise _Abort(
                failed,
                f"dbt-hodor serves project id {self.project_id!r}; "
                f"the selector asks for {req.project_id!r}",
            )
        wanted = criterion(req.selector_criteria)
        if wanted is None:
            raise _Abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"dbt-hodor cannot answer the state selector criteria {req.selector_criteria}",
            )
        defer_to = self._session(_invocation_id(context)).defer_to
        # dbt 2.0.8 sends its defer_to as target
        if req.target and defer_to and req.target != defer_to:
            # one of them is not the defer target, and comparing with the wrong one would
            # select the wrong models
            raise _Abort(
                failed,
                f"dbt-hodor cannot tell the base target: the selector names {req.target!r}, "
                f"the session defers to {defer_to!r}",
            )
        base = req.target or defer_to
        if not base:
            raise _Abort(failed, "dbt-hodor cannot compare state: there is no base target")
        records = self._selection_records((self.project_name, base))
        current = {node.node_unique_id: v2_node_hashes(node, "") for node in req.nodes}
        key = SUB_CRITERIA.get(wanted)
        if key is not None and current and all(h.get(key) is None for h in current.values()):
            # dbt sent no such hash (2.0.8: macros, persisted descriptions, and the relation
            # of the base target): an empty answer would be a guess
            raise _Abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"dbt-hodor cannot compare state:{wanted}: dbt sends no hash for it",
            )
        chosen = select(wanted, current, _by_unique_id(records), "v2")
        return out_cls(node_unique_ids=sorted(chosen))

    def _selection_records(self, key: tuple[str, str]) -> list[Record]:
        """The latest successful or reuse record per node of a target, read from the store for
        each selection, so runs other processes wrote since the server started count."""
        deadline = time.monotonic() + STORE_WAIT_S

        def unavailable(why: str) -> _Abort:
            return _Abort(
                grpc.StatusCode.UNAVAILABLE, f"dbt-hodor cannot compare with {key[1]}: {why}"
            )

        slow = unavailable(f"the state store did not answer in {STORE_WAIT_S:g} s")
        if not self._store_ready.wait(STORE_WAIT_S):
            raise slow
        store = self._store
        if store is None:
            raise unavailable(self._store_error or "no state store")
        since = now_ms() - self.lookback_days * DAY_MS
        found: dict[str, Any] = {}
        with self._lock:  # before the read, as in `_load`
            waiting = [
                r
                for r in self._unflushed.values()
                if (r.project_name, r.target_name) == key and r.status in _SELECTION_STATUSES
            ]

        def read() -> None:
            try:
                found["records"] = store.load_latest(key[0], key[1], since, reused=True)
            except Exception as exc:  # noqa: BLE001 - the selection fails below
                found["error"] = exc

        reader = threading.Thread(target=read, name="hodor-select", daemon=True)
        with self._lock:
            self._threads = [t for t in self._threads if t.is_alive()] + [reader]
        reader.start()
        reader.join(max(0.0, deadline - time.monotonic()))
        if reader.is_alive():
            raise slow
        if "error" in found:
            log.warning("dbt-hodor: cannot load records of %s/%s: %s", *key, found["error"])
            raise unavailable(f"cannot read records: {found['error']}")
        records: dict[str, Record] = dict(found["records"])
        for record in waiting:  # records this server wrote that the store may not hold yet
            if _newer(records.get(record.node_key), record):
                records[record.node_key] = record
        return list(records.values())

    def _defer(self, req: Any, out_cls: Any, context: Any) -> Any:
        """The base target's relation of each node, where it has a build there. Nothing
        when the base target is unknown, the current one, or its records are not there."""
        session = self._session(_invocation_id(context))
        base = req.target_name or session.defer_to
        if not base or base == session.target:
            return out_cls()
        # dbt asks once per run, and a wrong answer would read the current target's tables
        entry = self._entry((req.project_name or self.project_name, base))
        if not entry.ready.wait(STORE_WAIT_S):
            return out_cls()
        with self._lock:
            if not entry.ok:
                return out_cls()
        with self._lock:
            records = [*entry.builds.values()]
            records += [r for r in entry.latest.values() if r.action == CLONE]
        relations = deferred_relations(
            req.node_unique_ids, _by_unique_id(records), base, session.target or ""
        )
        return out_cls(fqn_by_unique_id=relations)

    def _register_clone(self, req: Any, out_cls: Any, context: Any) -> Any:
        """dbt's own clone (an incremental model missing in dev): accepted, unless clones
        are off or dbt v2 cannot clone on the dialect."""
        if not self.allow_clones:
            return _unable_to_clone(req, "clones_off", out_cls)
        source, target = req.clone_source_table, req.target_table
        if not source or not target or normalize(source) == normalize(target):
            return _unable_to_clone(req, "unsupported", out_cls)
        sql = clone_sql(req.dialect, target, source)
        if sql is None:
            return _unable_to_clone(req, "unsupported", out_cls)
        epoch = (
            req.clone_source_last_modified_epoch
            if req.HasField("clone_source_last_modified_epoch")
            else None
        )
        ready = clone_service_pb2.ReadyToCloneResponse(
            request_id=str(uuid.uuid4()),
            clone_sqls=[sql],
            clone_source=source,
            clone_target=target,
            clone_required_last_modified_epoch=epoch,
            explained_decision=shared_pb2.ExplainedDecision(
                decision=shared_pb2.READY_TO_CLONE, decision_description="clone_registered"
            ),
        )
        return out_cls(ready_to_clone=ready)

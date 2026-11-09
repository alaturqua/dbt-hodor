"""State stores: load the latest build records at run start, append new records."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

from dbt_hodor.core.records import Record

if TYPE_CHECKING:
    from dbt_hodor.core.config import Settings


class StoreError(Exception):
    """Any store failure. Callers fail open: the affected nodes build."""


class StateStore(Protocol):
    def load(
        self, project_name: str, target_name: str, since_ms: int, before_ms: int | None = None
    ) -> dict[str, Record]:
        """Latest successful build record per node_key, recorded in [since_ms, before_ms)."""
        ...

    def load_latest(
        self,
        project_name: str,
        target_name: str,
        since_ms: int,
        any_status: bool = False,
        reused: bool = False,
    ) -> dict[str, Record]:
        """Latest successful record of any action per node_key, recorded at or after since_ms;
        of any status too when `any_status`; reuse records of models too when `reused`."""
        ...

    def invocation(
        self, project_name: str, target_name: str, invocation_id: str | None
    ) -> list[Record]:
        """All records of one invocation in record order; the latest invocation when None."""
        ...

    def append(self, records: Sequence[Record]) -> None: ...

    def prune(self, project_name: str, target_name: str, before_ms: int, delete: bool) -> int:
        """Records of this project and target older than before_ms. Deletes only when asked."""
        ...

    def close(self) -> None: ...


def open_local_store(settings: Settings, project_root: str, read_only: bool = False) -> StateStore:
    """The duckdb or postgres store of these settings: the stores that need no dbt adapter.
    A relative duckdb path is relative to the dbt project."""
    if settings.store == "duckdb":
        from dbt_hodor.store.duckdb import DuckDBStore

        path = settings.path
        if path != ":memory:" and not os.path.isabs(path):
            path = os.path.join(project_root, path)
        return DuckDBStore(path, settings.schema or "main", settings.table, read_only)
    from dbt_hodor.store.postgres import PostgresStore, lakebase_target

    if not (settings.database and settings.schema):  # load_settings turns hodor off otherwise
        raise StoreError("the postgres store needs hodor_database and hodor_schema")
    host, user, credential = settings.host, settings.user, None
    if settings.endpoint:  # Lakebase: the endpoint supplies host, user and the token
        host, user, credential = lakebase_target(settings.endpoint)
    if credential is None:
        password = settings.password or ""
        credential = lambda: password  # noqa: E731 - a plain Postgres password
    if not host:  # load_settings turns hodor off without a host or an endpoint
        raise StoreError("the postgres store needs hodor_endpoint (Lakebase) or hodor_host")
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

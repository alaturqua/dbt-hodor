"""State stores: load the latest build records at run start, append new records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from dbt_hodor.records import Record


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

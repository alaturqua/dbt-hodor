"""State in a table of a Trino catalog (Iceberg or Delta Lake), through dbt's own connection."""

from __future__ import annotations

from dbt_hodor.store.delta import DeltaStore, trino_literal


class TrinoStore(DeltaStore):
    """The Delta store with Trino's quoting, literals and types. The catalog sets the format."""

    _quote = '"'
    _dialect = "trino"

    @staticmethod
    def _literal(value: object) -> str:
        return trino_literal(value)

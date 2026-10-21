"""dbt-hodor: reuse unchanged dbt nodes, rebuild only when code or upstream data changed.

dbt imports every installed `dbt_*` module on every command, so this module must stay
cheap to import: no sqlglot, duckdb or dbt imports at module level.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dbt-hodor")
except PackageNotFoundError:  # running from a source tree that is not installed
    __version__ = "0.0.0"


def __getattr__(name: str) -> object:
    # dbt's plugin manager looks up `plugins`; load the dbt-facing code only then.
    if name == "plugins":
        from dbt_hodor.plugin import HodorPlugin

        return [HodorPlugin]
    raise AttributeError(f"module 'dbt_hodor' has no attribute {name!r}")

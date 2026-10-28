"""dbt-hodor command line: helpers that run outside dbt."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import Any

from dbt_hodor.config import IDENT_RE, WAREHOUSE_STORES
from dbt_hodor.records import create_table_sql


def _ident(value: str) -> str:
    if not IDENT_RE.match(value):
        raise argparse.ArgumentTypeError(f"invalid name part {value!r}")
    return value


def _days(value: str) -> int:
    if not value.isdigit():
        raise argparse.ArgumentTypeError(f"expected 0 or more days, got {value!r}")
    return int(value)


def _ddl(args: argparse.Namespace) -> str:
    from dbt_hodor.store.delta import quote_table

    if args.store in WAREHOUSE_STORES:
        if not args.database or not args.schema:
            raise SystemExit(
                f"dbt-hodor ddl: --database and --schema are required for {args.store}"
            )
        quote = "`" if args.store == "delta" else '"'
        table = quote_table(args.database, args.schema, args.table, quote=quote)
    elif args.store == "postgres" and not args.schema:
        raise SystemExit("dbt-hodor ddl: --schema is required for postgres")
    else:
        table = ".".join(f'"{p}"' for p in (args.schema or "main", args.table))
    return create_table_sql(table, args.store) + ";"


def _open_store(
    args: argparse.Namespace, command: str = "explain", read_only: bool = True
) -> tuple[Any, Any, str, str]:
    """The store of the project and target, found and rendered the way dbt does it."""
    from dbt.adapters.factory import get_adapter, register_adapter
    from dbt.config.runtime import RuntimeConfig, load_profile, load_project
    from dbt.flags import get_flags, set_from_args
    from dbt.mp_context import get_mp_context

    from dbt_hodor.config import load_settings
    from dbt_hodor.runtime import _profile_settings, open_store
    from dbt_hodor.store import StoreError

    # unset options keep dbt's defaults and DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_TARGET
    given = {k: getattr(args, k) for k in ("project_dir", "profiles_dir", "profile", "target")}
    set_from_args(argparse.Namespace(**{k: v for k, v in given.items() if v}), None)
    flags = get_flags()
    profile = load_profile(str(flags.PROJECT_DIR), {}, flags.PROFILE, flags.TARGET, None)
    project = load_project(str(flags.PROJECT_DIR), False, profile, {}, require_vars=False)
    config = RuntimeConfig.from_parts(project, profile, flags)
    settings = load_settings(
        _profile_settings(config),
        os.environ,
        lambda m: print(f"dbt-hodor: {m}", file=sys.stderr),
        adapter_type=config.credentials.type,
    )
    if settings.store in WAREHOUSE_STORES and not (settings.database and settings.schema):
        raise SystemExit(
            f"dbt-hodor {command}: the {settings.store} store needs hodor_database and hodor_schema"
        )
    adapter = None
    if settings.store in WAREHOUSE_STORES:  # the state table is read through dbt's own connection
        register_adapter(config, get_mp_context())
        adapter = get_adapter(config)
    try:
        store = open_store(settings, config, adapter, read_only=read_only)
    except StoreError as exc:
        raise SystemExit(f"dbt-hodor {command}: {exc}") from exc
    return settings, store, config.project_name, config.target_name


def _explain(args: argparse.Namespace) -> str:
    from dbt_hodor.explain import render, render_summary, report, summarize
    from dbt_hodor.runtime import DAY_MS
    from dbt_hodor.store import StoreError

    settings, store, project, target = _open_store(args)
    which = f"invocation {args.invocation}" if args.invocation else "any invocation"
    try:
        records = store.invocation(project, target, args.invocation)
        if not records:
            raise SystemExit(f"dbt-hodor explain: no records for {project}/{target}, {which}")
        start = records[0].recorded_at_ms
        # the same window the run loaded at its start
        since = start - settings.lookback_days * DAY_MS
        last_builds = store.load(project, target, since, before_ms=start)
    except StoreError as exc:
        raise SystemExit(f"dbt-hodor explain: {exc}") from exc
    finally:
        store.close()
    rep = report(records, last_builds, args.select or ())
    if not rep["nodes"]:
        raise SystemExit(
            f"dbt-hodor explain: no records for {', '.join(args.select)} in invocation "
            f"{rep['invocation_id']}; pass --invocation for an earlier run"
        )
    if args.summary:
        summary = summarize(rep)
        return json.dumps(summary, indent=2) if args.json else render_summary(summary)
    return json.dumps(rep, indent=2) if args.json else render(rep)


def _prune(args: argparse.Namespace) -> str:
    from dbt_hodor.runtime import DAY_MS
    from dbt_hodor.store import StoreError

    settings, store, project, target = _open_store(args, "prune", read_only=not args.yes)
    keep_days = args.keep_days if args.keep_days is not None else settings.lookback_days
    before_ms = int(time.time() * 1000) - keep_days * DAY_MS
    try:
        count = store.prune(project, target, before_ms, delete=args.yes)
    except StoreError as exc:
        raise SystemExit(f"dbt-hodor prune: {exc}") from exc
    finally:
        store.close()
    where = f"{project}/{target}, records older than {keep_days} days"
    if not args.yes:
        note = "" if count == 0 else "\nRun again with --yes to delete them."
        warn = (
            ""
            if keep_days >= settings.lookback_days
            else (
                f"\nWarning: hodor_lookback_days is {settings.lookback_days}. Keeping only "
                f"{keep_days} days drops records the next run still needs, so those nodes "
                "build once (no_history)."
            )
        )
        return f"Would delete {count} records: {where}.{warn}{note}"
    return f"Deleted {count} records: {where}."


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dbt-hodor")
    commands = parser.add_subparsers(dest="command", required=True)
    ddl = commands.add_parser(
        "ddl", help="print the CREATE TABLE statement for the state table (for manual creation)"
    )
    ddl.add_argument(
        "--store",
        choices=("delta", "duckdb", "postgres", "trino"),
        default="delta",
        help="ddl has no adapter and defaults to delta; pass --store trino for Trino",
    )
    ddl.add_argument("--database", type=_ident, help="catalog (delta, trino)")
    ddl.add_argument("--schema", type=_ident)
    ddl.add_argument("--table", type=_ident, default="hodor_state")
    explain = commands.add_parser(
        "explain",
        help="show why each node of the latest (or a given) invocation was reused or built",
    )
    explain.add_argument(
        "-s",
        "--select",
        action="append",
        metavar="NODE",
        help="unique_id, relation or name; repeat for more nodes",
    )
    explain.add_argument("--invocation", metavar="ID", help="invocation_id (default: the latest)")
    explain.add_argument("--json", action="store_true", help="print JSON")
    explain.add_argument(
        "--summary",
        action="store_true",
        help="counts per reason and the changed upstreams behind the builds",
    )
    explain.add_argument("--project-dir")
    explain.add_argument("--profiles-dir")
    explain.add_argument("--profile")
    explain.add_argument("-t", "--target")
    prune = commands.add_parser(
        "prune",
        help="delete old records of this project and target from the state table "
        "(a dry run unless --yes)",
    )
    prune.add_argument(
        "--keep-days",
        type=_days,
        metavar="N",
        help="keep records of the last N days (default: hodor_lookback_days)",
    )
    prune.add_argument("--yes", action="store_true", help="actually delete; otherwise a dry run")
    prune.add_argument("--project-dir")
    prune.add_argument("--profiles-dir")
    prune.add_argument("--profile")
    prune.add_argument("-t", "--target")
    args = parser.parse_args(argv)
    handlers = {"explain": _explain, "prune": _prune, "ddl": _ddl}
    print(handlers[args.command](args))
    return 0


if __name__ == "__main__":
    sys.exit(main())

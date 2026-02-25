# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Lightweight CLI skeleton for the ETL tool.

Commands:
  - plugins list
  - run <pipeline.yml>
  - validate <pipeline.yml> (stub)

The implementation is intentionally minimal; real pipeline parsing and
execution wiring will be layered in later.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etl.app_logging import configure_app_logger
from etl.db import ensure_database_schema, DatabaseError, get_database_url
from etl.db_sync_queue import apply_tracking_queue
from etl.entrypoint import guarded_entrypoint
from etl.cli_parser import build_parser as build_cli_parser
from etl.cli_cmd.common import emit_error_with_report
from etl.cli_cmd.run import cmd_run
from etl.cli_cmd.datasets import (
    cmd_datasets_list,
    cmd_datasets_show,
    cmd_datasets_store,
    cmd_datasets_get,
    cmd_datasets_dictionary_pr,
)
from etl.cli_cmd.runs import cmd_runs_list, cmd_runs_show
from etl.cli_cmd.plugins import cmd_plugins_list
from etl.cli_cmd.diagnostics import cmd_diagnostics_latest
from etl.cli_cmd.artifacts import cmd_artifacts_enforce
from etl.cli_cmd.ai import cmd_ai_research
from etl.cli_cmd.web import cmd_web
from etl.cli_cmd.sync_db import cmd_sync_db
from etl.cli_cmd.validate import cmd_validate

def _format_database_init_error(exc: DatabaseError) -> str:
    text = str(exc).strip()
    if "not a valid URL" in text:
        return (
            "Database initialization error: ETL_DATABASE_URL is set but invalid. "
            "Use a full DSN like postgresql://user:pass@host:5432/dbname."
        )
    if "DDL directory not found" in text or "No .sql DDL scripts found" in text:
        return f"Database initialization error: {text} Check db/ddl migration files."
    return f"Database initialization error: {text}"


def _auto_apply_local_db_queue() -> None:
    queue_dir = Path(".runs") / "db_sync_inbox" / "pending"
    if not queue_dir.exists():
        return
    if not get_database_url():
        return
    processed_dir = queue_dir.parent / "processed"
    summary = apply_tracking_queue(queue_dir, processed_dir=processed_dir)
    if summary.applied or summary.failed:
        print(
            f"DB queue sync: queued={summary.queued} applied={summary.applied} failed={summary.failed}",
            file=sys.stderr,
        )

def build_parser() -> argparse.ArgumentParser:
    from etl import __version__

    return build_cli_parser(
        version=__version__,
        cmd_plugins_list=cmd_plugins_list,
        cmd_run=cmd_run,
        cmd_runs_list=cmd_runs_list,
        cmd_runs_show=cmd_runs_show,
        cmd_datasets_list=cmd_datasets_list,
        cmd_datasets_show=cmd_datasets_show,
        cmd_datasets_store=cmd_datasets_store,
        cmd_datasets_get=cmd_datasets_get,
        cmd_datasets_dictionary_pr=cmd_datasets_dictionary_pr,
        cmd_diagnostics_latest=cmd_diagnostics_latest,
        cmd_validate=cmd_validate,
        cmd_web=cmd_web,
        cmd_artifacts_enforce=cmd_artifacts_enforce,
        cmd_ai_research=cmd_ai_research,
        cmd_sync_db=cmd_sync_db,
    )


def _main_impl(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args, unknown_args = parser.parse_known_args(argv)
    logger = configure_app_logger()
    if unknown_args:
        print(f"[etl][WARN] ignoring unknown arguments: {' '.join(str(x) for x in unknown_args)}")
        logger.warning("Ignoring unknown CLI arguments: %s", " ".join(str(x) for x in unknown_args))
    args._raw_argv = argv if argv is not None else sys.argv[1:]
    try:
        logger.info("Ensuring database schema from db/ddl")
        ensure_database_schema(Path("db/ddl"))
    except DatabaseError as exc:
        logger.exception("Database initialization failed: %s", exc)
        emit_error_with_report(_format_database_init_error(exc), exc, args)
        return 1
    try:
        logger.info("Applying local DB sync queue if pending")
        _auto_apply_local_db_queue()
    except Exception:
        # Startup queue apply should not block CLI commands.
        pass
    try:
        logger.info("Dispatching command: %s", str(getattr(args, "command", "") or ""))
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled CLI error: %s", exc)
        emit_error_with_report(f"Unhandled error: {exc}", exc, args)
        return 1


def main(argv: list[str] | None = None) -> int:
    return guarded_entrypoint(
        lambda: _main_impl(argv),
        logger_name="cli",
        label="cli",
    )


if __name__ == "__main__":
    raise SystemExit(main())

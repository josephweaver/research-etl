# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import argparse
from typing import Callable

from etl.cli_cmd.ai import register_ai_args
from etl.cli_cmd.artifacts import register_artifacts_args
from etl.cli_cmd.datasets import register_datasets_args
from etl.cli_cmd.diagnostics import register_diagnostics_args
from etl.cli_cmd.plugins import register_plugins_args
from etl.cli_cmd.run import register_run_args
from etl.cli_cmd.runs import register_runs_args
from etl.cli_cmd.sync_db import register_sync_db_args
from etl.cli_cmd.validate import register_validate_args
from etl.cli_cmd.web import register_web_args

CommandHandler = Callable[[argparse.Namespace], int]


def build_parser(
    *,
    version: str,
    cmd_plugins_list: CommandHandler,
    cmd_run: CommandHandler,
    cmd_runs_list: CommandHandler,
    cmd_runs_show: CommandHandler,
    cmd_datasets_list: CommandHandler,
    cmd_datasets_show: CommandHandler,
    cmd_datasets_store: CommandHandler,
    cmd_datasets_get: CommandHandler,
    cmd_datasets_dictionary_pr: CommandHandler,
    cmd_diagnostics_latest: CommandHandler,
    cmd_validate: CommandHandler,
    cmd_web: CommandHandler,
    cmd_artifacts_enforce: CommandHandler,
    cmd_ai_research: CommandHandler,
    cmd_sync_db: CommandHandler,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl", description="ETL pipeline runner")
    parser.add_argument("--version", action="version", version=f"%(prog)s {version}")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_plugins = subparsers.add_parser("plugins", help="Plugin related commands")
    plugins_sub = p_plugins.add_subparsers(dest="plugins_command", required=True)
    register_plugins_args(
        plugins_sub,
        cmd_plugins_list=cmd_plugins_list,
    )

    register_run_args(
        subparsers,
        cmd_run=cmd_run,
    )

    p_runs = subparsers.add_parser("runs", help="Inspect previous runs")
    runs_sub = p_runs.add_subparsers(dest="runs_command", required=True)
    register_runs_args(
        runs_sub,
        cmd_runs_list=cmd_runs_list,
        cmd_runs_show=cmd_runs_show,
    )

    p_datasets = subparsers.add_parser("datasets", help="Inspect registered datasets")
    datasets_sub = p_datasets.add_subparsers(dest="datasets_command", required=True)
    register_datasets_args(
        datasets_sub,
        cmd_datasets_list=cmd_datasets_list,
        cmd_datasets_show=cmd_datasets_show,
        cmd_datasets_store=cmd_datasets_store,
        cmd_datasets_get=cmd_datasets_get,
        cmd_datasets_dictionary_pr=cmd_datasets_dictionary_pr,
    )

    p_diag = subparsers.add_parser("diagnostics", help="Inspect generated diagnostic reports")
    diag_sub = p_diag.add_subparsers(dest="diagnostics_command", required=True)
    register_diagnostics_args(
        diag_sub,
        cmd_diagnostics_latest=cmd_diagnostics_latest,
    )

    register_validate_args(
        subparsers,
        cmd_validate=cmd_validate,
    )

    register_web_args(
        subparsers,
        cmd_web=cmd_web,
    )

    p_artifacts = subparsers.add_parser("artifacts", help="Artifact policy commands")
    artifacts_sub = p_artifacts.add_subparsers(dest="artifacts_command", required=True)
    register_artifacts_args(
        artifacts_sub,
        cmd_artifacts_enforce=cmd_artifacts_enforce,
    )

    p_ai = subparsers.add_parser("ai", help="AI-assisted research/documentation helpers")
    ai_sub = p_ai.add_subparsers(dest="ai_command", required=True)
    register_ai_args(
        ai_sub,
        cmd_ai_research=cmd_ai_research,
    )

    register_sync_db_args(
        subparsers,
        cmd_sync_db=cmd_sync_db,
    )

    return parser

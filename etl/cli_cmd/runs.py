# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Runs CLI command group.

High-level idea:
- Provide read-only inspection commands for historical run tracking records.
- Commands print concise summaries and per-run details from the run store.
"""

import argparse
import sys
from pathlib import Path

from etl.cli_cmd.common import CommandHandler
from etl.tracking import find_run, load_runs


def register_runs_args(
    runs_sub: argparse._SubParsersAction,
    *,
    cmd_runs_list: CommandHandler,
    cmd_runs_show: CommandHandler,
) -> None:
    # CLI inputs: runs list/show options and positional run id for show.
    # CLI output: run summaries or a detailed single-run report.
    p_runs_list = runs_sub.add_parser("list", help="List recorded runs")
    p_runs_list.add_argument("--store", default=".runs/runs.jsonl", help="Run record store path")
    p_runs_list.add_argument("-n", "--num", type=int, default=10, help="Number of runs to show")
    p_runs_list.set_defaults(func=cmd_runs_list)

    p_runs_show = runs_sub.add_parser("show", help="Show details for a run")
    p_runs_show.add_argument("run_id", help="Run ID to show")
    p_runs_show.add_argument("--store", default=".runs/runs.jsonl", help="Run record store path")
    p_runs_show.set_defaults(func=cmd_runs_show)


def cmd_runs_list(args: argparse.Namespace) -> int:
    """Print recent run records.

    Inputs:
    - `args.store`: path to run JSONL store.
    - `args.num`: number of trailing records to print.

    Outputs:
    - stdout: run summary lines.
    - return code: `0` (also when no records are found).
    """
    records = load_runs(Path(args.store))
    if not records:
        print("No run records found.")
        return 0
    for rec in records[-args.num :]:
        print(f"{rec.run_id} | {rec.status} | {rec.pipeline} | {rec.started_at}")
    return 0


def cmd_runs_show(args: argparse.Namespace) -> int:
    """Print detailed information for one run.

    Inputs:
    - `args.store`: path to run JSONL store.
    - `args.run_id`: run identifier to lookup.

    Outputs:
    - stdout: run metadata and step status lines when found.
    - stderr: not-found message.
    - return code: `0` on success, `1` when run does not exist.
    """
    rec = find_run(Path(args.store), args.run_id)
    if not rec:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        return 1
    print(f"Run: {rec.run_id}")
    print(f"Status: {rec.status} (success={rec.success})")
    print(f"Pipeline: {rec.pipeline}")
    print(f"Started: {rec.started_at}")
    print(f"Ended: {rec.ended_at}")
    print("Steps:")
    for step in rec.steps:
        status = "ok" if step["success"] else f"error: {step['error']}"
        print(f"  - {step['name']}: {status}")
    return 0

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Diagnostics CLI command group.

High-level idea:
- Surface generated diagnostic error reports for fast troubleshooting.
- Commands in this group locate and optionally print report contents.
"""

import argparse
import sys
from pathlib import Path

from etl.cli_cmd.common import CommandHandler
from etl.diagnostics import find_latest_error_report


def register_diagnostics_args(
    diag_sub: argparse._SubParsersAction,
    *,
    cmd_diagnostics_latest: CommandHandler,
) -> None:
    # CLI inputs: diagnostics latest [--workdir] [--show].
    # CLI output: latest report path, optionally full report JSON text.
    p_diag_latest = diag_sub.add_parser("latest", help="Print path to most recent diagnostic report")
    p_diag_latest.add_argument("--workdir", default=".runs", help="Base run artifact directory")
    p_diag_latest.add_argument("--show", action="store_true", help="Also print report JSON contents")
    p_diag_latest.set_defaults(func=cmd_diagnostics_latest)


def cmd_diagnostics_latest(args: argparse.Namespace) -> int:
    """Print the latest diagnostic report path and optional body.

    Inputs:
    - `args.workdir`: run workspace root containing `error_reports/`.
    - `args.show`: when true, print report file contents.

    Outputs:
    - stdout: latest report path and optionally report content.
    - stderr: missing-report or read-error messages.
    - return code: `0` on success, `1` on error.
    """
    latest = find_latest_error_report(Path(args.workdir))
    if not latest:
        print(f"No diagnostic reports found under {Path(args.workdir) / 'error_reports'}.", file=sys.stderr)
        return 1
    print(latest)
    if args.show:
        try:
            print(latest.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to read diagnostic report: {exc}", file=sys.stderr)
            return 1
    return 0

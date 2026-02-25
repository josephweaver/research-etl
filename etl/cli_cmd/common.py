# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable, Optional

from etl.diagnostics import write_error_report
from etl.variable_solver import VariableSolver

CommandHandler = Callable[[argparse.Namespace], int]


def emit_error_with_report(message: str, exc: Exception, args: argparse.Namespace) -> None:
    print(message, file=sys.stderr)
    try:
        report_path = write_error_report(
            exc=exc,
            command="etl",
            argv=getattr(args, "_raw_argv", []) or [],
            workdir=Path(".runs"),
            repo_root=Path(".").resolve(),
        )
        print(f"Diagnostic report saved: {report_path}", file=sys.stderr)
    except Exception:
        # Keep user-facing failure path robust even if diagnostics fail.
        pass


def resolve_workdir_from_solver(
    *,
    solver: Optional[VariableSolver],
    pipeline_workdir: str | None = None,
    fallback: str = ".runs",
) -> str:
    """Resolve workdir precedence from an existing solver context."""
    if solver is None:
        return fallback

    cli_workdir = str(solver.get("commandline.workdir", "", resolve=True) or "").strip()
    if cli_workdir:
        return cli_workdir

    pipeline_text = str(pipeline_workdir or "").strip()
    if pipeline_text:
        resolved = solver.resolve(pipeline_text, context=solver.resolved_context())
        text = str(resolved or "").strip()
        if text and "{" not in text and "}" not in text:
            return text

    env_workdir = str(solver.get("env.workdir", "", resolve=True) or "").strip()
    if env_workdir:
        return env_workdir

    global_workdir = str(solver.get("global.workdir", "", resolve=True) or "").strip()
    if global_workdir:
        return global_workdir

    return fallback

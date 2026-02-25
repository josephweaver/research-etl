# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

from etl.diagnostics import write_error_report

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


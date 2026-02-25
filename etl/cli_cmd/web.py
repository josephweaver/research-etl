# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Web CLI command group.

High-level idea:
- Launch the FastAPI-based UI/API server for interactive operations.
- This group currently contains a single command that boots `uvicorn`.
"""

import argparse
import sys

from etl.cli_cmd.common import CommandHandler


def register_web_args(
    subparsers: argparse._SubParsersAction,
    *,
    cmd_web: CommandHandler,
) -> None:
    # CLI inputs: web [--host] [--port] [--reload].
    # CLI output: starts a long-running web process (or prints dependency error).
    p_web = subparsers.add_parser("web", help="Run the minimal web UI/API server")
    p_web.add_argument("--host", default="127.0.0.1", help="Bind host")
    p_web.add_argument("--port", type=int, default=8000, help="Bind port")
    p_web.add_argument("--reload", action="store_true", help="Auto-reload on code changes")
    p_web.set_defaults(func=cmd_web)


def cmd_web(args: argparse.Namespace) -> int:
    """Run the web UI/API server.

    Inputs:
    - `args.host`, `args.port`, `args.reload`: uvicorn server options.

    Outputs:
    - process side-effect: starts uvicorn serving `etl.web_api:app`.
    - stderr: dependency guidance when `uvicorn` is missing.
    - return code: `0` on normal startup path, `1` on import failure.
    """
    try:
        import uvicorn  # type: ignore
    except ImportError:
        print(
            "Web UI requires uvicorn/fastapi. Install with: pip install \"research-etl[web]\"",
            file=sys.stderr,
        )
        return 1
    uvicorn.run("etl.web_api:app", host=args.host, port=args.port, reload=bool(args.reload))
    return 0

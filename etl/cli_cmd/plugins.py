# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Plugins CLI command group.

High-level idea:
- Expose plugin discovery utilities for operators and developers.
- Commands in this group inspect plugin modules and print human-readable summaries.
"""

import argparse
import sys
from pathlib import Path

from etl.cli_cmd.common import CommandHandler
from etl.plugins.base import PluginLoadError, describe_plugin, discover_plugins
DEFAULT_PLUGIN_DIR = Path("plugins")


def register_plugins_args(
    plugins_sub: argparse._SubParsersAction,
    *,
    cmd_plugins_list: CommandHandler,
) -> None:
    # CLI inputs: plugins list [-d/--directory].
    # CLI output: prints discovered plugin metadata to stdout.
    p_plugins_list = plugins_sub.add_parser("list", help="List available plugins")
    p_plugins_list.add_argument(
        "-d", "--directory", help="Directory to search for plugins", default=None
    )
    p_plugins_list.set_defaults(func=cmd_plugins_list)


def cmd_plugins_list(args: argparse.Namespace) -> int:
    """List plugin modules in a directory.

    Inputs:
    - `args.directory`: optional plugins directory path (defaults to `plugins`).

    Outputs:
    - stdout: one line per plugin (`name`, `version`, `description`).
    - stderr: error messages for missing directory or load failures.
    - return code: `0` on success, `1` on error.
    """
    directory = Path(args.directory or DEFAULT_PLUGIN_DIR)
    if not directory.exists():
        print(f"No plugin directory found at {directory}", file=sys.stderr)
        return 1
    try:
        plugins = discover_plugins(directory)
    except PluginLoadError as exc:
        print(f"Error loading plugins: {exc}", file=sys.stderr)
        return 1
    if not plugins:
        print("No plugins discovered.")
        return 0
    for p in plugins:
        info = describe_plugin(p)
        print(f"- {info['name']} ({info['version']}): {info['description']}")
    return 0

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Sync-DB CLI command group.

High-level idea:
- Pull queued tracking updates from remote execution environments and apply them locally.
- Bridges remote/offline tracking writes into the local DB when connectivity allows.
"""

import argparse
import shlex
import sys
from pathlib import Path
from typing import Any, Dict

from etl.app_logging import get_app_logger
from etl.cli_cmd.common import CommandHandler
from etl.config import ConfigError, load_global_config, resolve_global_config_path
from etl.db import get_database_url
from etl.db_sync_queue import apply_tracking_queue
from etl.execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_config_path,
    resolve_execution_env_templates,
)
from etl.subprocess_logging import run_logged_subprocess


def register_sync_db_args(
    subparsers: argparse._SubParsersAction,
    *,
    cmd_sync_db: CommandHandler,
) -> None:
    # CLI inputs: sync-db transfer/apply options and required env selection.
    # CLI output: registers parser for remote queue sync/apply workflow.
    p_sync_db = subparsers.add_parser("sync-db", help="Pull/apply queued DB updates from remote environment")
    p_sync_db.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_sync_db.add_argument(
        "--environments-config",
        help="Path to environments config YAML (default: config/environments.yml)",
        default=None,
    )
    p_sync_db.add_argument("--env", help="Execution environment name (from environments config)", required=True)
    p_sync_db.add_argument("--remote-dir", default=None, help="Remote queue directory override")
    p_sync_db.add_argument("--local-queue-dir", default=".runs/db_sync_inbox/pending", help="Local inbox queue directory")
    p_sync_db.add_argument("--processed-dir", default=".runs/db_sync_inbox/processed", help="Local processed queue directory")
    p_sync_db.add_argument("--apply-only", action="store_true", help="Apply local queue only; do not pull via SSH/SCP")
    p_sync_db.add_argument("--keep-remote", action="store_true", help="Do not delete remote files after successful apply")
    p_sync_db.set_defaults(func=cmd_sync_db)


def cmd_sync_db(args: argparse.Namespace) -> int:
    """Pull/apply queued DB tracking updates for one execution environment.

    Inputs:
    - local/remote queue paths, environment config, and apply/pull toggles.

    Outputs:
    - stdout: sync/apply summary counts.
    - stderr: config/SSH/SCP/apply errors.
    - side effects: SSH/SCP operations and local DB queue application.
    - return code: `0` when all updates apply cleanly, `1` otherwise.
    """
    logger = get_app_logger("cli.sync_db")
    local_queue = Path(args.local_queue_dir)
    processed_dir = Path(args.processed_dir)
    local_queue.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    global_vars: Dict[str, Any] = {}
    try:
        selected_global = resolve_global_config_path(Path(args.global_config) if args.global_config else None)
    except ConfigError as exc:
        print(f"Global config error: {exc}", file=sys.stderr)
        return 1
    if selected_global:
        try:
            global_vars = load_global_config(selected_global)
        except ConfigError as exc:
            print(f"Global config error: {exc}", file=sys.stderr)
            return 1

    try:
        cfg_path = resolve_execution_config_path(Path(args.environments_config) if args.environments_config else None)
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1
    if not cfg_path:
        print("Environments config error: no environments config found.", file=sys.stderr)
        return 1
    if not args.env:
        print("`--env` is required for sync-db.", file=sys.stderr)
        return 1
    try:
        envs = load_execution_config(cfg_path)
        exec_env = envs.get(args.env, {})
        if not exec_env:
            print(f"Execution env '{args.env}' not found in config", file=sys.stderr)
            return 1
        exec_env = apply_execution_env_overrides(exec_env)
        exec_env = resolve_execution_env_templates(exec_env, global_vars=global_vars)
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1

    ssh_host = str(exec_env.get("ssh_host") or "").strip()
    if not ssh_host:
        print(f"Execution env '{args.env}' has no ssh_host; cannot pull remote queue.", file=sys.stderr)
        return 1
    ssh_user = str(exec_env.get("ssh_user") or "").strip()
    ssh_jump = str(exec_env.get("ssh_jump") or "").strip()
    target = f"{ssh_user + '@' if ssh_user else ''}{ssh_host}"
    remote_dir = str(args.remote_dir or exec_env.get("db_sync_queue_dir") or "").strip()
    if not remote_dir:
        workdir = str(exec_env.get("workdir") or "").strip()
        remote_dir = f"{workdir}/db_sync_queue/pending" if workdir else "~/.etl/db_sync_queue/pending"

    ssh_cmd = ["ssh"] + (["-J", ssh_jump] if ssh_jump else []) + [target, f"mkdir -p {shlex.quote(remote_dir)}"]
    proc = run_logged_subprocess(ssh_cmd, logger=logger, action="cli.sync_db.ssh", check=False)
    if proc.returncode != 0:
        print(f"Failed to ensure remote queue dir: {proc.stderr or proc.stdout}", file=sys.stderr)
        return 1

    if not args.apply_only:
        scp_cmd = ["scp"] + (["-J", ssh_jump] if ssh_jump else []) + ["-r", f"{target}:{remote_dir}/.", str(local_queue)]
        proc_pull = run_logged_subprocess(scp_cmd, logger=logger, action="cli.sync_db.scp_pull", check=False)
        if proc_pull.returncode != 0:
            stderr_text = (proc_pull.stderr or "").strip()
            if "No such file or directory" not in stderr_text and "not a regular file" not in stderr_text:
                print(f"Failed to pull remote queue: {stderr_text or proc_pull.stdout}", file=sys.stderr)
                return 1

    if not get_database_url():
        print("ETL_DATABASE_URL is not set locally; cannot apply pulled queue.", file=sys.stderr)
        return 1

    before = {p.name for p in processed_dir.glob("*.json")}
    summary = apply_tracking_queue(local_queue, processed_dir=processed_dir)
    after = {p.name for p in processed_dir.glob("*.json")}
    new_processed = sorted(after - before)

    if new_processed and not args.keep_remote:
        quoted = " ".join(shlex.quote(str((Path(remote_dir) / name).as_posix())) for name in new_processed)
        rm_cmd = ["ssh"] + (["-J", ssh_jump] if ssh_jump else []) + [target, f"rm -f {quoted}"]
        run_logged_subprocess(rm_cmd, logger=logger, action="cli.sync_db.remote_rm", check=False)

    print(
        f"DB queue sync complete: queued={summary.queued} applied={summary.applied} failed={summary.failed} local_queue={local_queue}"
    )
    return 0 if summary.failed == 0 else 1

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
SLURM executor: submits the whole pipeline as a single SLURM job.

This first-cut executor runs the full pipeline inside one SLURM job using
the local runner on the compute node. It uses execution environment
settings provided via `--environments-config` / `--env`.

Future enhancement: expand batches/foreach into job arrays with dependencies.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import math
import re
from datetime import datetime
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse

from ..base import Executor, RunState, RunStatus, SubmissionResult
from ...common.parsing import parse_bool
from ...git_checkout import GitExecutionSpec
from ...pipeline import Pipeline
from ...pipeline import parse_pipeline
from ...pipeline_assets import pipeline_asset_sources_from_project_vars
from ...plugins.base import load_plugin
from ...provisioners import SlurmProvisioner, WorkloadSpec
from ...source_control import (
    SourceControlError,
    SourceExecutionSpec,
    make_git_source_provider,
    resolve_source_override,
)
from ...subprocess_logging import run_logged_subprocess
from ...tracking import fetch_plugin_resource_stats, upsert_run_status
from ...transports import ExecutionOptions, SshTransport, TransportError
from ...variable_solver import VariableSolver
from .job_spec_builder import SlurmJobSpecBuilder
from .run_spec_builder import SlurmRunSpecBuilder
from .sbatch_setup import render_setup_script
from .sbatch_step import render_step_script
from .sbatch_controller import render_controller_script

_SOURCE_PROVIDER = make_git_source_provider()


def _to_source_spec(spec: SourceExecutionSpec | GitExecutionSpec) -> SourceExecutionSpec:
    if isinstance(spec, SourceExecutionSpec):
        return spec
    return SourceExecutionSpec(
        provider="git",
        revision=str(spec.commit_sha or ""),
        origin_url=spec.origin_url,
        repo_name=spec.repo_name,
        is_dirty=spec.git_is_dirty,
        extra={"commit_sha": str(spec.commit_sha or "")},
    )


def resolve_execution_spec(**kwargs) -> SourceExecutionSpec:
    return _SOURCE_PROVIDER.resolve_execution_spec(**kwargs)


def repo_relative_path(path: Path, repo_root: Path, label: str) -> Path:
    return _SOURCE_PROVIDER.repo_relative_path(path, repo_root, label)


class SlurmSubmitError(RuntimeError):
    """Raised when sbatch submission fails."""


def _parse_slurm_time_to_minutes(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    days = 0
    rest = text
    if "-" in text:
        parts = text.split("-", 1)
        if len(parts) != 2:
            return None
        try:
            days = int(parts[0])
        except ValueError:
            return None
        rest = parts[1]
    toks = rest.split(":")
    try:
        if len(toks) == 3:
            h, m, s = int(toks[0]), int(toks[1]), int(toks[2])
        elif len(toks) == 2:
            h, m, s = 0, int(toks[0]), int(toks[1])
        elif len(toks) == 1:
            h, m, s = 0, int(toks[0]), 0
        else:
            return None
    except ValueError:
        return None
    return float(days * 24 * 60 + h * 60 + m + (s / 60.0))


def _format_minutes_as_slurm_time(minutes: float) -> str:
    total_seconds = int(max(60, round(float(minutes) * 60.0)))
    hours, rem = divmod(total_seconds, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hours:02d}:{mins:02d}:{secs:02d}"


def _flatten_vars_for_cli(values: Dict[str, Any], prefix: str = "") -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for key in sorted(values.keys()):
        k = str(key).strip()
        if not k:
            continue
        full_key = f"{prefix}.{k}" if prefix else k
        raw = values.get(key)
        if isinstance(raw, dict):
            items.extend(_flatten_vars_for_cli(raw, prefix=full_key))
            continue
        items.append((full_key, "" if raw is None else str(raw)))
    return items


def _lookup_ctx_path(ctx: Dict[str, Any], dotted: str) -> tuple[Any, bool]:
    cur: Any = ctx
    for part in str(dotted or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return None, False
    return cur, True


def _parse_mem_to_mb(value: str) -> Optional[int]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    unit = "M"
    number_text = text
    if text[-1].isalpha():
        unit = text[-1]
        number_text = text[:-1]
    try:
        amount = float(number_text.strip())
    except ValueError:
        return None
    if amount < 0:
        return None
    if unit == "K":
        return max(1, int(math.ceil(amount / 1024.0)))
    if unit == "M":
        return max(1, int(math.ceil(amount)))
    if unit == "G":
        return max(1, int(math.ceil(amount * 1024.0)))
    if unit == "T":
        return max(1, int(math.ceil(amount * 1024.0 * 1024.0)))
    return None


def _format_mb_as_slurm_mem(mb: int) -> str:
    value = max(1, int(mb))
    if value % 1024 == 0:
        return f"{value // 1024}G"
    return f"{value}M"

def _parse_step_indices(value: Any, step_count: int) -> list[int]:
    if value is None or value == "":
        return []
    raw_items: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [x.strip() for x in str(value).replace(";", ",").split(",")]
    out: list[int] = []
    seen: set[int] = set()
    for raw in raw_items:
        text = str(raw).strip()
        if not text:
            continue
        try:
            idx = int(text)
        except (TypeError, ValueError) as exc:
            raise SlurmSubmitError(f"Invalid step index in context.step_indices: {text}") from exc
        if idx < 0 or idx >= int(step_count):
            raise SlurmSubmitError(f"Step index out of range in context.step_indices: {idx} (step_count={step_count})")
        if idx in seen:
            continue
        seen.add(idx)
        out.append(idx)
    out.sort()
    return out


def _rewrite_asset_cache_pipeline_rel(pipeline_rel: Path) -> Optional[Path]:
    parts = list(pipeline_rel.parts)
    for idx, part in enumerate(parts):
        if part in {".pipeline_assets_cache", ".pipeline_assets"}:
            try:
                pipe_idx = parts.index("pipelines", idx + 1)
            except ValueError:
                return None
            tail = parts[pipe_idx + 1 :]
            return Path("pipelines", *tail) if tail else Path("pipelines")
    return None


def _git_current_branch(repo_path: Path) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:  # noqa: BLE001
        return None
    if proc.returncode != 0:
        return None
    branch = str(proc.stdout or "").strip()
    if not branch or branch == "HEAD":
        return None
    return branch


def _resolve_pipeline_asset_overlays(
    project_vars: Dict[str, Any],
    repo_root: Path,
    commandline_vars: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    try:
        sources = pipeline_asset_sources_from_project_vars(project_vars)
    except Exception:  # noqa: BLE001
        return []

    overlays: List[Dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    base_root = Path(repo_root).resolve()
    for src in sources:
        repo_url = str(getattr(src, "repo_url", "") or "").strip()
        if not repo_url:
            continue
        ref = str(getattr(src, "ref", "") or "main").strip() or "main"
        local_repo_path = str(getattr(src, "local_repo_path", "") or "").strip()
        if local_repo_path:
            local = Path(local_repo_path).expanduser()
            if not local.is_absolute():
                local = (base_root / local).resolve()
            if local.exists() and local.is_dir():
                local_branch = _git_current_branch(local)
                if local_branch:
                    ref = local_branch
        pipelines_dir = str(getattr(src, "pipelines_dir", "") or "pipelines").strip() or "pipelines"
        scripts_dir = str(getattr(src, "scripts_dir", "") or "scripts").strip() or "scripts"
        key = (repo_url, ref, pipelines_dir, scripts_dir)
        if key in seen:
            continue
        seen.add(key)
        overlays.append(
            {
                "repo_url": repo_url,
                "ref": ref,
                "pipelines_dir": pipelines_dir,
                "scripts_dir": scripts_dir,
            }
        )
    override = resolve_source_override(
        commandline_vars=dict(commandline_vars or {}),
        kind="pipeline",
        fallback=None,
    )
    if override and overlays:
        overlays[0]["repo_url"] = str(override.origin_url or overlays[0].get("repo_url") or "").strip()
        overlays[0]["ref"] = str(override.revision or overlays[0].get("ref") or "").strip() or "main"
    return overlays


@dataclass
class SlurmEnv:
    partition: Optional[str] = None
    account: Optional[str] = None
    time: Optional[str] = None
    cpus_per_task: Optional[int] = None
    mem: Optional[str] = None
    logdir: Optional[str] = None
    workdir: Optional[str] = None
    modules: Optional[list] = None
    conda_env: Optional[str] = None
    sbatch_extra: Optional[list] = None
    ssh_host: Optional[str] = None
    ssh_user: Optional[str] = None
    ssh_jump: Optional[str] = None  # optional ProxyJump/bastion
    remote_repo: Optional[str] = None
    sync: Optional[bool] = False
    venv: Optional[str] = None
    requirements: Optional[str] = None
    python: Optional[str] = None
    step_max_retries: Optional[int] = None
    step_retry_delay_seconds: Optional[float] = None
    max_time: Optional[str] = None
    max_cpus_per_task: Optional[int] = None
    max_mem: Optional[str] = None
    setup_time: Optional[str] = None
    execution_source: Optional[str] = None
    source_bundle: Optional[str] = None
    source_snapshot: Optional[str] = None
    allow_workspace_source: Optional[bool] = False
    git_remote_url: Optional[str] = None
    propagate_db_secret: Optional[bool] = True
    load_secrets_file: Optional[bool] = True
    database_url: Optional[str] = None
    db_tunnel_command: Optional[str] = None
    db_tunnel_via_tmux: Optional[bool] = False
    db_tunnel_session_prefix: Optional[str] = None
    db_tunnel_host: Optional[str] = None
    db_tunnel_port: Optional[int] = None
    db_tunnel_rewrite_database_url: Optional[bool] = None
    ssh_retries: Optional[int] = None
    scp_retries: Optional[int] = None
    remote_retry_delay_seconds: Optional[float] = None
    ssh_connect_timeout: Optional[int] = None
    ssh_strict_host_key_checking: Optional[str] = None


class SlurmExecutor(Executor):
    name = "slurm"

    def __init__(
        self,
        env_config: Dict[str, Any],
        repo_root: Path,
        plugins_dir: Path = Path("plugins"),
        workdir: Path = Path(".runs"),
        global_config: Optional[Path] = None,
        projects_config: Optional[Path] = None,
        environments_config: Optional[Path] = None,
        env_name: Optional[str] = None,
        dry_run: bool = False,
        verbose: bool = False,
        enforce_git_checkout: bool = False,
        require_clean_git: bool = True,
        execution_source: Optional[str] = None,
        source_bundle: Optional[str] = None,
        source_snapshot: Optional[str] = None,
        allow_workspace_source: Optional[bool] = None,
    ):
        self.env_config = dict(env_config or {})
        # filter known SlurmEnv fields
        env_kwargs = {k: v for k, v in env_config.items() if k in {
            "partition", "account", "time", "cpus_per_task", "mem",
            "logdir", "workdir", "modules", "conda_env", "sbatch_extra",
            "ssh_host", "ssh_user", "ssh_jump", "remote_repo", "sync",
            "venv", "requirements", "python", "step_max_retries", "step_retry_delay_seconds",
            "max_time", "max_cpus_per_task", "max_mem", "setup_time",
            "execution_source", "source_bundle", "source_snapshot", "allow_workspace_source",
            "git_remote_url", "propagate_db_secret", "load_secrets_file",
            "database_url", "db_tunnel_command", "db_tunnel_via_tmux", "db_tunnel_session_prefix",
            "db_tunnel_host", "db_tunnel_port", "db_tunnel_rewrite_database_url",
            "ssh_retries", "scp_retries", "remote_retry_delay_seconds",
            "ssh_connect_timeout", "ssh_strict_host_key_checking",
        }}
        self.env = SlurmEnv(**env_kwargs)
        # Limits/concurrency hints; used by future array/dependency planner.
        self.job_limit = int(env_config.get("job_limit", 1000))
        self.array_task_limit = int(env_config.get("array_task_limit", 1000))
        self.max_parallel = int(env_config.get("max_parallel", 50))
        self.ssh_timeout = int(env_config.get("ssh_timeout", 120))
        self.scp_timeout = int(env_config.get("scp_timeout", 300))
        self.ssh_retries = max(0, int(env_config.get("ssh_retries", 2)))
        self.scp_retries = max(0, int(env_config.get("scp_retries", 2)))
        self.remote_retry_delay_seconds = max(0.0, float(env_config.get("remote_retry_delay_seconds", 2.0)))
        self.ssh_connect_timeout = max(1, int(env_config.get("ssh_connect_timeout", min(30, self.ssh_timeout))))
        self.ssh_strict_host_key_checking = str(
            env_config.get("ssh_strict_host_key_checking", "accept-new")
        ).strip()
        self.step_max_retries = int(env_config.get("step_max_retries", 0))
        self.step_retry_delay_seconds = float(env_config.get("step_retry_delay_seconds", 0.0))
        self.resource_low_sample_multiplier = float(env_config.get("resource_low_sample_multiplier", 1.5))
        self.enable_controller_job = bool(env_config.get("enable_controller_job", False))
        self.controller_wait_children = bool(env_config.get("controller_wait_children", True))
        self.controller_poll_seconds = max(2, int(env_config.get("controller_poll_seconds", 10) or 10))
        self.local_repo_name = Path(repo_root).name
        self.remote_base = Path(env_config.get("remote_repo") or repo_root)
        self.repo_root = self.remote_base / self.local_repo_name
        self.plugins_dir = plugins_dir
        self.workdir = workdir
        self.global_config = global_config
        self.projects_config = projects_config
        self.environments_config = environments_config
        self.env_name = env_name
        self.dry_run = dry_run
        self.verbose = verbose
        self.enforce_git_checkout = enforce_git_checkout
        self.require_clean_git = require_clean_git
        self.execution_source = (
            str(execution_source or env_config.get("execution_source") or "auto").strip().lower()
        )
        self.source_bundle = source_bundle or env_config.get("source_bundle")
        self.source_snapshot = source_snapshot or env_config.get("source_snapshot")
        self.propagate_db_secret = parse_bool(env_config.get("propagate_db_secret"), default=True)
        self.load_secrets_file = parse_bool(env_config.get("load_secrets_file"), default=True)
        if allow_workspace_source is None:
            self.allow_workspace_source = parse_bool(env_config.get("allow_workspace_source"), default=False)
        else:
            self.allow_workspace_source = parse_bool(allow_workspace_source, default=False)
        config_database_url = str(env_config.get("database_url") or "").strip()
        self.database_url = config_database_url or self._load_database_url()
        self.db_tunnel_command = str(env_config.get("db_tunnel_command") or "").strip()
        self.db_tunnel_via_tmux = parse_bool(env_config.get("db_tunnel_via_tmux"), default=False)
        self.db_tunnel_session_prefix = (
            str(env_config.get("db_tunnel_session_prefix") or "").strip() or "etl-db-tunnel"
        )
        self.db_tunnel_host = str(env_config.get("db_tunnel_host") or "127.0.0.1").strip() or "127.0.0.1"
        try:
            self.db_tunnel_port = int(env_config.get("db_tunnel_port", 6543) or 6543)
        except (TypeError, ValueError):
            self.db_tunnel_port = 6543
        self.db_tunnel_rewrite_database_url = parse_bool(
            env_config.get("db_tunnel_rewrite_database_url"),
            default=bool(self.db_tunnel_command),
        )
        self._statuses: Dict[str, RunStatus] = {}

    def capabilities(self) -> Dict[str, bool]:
        return {
            "cancel": False,
            "artifact_tree": True,
            "artifact_file": True,
            "query_data": False,
        }

    def _append_db_tunnel_lines(self, lines: list[str]) -> None:
        if not self.db_tunnel_command:
            return
        if self.db_tunnel_via_tmux:
            if self.verbose:
                lines.append("log_step 'starting DB SSH tunnel via tmux'")
            lines.extend(
                [
                    "if ! command -v tmux >/dev/null 2>&1; then echo '[etl][db_tunnel] tmux is required when db_tunnel_via_tmux=true' >&2; exit 1; fi",
                    f"export ETL_DB_TUNNEL_HOST={shlex.quote(str(self.db_tunnel_host or '127.0.0.1'))}",
                    f"export ETL_DB_TUNNEL_PORT_BASE={int(self.db_tunnel_port or 6543)}",
                    "if [ -n \"${SLURM_ARRAY_TASK_ID:-}\" ]; then",
                    "  export ETL_DB_TUNNEL_PORT=$((ETL_DB_TUNNEL_PORT_BASE + SLURM_ARRAY_TASK_ID))",
                    "elif [ -n \"${SLURM_PROCID:-}\" ]; then",
                    "  export ETL_DB_TUNNEL_PORT=$((ETL_DB_TUNNEL_PORT_BASE + SLURM_PROCID))",
                    "else",
                    "  export ETL_DB_TUNNEL_PORT=${ETL_DB_TUNNEL_PORT_BASE}",
                    "fi",
                    f"ETL_DB_TUNNEL_SESSION_PREFIX={shlex.quote(self.db_tunnel_session_prefix)}",
                    "ETL_DB_TUNNEL_SESSION_SUFFIX=\"${SLURM_JOB_ID:-$$}-${SLURM_ARRAY_TASK_ID:-na}-${ETL_DB_TUNNEL_PORT}\"",
                    "ETL_DB_TUNNEL_SESSION=\"${ETL_DB_TUNNEL_SESSION_PREFIX}-${ETL_DB_TUNNEL_SESSION_SUFFIX}\"",
                    "tmux has-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1 && tmux kill-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1 || true",
                    f"ETL_DB_TUNNEL_COMMAND_RAW={shlex.quote(self.db_tunnel_command)}",
                    "ETL_DB_TUNNEL_COMMAND=$(python3 - <<'PY'\n"
                    "import os\n"
                    "cmd = str(os.environ.get('ETL_DB_TUNNEL_COMMAND_RAW') or '').strip()\n"
                    "port = str(os.environ.get('ETL_DB_TUNNEL_PORT') or '6543').strip() or '6543'\n"
                    "needle = '-L 6543:'\n"
                    "if needle in cmd:\n"
                    "    cmd = cmd.replace(needle, f'-L {port}:', 1)\n"
                    "print(cmd)\n"
                    "PY\n"
                    ")",
                    "tmux new-session -d -s \"$ETL_DB_TUNNEL_SESSION\" \"bash -lc \\\"$ETL_DB_TUNNEL_COMMAND\\\"\"",
                    "sleep 1",
                    "if ! tmux has-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1; then echo '[etl][db_tunnel] tmux tunnel session failed to start' >&2; exit 1; fi",
                    "ETL_DB_TUNNEL_READY=0",
                    "for _i in $(seq 1 20); do",
                    "  if python3 - <<'PY' >/dev/null 2>&1",
                    "import os, socket",
                    "host = str(os.environ.get('ETL_DB_TUNNEL_HOST') or '127.0.0.1').strip() or '127.0.0.1'",
                    "port = int(str(os.environ.get('ETL_DB_TUNNEL_PORT') or '6543').strip() or '6543')",
                    "s = socket.socket()",
                    "s.settimeout(0.5)",
                    "rc = s.connect_ex((host, port))",
                    "s.close()",
                    "raise SystemExit(0 if rc == 0 else 1)",
                    "PY",
                    "  then",
                    "    ETL_DB_TUNNEL_READY=1",
                    "    break",
                    "  fi",
                    "  if ! tmux has-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1; then",
                    "    echo '[etl][db_tunnel] tmux tunnel session exited before port became ready' >&2",
                    "    break",
                    "  fi",
                    "  sleep 1",
                    "done",
                    "if [ \"$ETL_DB_TUNNEL_READY\" != \"1\" ]; then",
                    "  echo \"[etl][db_tunnel] tunnel not ready at ${ETL_DB_TUNNEL_HOST:-127.0.0.1}:${ETL_DB_TUNNEL_PORT:-6543}\" >&2",
                    "  tmux capture-pane -pt \"$ETL_DB_TUNNEL_SESSION\" -S -200 >/tmp/etl_db_tunnel_capture.$$ 2>/dev/null || true",
                    "  if [ -f /tmp/etl_db_tunnel_capture.$$ ]; then cat /tmp/etl_db_tunnel_capture.$$ >&2; rm -f /tmp/etl_db_tunnel_capture.$$; fi",
                    "  exit 1",
                    "fi",
                    "_etl_db_tunnel_cleanup(){ tmux has-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1 && tmux kill-session -t \"$ETL_DB_TUNNEL_SESSION\" >/dev/null 2>&1 || true; }",
                    "trap _etl_db_tunnel_cleanup EXIT INT TERM",
                ]
            )
            return
        if self.verbose:
            lines.append("log_step 'starting DB SSH tunnel'")
        lines.append(f"{self.db_tunnel_command}")

    def _append_db_tunnel_database_url_rewrite_lines(self, lines: list[str], *, python_expr: str) -> None:
        if not self.db_tunnel_rewrite_database_url:
            return
        lines.extend(
            [
                f"export ETL_DB_TUNNEL_HOST=${{ETL_DB_TUNNEL_HOST:-{shlex.quote(str(self.db_tunnel_host or '127.0.0.1'))}}}",
                f"export ETL_DB_TUNNEL_PORT=${{ETL_DB_TUNNEL_PORT:-{int(self.db_tunnel_port or 6543)}}}",
                "if [ -n \"${ETL_DATABASE_URL:-}\" ]; then",
                f"  ETL_DATABASE_URL=\"$({python_expr} - <<'PY'\n"
                "import os\n"
                "import sys\n"
                "repo_root = str(os.environ.get('ETL_REPO_ROOT') or os.environ.get('CHECKOUT_ROOT') or '').strip()\n"
                "if repo_root:\n"
                "    sys.path.insert(0, repo_root)\n"
                "from etl.common.db_urls import rewrite_tunneled_database_url\n"
                "raw = str(os.environ.get('ETL_DATABASE_URL') or '').strip()\n"
                "if not raw:\n"
                "    print('')\n"
                "    raise SystemExit(0)\n"
                "host = str(os.environ.get('ETL_DB_TUNNEL_HOST') or '127.0.0.1').strip() or '127.0.0.1'\n"
                "port = int(str(os.environ.get('ETL_DB_TUNNEL_PORT') or '6543').strip() or '6543')\n"
                "print(rewrite_tunneled_database_url(raw, host=host, port=port))\n"
                "PY\n"
                "  )\"",
                "  export ETL_DATABASE_URL",
                "fi",
            ]
        )

    def _ssh_target(self) -> str:
        host = str(self.env.ssh_host or "").strip()
        if not host:
            raise SlurmSubmitError("Remote SSH mode requires ssh_host.")
        user = str(self.env.ssh_user or "").strip()
        return f"{user + '@' if user else ''}{host}"

    def _ssh_transport(
        self,
        *,
        timeout_seconds: Optional[int] = None,
        retries: Optional[int] = None,
    ) -> SshTransport:
        return SshTransport(
            target=self._ssh_target(),
            ssh_jump=self.env.ssh_jump,
            ssh_connect_timeout=self.ssh_connect_timeout,
            ssh_strict_host_key_checking=self.ssh_strict_host_key_checking,
            timeout_seconds=int(timeout_seconds or self.ssh_timeout),
            retries=int(self.ssh_retries if retries is None else retries),
            retry_delay_seconds=self.remote_retry_delay_seconds,
            verbose=self.verbose,
        )

    def _ssh_run(self, argv: List[str], *, timeout: Optional[int] = None, retries: Optional[int] = None) -> subprocess.CompletedProcess:
        try:
            return self._ssh_transport(timeout_seconds=timeout, retries=retries).run_completed(
                argv,
                check=False,
                options=ExecutionOptions(
                    total_timeout_seconds=float(timeout or self.ssh_timeout),
                    stream_output=False,
                ),
            )
        except TransportError as exc:
            raise SlurmSubmitError(str(exc)) from exc

    def _ssh_run_text(self, text: str, *, timeout: Optional[int] = None, retries: Optional[int] = None) -> subprocess.CompletedProcess:
        try:
            return self._ssh_transport(timeout_seconds=timeout, retries=retries).run_text_completed(
                text,
                check=False,
                options=ExecutionOptions(
                    total_timeout_seconds=float(timeout or self.ssh_timeout),
                    stream_output=False,
                ),
            )
        except TransportError as exc:
            raise SlurmSubmitError(str(exc)) from exc

    def _ssh_put_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
    ) -> subprocess.CompletedProcess:
        try:
            return self._ssh_transport(timeout_seconds=timeout or self.scp_timeout, retries=self.scp_retries if retries is None else retries).put_file_completed(
                source,
                destination,
                options=ExecutionOptions(
                    total_timeout_seconds=float(timeout or self.scp_timeout),
                    stream_output=False,
                ),
            )
        except TransportError as exc:
            raise SlurmSubmitError(str(exc)) from exc

    def _slurm_provisioner(self):
        if self.env.ssh_host:
            transport = self._ssh_transport(timeout_seconds=self.ssh_timeout, retries=self.ssh_retries)
        else:
            from ...transports import LocalProcessTransport

            transport = LocalProcessTransport()
        return SlurmProvisioner(
            transport,
            dry_run=self.dry_run,
            default_submit_timeout_seconds=float(self.ssh_timeout),
            default_transfer_timeout_seconds=float(self.scp_timeout),
        )

    @staticmethod
    def _group_steps_with_indices(steps: List[Any]) -> List[List[Tuple[int, Any]]]:
        batches: List[List[Tuple[int, Any]]] = []
        i = 0
        while i < len(steps):
            current = steps[i]
            group = [(i, current)]
            if getattr(current, "parallel_with", None):
                key = current.parallel_with
                j = i + 1
                while j < len(steps) and getattr(steps[j], "parallel_with", None) == key:
                    group.append((j, steps[j]))
                    j += 1
                i = j
            else:
                i += 1
            batches.append(group)
        return batches

    @staticmethod
    def _foreach_count_from_pipeline(step: Any, pipeline: Pipeline) -> Optional[int]:
        foreach_key = str(getattr(step, "foreach", "") or "").strip()
        if not foreach_key:
            return None
        local_ctx: Dict[str, Any] = {}
        try:
            local_ctx.update(dict(getattr(pipeline, "vars", {}) or {}))
            local_ctx.update(dict(getattr(pipeline, "dirs", {}) or {}))
        except Exception:  # noqa: BLE001
            local_ctx = {}
        value, ok = _lookup_ctx_path(local_ctx, foreach_key)
        if not ok or not isinstance(value, (list, tuple)):
            return None
        return len(value)

    def _foreach_array_max_concurrency(self, step: Any) -> Optional[int]:
        resources = dict(getattr(step, "resources", {}) or {})
        raw = (
            resources.get("foreach_max_concurrency")
            if resources.get("foreach_max_concurrency") not in (None, "")
            else (
                resources.get("max_concurrency")
                if resources.get("max_concurrency") not in (None, "")
                else resources.get("array_max_parallel")
            )
        )
        if raw in (None, ""):
            return None
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        if int(self.max_parallel or 0) > 0:
            value = min(value, int(self.max_parallel))
        return max(1, value)

    @staticmethod
    def _parse_script_plugin_ref(script: str) -> Optional[str]:
        text = str(script or "").strip()
        if not text:
            return None
        try:
            parts = shlex.split(text)
        except Exception:  # noqa: BLE001
            parts = text.split()
        if not parts:
            return None
        return str(parts[0]).strip() or None

    def _resolve_plugin_resources(self, step) -> Dict[str, Any]:
        plugin_ref = self._parse_script_plugin_ref(getattr(step, "script", ""))
        if not plugin_ref:
            return {}
        candidates: List[Path] = []
        ref_path = Path(plugin_ref)
        if ref_path.is_absolute():
            candidates.append(ref_path)
        else:
            candidates.append((self.plugins_dir / ref_path).resolve())
            candidates.append((Path(".").resolve() / self.plugins_dir / ref_path).resolve())
            candidates.append((Path(".").resolve() / ref_path).resolve())
        for path in candidates:
            if not path.exists() or not path.is_file():
                continue
            try:
                plugin = load_plugin(path)
            except Exception:  # noqa: BLE001
                return {}
            out = dict(getattr(plugin.meta, "resources", {}) or {})
            out["plugin_ref"] = plugin_ref
            if getattr(plugin.meta, "name", None) and "plugin_name" not in out:
                out["plugin_name"] = str(plugin.meta.name)
            if getattr(plugin.meta, "version", None) and "plugin_version" not in out:
                out["plugin_version"] = str(plugin.meta.version)
            return out
        return {}

    def _estimate_from_stats(self, stats: Dict[str, Any], key: str) -> Optional[float]:
        samples_raw = stats.get(f"{key}_samples", stats.get("samples"))
        mean_raw = stats.get(f"{key}_mean", stats.get("mean"))
        std_raw = stats.get(f"{key}_std", stats.get("std"))
        if samples_raw in (None, "") or mean_raw in (None, ""):
            return None
        try:
            samples = int(samples_raw)
            mean = float(mean_raw)
        except (TypeError, ValueError):
            return None
        if samples <= 0:
            return None
        if samples < 5:
            return mean * float(self.resource_low_sample_multiplier)
        try:
            std = float(std_raw or 0.0)
        except (TypeError, ValueError):
            std = 0.0
        return mean + (3.0 * max(0.0, std))

    def _resolve_batch_resources(self, steps: List[Any]) -> Dict[str, Optional[Any]]:
        # Start from environment defaults; each step can only raise these requests.
        requested_time_min = _parse_slurm_time_to_minutes(str(self.env.time or ""))
        requested_cpus = int(self.env.cpus_per_task or 0) or None
        requested_mem_mb = _parse_mem_to_mb(str(self.env.mem or ""))

        max_time_min = _parse_slurm_time_to_minutes(str(self.env.max_time or ""))
        max_cpus = int(self.env.max_cpus_per_task or 0) or None
        max_mem_mb = _parse_mem_to_mb(str(self.env.max_mem or ""))

        history_map = {}
        try:
            history_map = dict(getattr(self, "env_config", {}) or {})
        except Exception:  # noqa: BLE001
            history_map = {}
        history_map = dict(history_map.get("resource_history") or {})

        for step in steps:
            # Merge static plugin metadata + explicit step.resources for sizing.
            plugin_resources = self._resolve_plugin_resources(step)
            step_resources = dict(getattr(step, "resources", {}) or {})
            merged = dict(plugin_resources)
            merged.update(step_resources)

            plugin_name = str(plugin_resources.get("plugin_name") or "").strip()
            plugin_version = str(plugin_resources.get("plugin_version") or "").strip()
            plugin_ref = str(plugin_resources.get("plugin_ref") or "").strip()
            hist_key = f"{plugin_name}@{plugin_version}" if plugin_name and plugin_version else ""
            stats = dict(history_map.get(hist_key) or {})
            # Runtime history from DB can override config-based defaults when available.
            db_stats = fetch_plugin_resource_stats(
                plugin_name=plugin_name,
                plugin_version=plugin_version,
                plugin_refs=[plugin_ref] if plugin_ref else [],
                executor="slurm",
                limit=200,
            )
            if db_stats:
                stats = db_stats

            cpus_raw = merged.get("cpus_per_task", merged.get("cpu_cores"))
            if cpus_raw not in (None, ""):
                try:
                    cpus = int(cpus_raw)
                    if cpus > 0:
                        requested_cpus = max(requested_cpus or 0, cpus)
                except (TypeError, ValueError):
                    pass
            else:
                est_cpus = self._estimate_from_stats(stats, "cpu_cores")
                if est_cpus is not None and est_cpus > 0:
                    requested_cpus = max(requested_cpus or 0, int(math.ceil(est_cpus)))

            mem_raw = merged.get("mem")
            if mem_raw in (None, "") and merged.get("memory_gb") not in (None, ""):
                try:
                    mem_raw = f"{float(merged.get('memory_gb'))}G"
                except (TypeError, ValueError):
                    mem_raw = None
            mem_mb = _parse_mem_to_mb(str(mem_raw or ""))
            if mem_mb is not None:
                requested_mem_mb = max(requested_mem_mb or 0, mem_mb)
            else:
                est_mem_gb = self._estimate_from_stats(stats, "memory_gb")
                if est_mem_gb is not None and est_mem_gb > 0:
                    requested_mem_mb = max(requested_mem_mb or 0, int(math.ceil(est_mem_gb * 1024.0)))

            wall_minutes = None
            wm_raw = merged.get("wall_minutes")
            if wm_raw not in (None, ""):
                try:
                    wall_minutes = float(wm_raw)
                except (TypeError, ValueError):
                    wall_minutes = None
            if wall_minutes is None and merged.get("time") not in (None, ""):
                wall_minutes = _parse_slurm_time_to_minutes(str(merged.get("time") or ""))
            if wall_minutes is None:
                wall_minutes = self._estimate_from_stats(stats, "wall_minutes")
            if wall_minutes is not None and wall_minutes > 0:
                requested_time_min = max(requested_time_min or 0.0, wall_minutes)

        if max_cpus is not None and requested_cpus is not None:
            requested_cpus = min(requested_cpus, max_cpus)
        if max_mem_mb is not None and requested_mem_mb is not None:
            requested_mem_mb = min(requested_mem_mb, max_mem_mb)
        if max_time_min is not None and requested_time_min is not None:
            requested_time_min = min(requested_time_min, max_time_min)

        return {
            "time": _format_minutes_as_slurm_time(requested_time_min) if requested_time_min is not None else None,
            "cpus_per_task": requested_cpus,
            "mem": _format_mb_as_slurm_mem(requested_mem_mb) if requested_mem_mb is not None else None,
        }

    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        try:
            run_spec = SlurmRunSpecBuilder(
                self,
                parse_pipeline_fn=parse_pipeline,
                resolve_execution_spec_fn=resolve_execution_spec,
                repo_relative_path_fn=repo_relative_path,
            ).build(pipeline_path, context)
        except SlurmSubmitError:
            raise
        except Exception as exc:
            raise SlurmSubmitError(str(exc)) from exc
        pipeline = run_spec.pipeline
        batches = run_spec.batches
        run_id = run_spec.run_id
        if not batches:
            self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message="No selected steps to run.")
            return SubmissionResult(run_id=run_id, message="No selected steps to run.")
        submission_records = []
        submitted_step_jobs = 0
        prev_jobid = None
        ts = run_spec.created_at
        provenance = dict(run_spec.provenance)
        resume_run_id = run_spec.selection.resume_run_id
        run_started_at = run_spec.selection.run_started_at or (ts.isoformat() + "Z")
        remote_workdir = run_spec.paths.workdir
        remote_workdir_root = Path(remote_workdir)
        child_jobs_file = run_spec.paths.child_jobs_file
        context_file = run_spec.paths.context_file
        checkout_root = run_spec.paths.checkout_root
        pipeline_remote = run_spec.paths.pipeline_path
        plugins_remote = run_spec.paths.plugins_dir
        global_config_remote = run_spec.paths.global_config_path
        projects_config_remote = run_spec.paths.projects_config_path
        environments_config_remote = run_spec.paths.environments_config_path
        venv_path = run_spec.paths.venv_path
        req_path = run_spec.paths.requirements_path
        python_bin = run_spec.paths.python_bin
        base_logdir = Path(run_spec.paths.base_logdir)
        setup_logdir = run_spec.paths.setup_logdir
        workdirs_to_create = list(run_spec.paths.workdirs_to_create)
        logdirs_to_create = list(run_spec.paths.logdirs_to_create)
        source_bundle = run_spec.metadata.get("source_bundle_path")
        source_snapshot = run_spec.metadata.get("source_snapshot_path")
        selected_source_mode = str(run_spec.metadata.get("selected_source_mode") or run_spec.etl_source.mode or "workspace")
        allow_workspace_source = bool(run_spec.metadata.get("allow_workspace_source", False))
        commandline_ns = dict(run_spec.commandline_vars)
        pipeline_path = run_spec.pipeline_path_input
        pipeline_asset_overlays = [
            {
                "repo_url": asset.repo_url,
                "ref": asset.revision,
                "pipelines_dir": asset.pipelines_dir,
                "scripts_dir": asset.scripts_dir,
            }
            for asset in run_spec.pipeline_assets
        ]

        if self.env.ssh_host and self.env.sync and not self.enforce_git_checkout:
            self._sync_repo()
        source_bundle_remote = self._stage_source_asset(
            source_bundle, remote_workdir, run_id=run_id, label="source-bundle"
        )
        source_snapshot_remote = self._stage_source_asset(
            source_snapshot, remote_workdir, run_id=run_id, label="source-snapshot"
        )

        planned_jobs = SlurmJobSpecBuilder(self).build(
            run_spec,
            source_bundle_path=source_bundle_remote,
            source_snapshot_path=source_snapshot_remote,
        )
        job_id_map: Dict[str, str] = {}
        for job_spec in planned_jobs:
            prev_dependencies = [job_id_map[dep] for dep in job_spec.dependencies if dep in job_id_map]
            prev_dependency = prev_dependencies[-1] if prev_dependencies else None
            jobid = self._submit_script(
                job_spec.script_text or "",
                run_id,
                label=str(job_spec.backend_options.get("label") or job_spec.name or job_spec.job_id),
                prev_dependency=prev_dependency,
                array_bounds=job_spec.backend_options.get("array_bounds"),
                remote_dest_dir=job_spec.backend_options.get("remote_dest_dir"),
            )
            job_id_map[job_spec.job_id] = jobid
            submission_records.append(jobid)
            if job_spec.kind == "step":
                submitted_step_jobs += 1

        if pipeline.steps and submitted_step_jobs == 0:
            raise SlurmSubmitError(
                "SLURM submit produced no step jobs for a non-empty pipeline. "
                "Check step parsing and batch expansion."
            )

        controller_jobs = 1 if self.enable_controller_job else 0
        status = RunStatus(
            run_id=run_id,
            state=RunState.QUEUED,
            message=(
                f"submitted {len(submission_records)} jobs "
                f"(setup=1, steps={submitted_step_jobs}, controller={controller_jobs})"
            ),
        )
        self._statuses[run_id] = status
        upsert_run_status(
            run_id=run_id,
            pipeline=pipeline_path,
            project_id=run_spec.project_id,
            status="queued",
            success=False,
            started_at=ts.isoformat() + "Z",
            ended_at=ts.isoformat() + "Z",
            message=status.message,
            executor=self.name,
            artifact_dir=remote_workdir,
            provenance=provenance,
            event_type="run_queued",
            event_details={"job_ids": submission_records},
        )
        return SubmissionResult(
            run_id=run_id,
            backend_run_id=",".join(submission_records),
            job_ids=submission_records,
            message=status.message,
        )

    def status(self, run_id: str) -> RunStatus:
        # Simple cache; future: query sacct/squeue with stored job id.
        return self._statuses.get(run_id, RunStatus(run_id=run_id, state=RunState.FAILED, message="unknown run_id"))

    def artifact_tree(self, artifact_dir: str) -> Dict[str, Any]:
        # For now, support local-visible artifact dirs only.
        # Remote SSH browsing can be added as a later enhancement.
        root = Path(artifact_dir).expanduser()
        if not root.is_absolute():
            root = (Path(".").resolve() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise RuntimeError(
                f"SLURM artifact directory not found locally: {root}. "
                "If this is a remote cluster path, add remote artifact retrieval support."
            )

        def walk(path: Path, rel: str) -> Dict[str, Any]:
            if path.is_dir():
                children = []
                for child in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
                    child_rel = f"{rel}/{child.name}" if rel else child.name
                    children.append(walk(child, child_rel))
                return {"name": path.name or ".", "path": rel, "type": "dir", "children": children}
            size = path.stat().st_size if path.exists() else 0
            return {"name": path.name, "path": rel, "type": "file", "size": size}

        return walk(root, "")

    def artifact_file(self, artifact_dir: str, relative_path: str, max_bytes: int = 256 * 1024) -> Dict[str, Any]:
        root = Path(artifact_dir).expanduser()
        if not root.is_absolute():
            root = (Path(".").resolve() / root).resolve()
        rel = (relative_path or "").strip().lstrip("/").replace("\\", "/")
        candidate = (root / rel).resolve()
        if root not in candidate.parents and candidate != root:
            raise RuntimeError("Invalid artifact path")
        if not candidate.exists() or not candidate.is_file():
            raise RuntimeError(f"Artifact file not found: {relative_path}")
        data = candidate.read_bytes()[:max_bytes]
        text = data.decode("utf-8", errors="replace")
        truncated = candidate.stat().st_size > max_bytes
        if truncated:
            text += "\n\n...[truncated]"
        return {"path": rel, "content": text, "truncated": truncated}

    # Internal helpers

    def _render_batch_script(
        self,
        run_id: str,
        checkout_root: str,
        pipeline_path: str,
        steps: list,
        step_indices: List[int],
        context_file: str,
        workdir: str,
        plugins_dir: str,
        logdir: str,
        venv_path: str,
        req_path: str,
        python_bin: str,
        project_id: Optional[str] = None,
        resume_run_id: Optional[str] = None,
        run_started_at: Optional[str] = None,
        global_config_path: Optional[str] = None,
        projects_config_path: Optional[str] = None,
        environments_config_path: Optional[str] = None,
        commandline_vars: Optional[Dict[str, Any]] = None,
        child_jobs_file: Optional[str] = None,
        sbatch_time: Optional[str] = None,
        sbatch_cpus_per_task: Optional[int] = None,
        sbatch_mem: Optional[str] = None,
        array_index: bool = False,
        array_count: Optional[int] = None,
        array_max_parallel: Optional[int] = None,
        foreach_from_array: bool = False,
        foreach_item_offset: int = 0,
    ) -> str:
        return render_step_script(
            executor=self,
            run_id=run_id,
            checkout_root=checkout_root,
            pipeline_path=pipeline_path,
            steps=steps,
            step_indices=step_indices,
            context_file=context_file,
            workdir=workdir,
            plugins_dir=plugins_dir,
            logdir=logdir,
            venv_path=venv_path,
            req_path=req_path,
            python_bin=python_bin,
            project_id=project_id,
            resume_run_id=resume_run_id,
            run_started_at=run_started_at,
            global_config_path=global_config_path,
            projects_config_path=projects_config_path,
            environments_config_path=environments_config_path,
            commandline_vars=commandline_vars,
            child_jobs_file=child_jobs_file,
            sbatch_time=sbatch_time,
            sbatch_cpus_per_task=sbatch_cpus_per_task,
            sbatch_mem=sbatch_mem,
            array_index=array_index,
            array_count=array_count,
            array_max_parallel=array_max_parallel,
            foreach_from_array=foreach_from_array,
            foreach_item_offset=foreach_item_offset,
            flatten_vars_for_cli=_flatten_vars_for_cli,
        )
        

    def _render_controller_script(
        self,
        *,
        run_id: str,
        workdir: str,
        logdir: str,
        child_jobs_file: str,
        wait_children: bool,
        poll_seconds: int,
    ) -> str:
        return render_controller_script(
            executor=self,
            run_id=run_id,
            workdir=workdir,
            logdir=logdir,
            child_jobs_file=child_jobs_file,
            wait_children=wait_children,
            poll_seconds=poll_seconds,
        )

    def _render_setup_script(
        self,
        run_id: str,
        checkout_root: str,
        workdir: str,
        logdir: str,
        venv_path: str,
        req_path: str,
        python_bin: str,
        workdirs_to_create: List[str],
        logdirs_to_create: List[str],
        execution_source: str = "auto",
        git_origin_url: Optional[str] = None,
        git_commit_sha: Optional[str] = None,
        source_bundle_path: Optional[str] = None,
        source_snapshot_path: Optional[str] = None,
        allow_workspace_source: bool = False,
        pipeline_asset_overlays: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        return render_setup_script(
            executor=self,
            run_id=run_id,
            checkout_root=checkout_root,
            workdir=workdir,
            logdir=logdir,
            venv_path=venv_path,
            req_path=req_path,
            python_bin=python_bin,
            workdirs_to_create=workdirs_to_create,
            logdirs_to_create=logdirs_to_create,
            execution_source=execution_source,
            git_origin_url=git_origin_url,
            git_commit_sha=git_commit_sha,
            source_bundle_path=source_bundle_path,
            source_snapshot_path=source_snapshot_path,
            allow_workspace_source=allow_workspace_source,
            pipeline_asset_overlays=pipeline_asset_overlays or [],
            parse_slurm_time_to_minutes=_parse_slurm_time_to_minutes,
            parse_mem_to_mb=_parse_mem_to_mb,
            format_minutes_as_slurm_time=_format_minutes_as_slurm_time,
            format_mb_as_slurm_mem=_format_mb_as_slurm_mem,
        )
        

    def _stage_source_asset(self, asset_path: Optional[str], remote_workdir: str, run_id: str, label: str) -> Optional[str]:
        if not asset_path:
            return None
        if not self.env.ssh_host:
            return asset_path
        local_candidate = Path(asset_path).expanduser()
        if not local_candidate.exists() or not local_candidate.is_file():
            # Assume caller provided a remote-visible path.
            return asset_path

        remote_dir = f"{remote_workdir}/source"
        remote_file = f"{remote_dir}/{local_candidate.name}"
        proc = self._ssh_run(
            ["mkdir", "-p", remote_dir],
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
        )
        if proc.returncode != 0:
            raise SlurmSubmitError(proc.stderr or proc.stdout)
        proc2 = self._ssh_put_file(
            local_candidate,
            remote_file,
            timeout=self.scp_timeout,
            retries=self.scp_retries,
        )
        if proc2.returncode != 0:
            raise SlurmSubmitError(proc2.stderr or proc2.stdout)
        return remote_file

    def _submit_script(self, script_text: str, run_id: str, label: str = "job", prev_dependency: Optional[str] = None, array_bounds: Optional[Tuple[int, int]] = None, remote_dest_dir: Optional[str] = None) -> str:
        _ = array_bounds

        if self.dry_run:
            return "dry-run"

        if self.env.ssh_host:
            if self.propagate_db_secret:
                self._ensure_remote_secrets_file(self._ssh_target())
            remote_dir = remote_dest_dir or self.env.remote_repo or "/tmp"
            handle = self._slurm_provisioner().submit(
                WorkloadSpec(
                    name=f"etl-{run_id}-{label}",
                    script_text=script_text,
                    backend_options={
                        "destination_dir": remote_dir,
                        "file_name": f"etl-{run_id}-{label}.sbatch",
                        "dependencies": [prev_dependency] if prev_dependency else [],
                        "submit_timeout_seconds": float(self.ssh_timeout),
                        "transfer_timeout_seconds": float(self.scp_timeout),
                    },
                )
            )
            return handle.backend_run_id or "unknown"
        else:
            handle = self._slurm_provisioner().submit(
                WorkloadSpec(
                    name=f"etl-{run_id}-{label}",
                    script_text=script_text,
                    backend_options={
                        "destination_dir": str(self.workdir),
                        "file_name": f"etl-{label}-{run_id}.sbatch",
                        "dependencies": [prev_dependency] if prev_dependency else [],
                        "submit_timeout_seconds": float(self.ssh_timeout),
                        "transfer_timeout_seconds": float(self.scp_timeout),
                    },
                )
            )
            return handle.backend_run_id or "unknown"

    def _ensure_remote_secrets_file(self, target: str) -> None:
        remote_lines = [
            "set -euo pipefail",
            "mkdir -p \"$HOME/.secrets\"",
            "chmod 700 \"$HOME/.secrets\"",
            "touch \"$HOME/.secrets/etl\"",
            "chmod 600 \"$HOME/.secrets/etl\"",
        ]
        if self.database_url:
            secret_line = f"export ETL_DATABASE_URL={shlex.quote(self.database_url)}"
            remote_lines.extend(
                [
                    f"line={shlex.quote(secret_line)}",
                    "grep -v '^export ETL_DATABASE_URL=' \"$HOME/.secrets/etl\" > \"$HOME/.secrets/etl.tmp\" || true",
                    "printf '%s\\n' \"$line\" >> \"$HOME/.secrets/etl.tmp\"",
                    "mv \"$HOME/.secrets/etl.tmp\" \"$HOME/.secrets/etl\"",
                ]
            )
        else:
            remote_lines.extend(
                [
                    "if ! grep -q '^export ETL_DATABASE_URL=' \"$HOME/.secrets/etl\"; then",
                    "  echo 'Missing ETL_DATABASE_URL in local env and remote ~/.secrets/etl' >&2",
                    "  exit 1",
                    "fi",
                ]
            )
        proc = self._ssh_run_text(
            "\n".join(remote_lines),
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
        )
        if proc.returncode != 0:
            raise SlurmSubmitError(
                "Could not initialize remote DB secret. Set ETL_DATABASE_URL in this shell "
                "or ensure ~/.secrets/etl on the remote host contains "
                "'export ETL_DATABASE_URL=...'.\n"
                + (proc.stderr or proc.stdout)
            )

    @staticmethod
    def _load_database_url() -> Optional[str]:
        raw = os.environ.get("ETL_DATABASE_URL")
        if raw is None:
            # Windows fallback: read persistent User/Machine environment
            # values (e.g., created by `setx`) if current shell does not
            # contain ETL_DATABASE_URL.
            try:
                import winreg  # type: ignore

                for hive, subkey in (
                    (winreg.HKEY_CURRENT_USER, r"Environment"),
                    (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
                ):
                    try:
                        with winreg.OpenKey(hive, subkey) as key:
                            val, _ = winreg.QueryValueEx(key, "ETL_DATABASE_URL")
                            if isinstance(val, str) and val.strip():
                                raw = val
                                break
                    except OSError:
                        continue
            except Exception:
                pass
        if raw is None:
            return None
        value = raw.strip()
        if not value:
            return None
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1].strip()
        if not value:
            return None
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.netloc:
            return None
        return value

    def _sync_repo(self) -> None:
        if not self.env.remote_repo:
            raise SlurmSubmitError("remote_repo must be set to sync code")
        target = self._ssh_target()
        remote_path = f"{target}:{self.env.remote_repo}"
        proc = self._ssh_run(
            ["mkdir", "-p", self.env.remote_repo],
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
        )
        if proc.returncode != 0:
            raise SlurmSubmitError(proc.stderr or proc.stdout)
        # Use scp to sync repo (simple recursive copy)
        transport = self._ssh_transport(timeout_seconds=self.scp_timeout, retries=self.scp_retries)
        scp_cmd = transport._scp_cmd("-r", str(Path(".").resolve()), remote_path)  # transport tree copy is a fast follower
        proc2 = run_logged_subprocess(
            scp_cmd,
            action="slurm.scp_sync_repo",
            timeout=self.scp_timeout,
            check=False,
        )
        if proc2.returncode != 0:
            raise SlurmSubmitError(proc2.stderr or proc2.stdout)

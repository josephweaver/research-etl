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
import tempfile
import math
import time
import re
from datetime import datetime
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import GitExecutionSpec
from ..pipeline import Pipeline
from ..pipeline import parse_pipeline
from ..plugins.base import load_plugin
from ..source_control import SourceControlError, SourceExecutionSpec, make_git_source_provider
from ..tracking import fetch_plugin_resource_stats, upsert_run_status
from ..variable_solver import VariableSolver

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


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return bool(default)


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
            "database_url", "db_tunnel_command",
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
        self.propagate_db_secret = _parse_bool(env_config.get("propagate_db_secret"), default=True)
        self.load_secrets_file = _parse_bool(env_config.get("load_secrets_file"), default=True)
        if allow_workspace_source is None:
            self.allow_workspace_source = _parse_bool(env_config.get("allow_workspace_source"), default=False)
        else:
            self.allow_workspace_source = _parse_bool(allow_workspace_source, default=False)
        config_database_url = str(env_config.get("database_url") or "").strip()
        self.database_url = config_database_url or self._load_database_url()
        self.db_tunnel_command = str(env_config.get("db_tunnel_command") or "").strip()
        self._statuses: Dict[str, RunStatus] = {}

    def _append_db_tunnel_lines(self, lines: list[str]) -> None:
        if not self.db_tunnel_command:
            return
        if self.verbose:
            lines.append("log_step 'starting DB SSH tunnel'")
        lines.append(f"{self.db_tunnel_command}")

    def _run_cmd_with_retries(
        self,
        cmd: List[str],
        *,
        timeout: int,
        retries: int,
        op_name: str,
    ) -> subprocess.CompletedProcess:
        attempts = max(1, int(retries) + 1)
        last_timeout: Optional[subprocess.TimeoutExpired] = None
        for attempt in range(1, attempts + 1):
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                last_timeout = exc
                if attempt < attempts:
                    if self.verbose:
                        print(f"{op_name} timeout (attempt {attempt}/{attempts}); retrying")
                    time.sleep(self.remote_retry_delay_seconds * attempt)
                    continue
                raise SlurmSubmitError(
                    f"{op_name} timed out after {timeout}s (attempt {attempt}/{attempts})"
                ) from exc

            if proc.returncode == 0:
                return proc

            if attempt < attempts:
                if self.verbose:
                    err = (proc.stderr or proc.stdout or "").strip()
                    print(f"{op_name} failed (attempt {attempt}/{attempts}): {err} | retrying")
                time.sleep(self.remote_retry_delay_seconds * attempt)
                continue
            return proc

        # Unreachable in normal flow; keep a defensive error.
        if last_timeout is not None:
            raise SlurmSubmitError(f"{op_name} timed out after {attempts} attempts")
        raise SlurmSubmitError(f"{op_name} failed after {attempts} attempts")

    def _ssh_common_args(self) -> List[str]:
        args: List[str] = []
        if self.env.ssh_jump:
            args += ["-J", self.env.ssh_jump]
        args += [
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={int(self.ssh_connect_timeout)}",
            "-o",
            "ConnectionAttempts=1",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
        ]
        if self.ssh_strict_host_key_checking:
            args += ["-o", f"StrictHostKeyChecking={self.ssh_strict_host_key_checking}"]
        return args

    def _build_ssh_cmd(self, target: str, *remote_parts: str) -> List[str]:
        return ["ssh"] + self._ssh_common_args() + [target, *remote_parts]

    def _build_scp_cmd(self, *parts: str) -> List[str]:
        return ["scp"] + self._ssh_common_args() + list(parts)

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
            plugin_resources = self._resolve_plugin_resources(step)
            step_resources = dict(getattr(step, "resources", {}) or {})
            merged = dict(plugin_resources)
            merged.update(step_resources)

            plugin_name = str(plugin_resources.get("plugin_name") or "").strip()
            plugin_version = str(plugin_resources.get("plugin_version") or "").strip()
            plugin_ref = str(plugin_resources.get("plugin_ref") or "").strip()
            hist_key = f"{plugin_name}@{plugin_version}" if plugin_name and plugin_version else ""
            stats = dict(history_map.get(hist_key) or {})
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
        pipeline_input = Path(pipeline_path)
        pipeline_path = pipeline_input.as_posix()
        resume_run_id = context.get("resume_run_id")
        provenance = dict(context.get("provenance") or {})
        source_repo_root = Path(context.get("repo_root") or Path(".").resolve()).resolve()
        run_id = context.get("run_id")
        if not run_id:
            import uuid

            run_id = uuid.uuid4().hex
        ts = datetime.utcnow()
        run_date = ts.strftime("%y%m%d")
        run_stamp = ts.strftime("%H%M%S")
        run_started_at = ts.isoformat() + "Z"
        run_fs_id = f"{run_stamp}-{run_id[:8]}"
        pipeline = parse_pipeline(
            Path(pipeline_path),
            global_vars=context.get("global_vars") or {},
            env_vars=context.get("execution_env") or {},
            project_vars=context.get("project_vars") or {},
            context_vars=context.get("commandline_vars") or {},
        )
        batches = self._group_steps_with_indices(pipeline.steps)
        submission_records = []
        prev_jobid = None
        jobname = str(pipeline.vars.get("jobname") or pipeline.vars.get("name") or "run")
        default_remote_workdir = (Path(self.env.workdir or self.workdir) / jobname / run_date / run_fs_id).as_posix()
        pipeline_workdir_template = str((getattr(pipeline, "dirs", {}) or {}).get("workdir") or "").strip()
        resolve_max_passes = max(1, int(getattr(pipeline, "resolve_max_passes", 20) or 20))
        path_style = str((context.get("execution_env") or {}).get("path_style") or "").strip()
        style_norm = VariableSolver._normalize_path_style(path_style)
        if not style_norm:
            # SLURM submission paths are cluster-side paths; default to POSIX.
            style_norm = "unix"
        solver = VariableSolver(max_passes=resolve_max_passes)
        global_ns = dict(context.get("global_vars") or {})
        env_ns = dict(context.get("execution_env") or {})
        commandline_ns = dict(context.get("commandline_vars") or {})
        project_ns = dict(context.get("project_vars") or {})
        solver.overlay("global", global_ns, add_namespace=True, add_flat=True)
        solver.overlay("globals", global_ns, add_namespace=True, add_flat=False)
        solver.overlay("env", env_ns, add_namespace=True, add_flat=True)
        solver.overlay("project", project_ns, add_namespace=True, add_flat=True)
        solver.overlay("pipe", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("vars", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=False)
        solver.overlay("dirs", dict(getattr(pipeline, "dirs", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("commandline", commandline_ns, add_namespace=True, add_flat=True)
        solver.with_sys(
            {
                "run": {"id": run_id, "short_id": run_id[:8]},
                "job": {"id": run_id, "name": jobname},
                "step": {"id": "", "name": "", "index": ""},
                "now": {"yymmdd": run_date, "hhmmss": run_stamp},
            }
        )
        solver.update({"run_id": run_id, "jobname": jobname})
        existing_workdir = str(solver.get("workdir", "", resolve=False) or "").strip()
        if not existing_workdir:
            solver.update({"workdir": default_remote_workdir})
        remote_workdir = solver.get_path("workdir", default_remote_workdir, path_style=style_norm)
        if pipeline_workdir_template:
            solver.update({"_candidate_workdir": pipeline_workdir_template})
            candidate = str(solver.get("_candidate_workdir", "", resolve=True) or "").strip()
            if "{" in candidate or "}" in candidate:
                raise SlurmSubmitError(
                    "Could not fully resolve pipeline dirs.workdir for SLURM submit: "
                    f"{pipeline_workdir_template}"
                )
            if candidate:
                remote_workdir = solver.get_path("_candidate_workdir", candidate, resolve=True, path_style=style_norm)
        solver.update({"workdir": remote_workdir})
        remote_workdir_root = Path(remote_workdir)
        child_jobs_file = f"{remote_workdir}/child_jobs.txt"
        context_file = f"{remote_workdir}/context.json"
        checkout_root = (self.remote_base / self.local_repo_name).as_posix()
        source_mode = str(context.get("execution_source") or self.execution_source or "auto").strip().lower()
        source_bundle = context.get("source_bundle") or self.source_bundle
        source_snapshot = context.get("source_snapshot") or self.source_snapshot
        allow_workspace_source = _parse_bool(
            context.get("allow_workspace_source", self.allow_workspace_source),
            default=self.allow_workspace_source,
        )
        selected_source_mode = "workspace"
        git_origin_url = None
        git_commit_sha = None
        git_remote_override = str(context.get("execution_env", {}).get("git_remote_url") or self.env.git_remote_url or "").strip() or None

        if self.enforce_git_checkout:
            if source_mode == "workspace" and not allow_workspace_source:
                raise SlurmSubmitError("execution_source=workspace requires allow_workspace_source=true")

            spec = None
            spec_error: Optional[Exception] = None
            if source_mode in {"git_remote", "auto"}:
                try:
                    spec = resolve_execution_spec(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=self.require_clean_git,
                        require_origin=not bool(git_remote_override),
                    )
                except Exception as exc:  # noqa: BLE001
                    spec_error = exc
            elif source_mode == "git_bundle":
                try:
                    spec = resolve_execution_spec(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=self.require_clean_git,
                        require_origin=False,
                    )
                except Exception as exc:  # noqa: BLE001
                    spec_error = exc
            elif source_mode == "snapshot":
                try:
                    spec = resolve_execution_spec(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=False,
                        require_origin=False,
                    )
                except Exception as exc:  # noqa: BLE001
                    spec_error = exc

            if source_mode in {"git_remote", "git_bundle", "snapshot"} and spec is None:
                raise SlurmSubmitError(str(spec_error or "Could not resolve execution source metadata"))

            if spec is not None:
                if git_remote_override:
                    spec = replace(spec, origin_url=git_remote_override)
                git_origin_url = spec.origin_url
                git_commit_sha = spec.commit_sha
                provenance["git_repo_name"] = spec.repo_name
                provenance["git_commit_sha"] = spec.commit_sha
                if spec.origin_url:
                    provenance["git_origin_url"] = spec.origin_url

                if source_mode == "git_remote":
                    selected_source_mode = "git_remote"
                elif source_mode == "git_bundle":
                    selected_source_mode = "git_bundle"
                elif source_mode == "snapshot":
                    selected_source_mode = "snapshot"
                else:
                    # auto: decide at runtime in setup script
                    selected_source_mode = "auto"

                if selected_source_mode in {"git_remote", "auto", "git_bundle", "snapshot"}:
                    if self.env.ssh_host:
                        remote_base = (self.env.remote_repo or "$HOME/.etl/checkouts").rstrip("/")
                        checkout_root = f"{remote_base}/{spec.repo_name}-{spec.commit_sha[:12]}"
                    else:
                        checkout_root = (Path(self.workdir) / "_code" / f"{spec.repo_name}-{spec.commit_sha[:12]}").as_posix()
            elif source_mode == "workspace":
                selected_source_mode = "workspace"
                checkout_root = (
                    (Path(self.env.remote_repo).as_posix() if (self.env.ssh_host and self.env.remote_repo) else (self.remote_base / self.local_repo_name).as_posix())
                    if self.env.ssh_host
                    else source_repo_root.as_posix()
                )
            elif source_mode == "auto":
                selected_source_mode = "auto"
                checkout_root = (
                    (Path(self.env.remote_repo).as_posix() if (self.env.ssh_host and self.env.remote_repo) else (self.remote_base / self.local_repo_name).as_posix())
                    if self.env.ssh_host
                    else source_repo_root.as_posix()
                )
            provenance["source_mode"] = selected_source_mode

        venv_path = (Path(self.env.venv) if self.env.venv else Path(checkout_root) / ".venv").as_posix()
        req_path = (
            Path(self.env.requirements).as_posix()
            if self.env.requirements
            else (Path(checkout_root) / "requirements.txt").as_posix()
        )
        python_bin = self.env.python or "python3"
        # resolve pipeline and plugins dir to POSIX paths on remote checkout
        use_repo_relative_paths = self.enforce_git_checkout and selected_source_mode != "workspace"
        workspace_repo_root = Path(checkout_root)
        pipeline_remote_hint = str(context.get("pipeline_remote_hint") or "").strip()

        if use_repo_relative_paths:
            try:
                pipeline_rel = repo_relative_path(pipeline_input, source_repo_root, "pipeline")
                pipeline_remote = (Path(checkout_root) / pipeline_rel).as_posix()
            except SourceControlError as exc:
                if pipeline_remote_hint:
                    pipeline_remote = (Path(checkout_root) / Path(pipeline_remote_hint)).as_posix()
                else:
                    raise SlurmSubmitError(
                        "Pipeline path is outside the execution source repository and no pipeline_remote_hint was "
                        f"provided: {pipeline_input}"
                    ) from exc
        else:
            if Path(pipeline_path).is_absolute():
                pipeline_remote = Path(pipeline_path).as_posix()
            else:
                pipeline_remote = (workspace_repo_root / Path(pipeline_path)).as_posix()

        if use_repo_relative_paths:
            plugins_rel = repo_relative_path(self.plugins_dir, source_repo_root, "plugins_dir")
            plugins_remote = (Path(checkout_root) / plugins_rel).as_posix()
        else:
            plugins_remote = (
                (workspace_repo_root / self.plugins_dir).as_posix()
                if not self.plugins_dir.is_absolute()
                else self.plugins_dir.as_posix()
            )

        global_config_remote = None
        if self.global_config:
            gc_path = Path(self.global_config)
            if use_repo_relative_paths:
                gc_rel = repo_relative_path(gc_path, source_repo_root, "global_config")
                global_config_remote = (Path(checkout_root) / gc_rel).as_posix()
            else:
                global_config_remote = ((Path(checkout_root) / gc_path).as_posix() if not gc_path.is_absolute() else gc_path.as_posix())

        environments_config_remote = None
        if self.environments_config and self.env_name:
            ec_path = Path(self.environments_config)
            if use_repo_relative_paths:
                ec_rel = repo_relative_path(ec_path, source_repo_root, "environments_config")
                environments_config_remote = (Path(checkout_root) / ec_rel).as_posix()
            else:
                environments_config_remote = (
                    (Path(checkout_root) / ec_path).as_posix() if not ec_path.is_absolute() else ec_path.as_posix()
                )
        projects_config_remote = None
        if self.projects_config:
            pc_path = Path(self.projects_config)
            if use_repo_relative_paths:
                pc_rel = repo_relative_path(pc_path, source_repo_root, "projects_config")
                projects_config_remote = (Path(checkout_root) / pc_rel).as_posix()
            else:
                projects_config_remote = (
                    (Path(checkout_root) / pc_path).as_posix() if not pc_path.is_absolute() else pc_path.as_posix()
                )

        pipeline_logdir_resolved = str(solver.get_path("dirs.logdir", "", path_style=style_norm) or "").strip()
        if "{" in pipeline_logdir_resolved or "}" in pipeline_logdir_resolved:
            pipeline_logdir_resolved = ""
        use_pipeline_logdir = bool(pipeline_logdir_resolved)
        base_logdir = Path(pipeline_logdir_resolved or self.env.logdir or (self.workdir / "slurm_logs"))

        if self.env.ssh_host and self.env.sync and not self.enforce_git_checkout:
            self._sync_repo()
        source_bundle_remote = self._stage_source_asset(
            source_bundle, remote_workdir, run_id=run_id, label="source-bundle"
        )
        source_snapshot_remote = self._stage_source_asset(
            source_snapshot, remote_workdir, run_id=run_id, label="source-snapshot"
        )

        # submit setup job to prep venv and work dirs
        if use_pipeline_logdir:
            setup_logdir = (base_logdir / "setup").as_posix()
        else:
            setup_logdir = (base_logdir / jobname / "setup" / run_date / run_fs_id).as_posix()
        workdirs_to_create = []
        logdirs_to_create = [setup_logdir]
        # gather work/log dirs for batches
        for batch_idx, batch in enumerate(batches):
            if len(batch) == 1:
                step_indices = [batch[0][0]]
                steps = [batch[0][1]]
                step_name = getattr(steps[0], "name", f"step{step_indices[0]}")
                label = step_name
                step_workdir = (remote_workdir_root / label).as_posix()
                if use_pipeline_logdir:
                    step_logdir = (base_logdir / label).as_posix()
                else:
                    step_logdir = (base_logdir / jobname / label / run_date / run_fs_id).as_posix()
                workdirs_to_create.append(step_workdir)
                logdirs_to_create.append(step_logdir)
            else:
                chunk_size = min(self.array_task_limit, len(batch))
                start = 0
                while start < len(batch):
                    chunk = batch[start:start+chunk_size]
                    first_name = getattr(chunk[0][1], "name", f"step{chunk[0][0]}")
                    label = f"{first_name}_array{batch_idx}_chunk{start}"
                    step_workdir = (remote_workdir_root / label).as_posix()
                    if use_pipeline_logdir:
                        step_logdir = (base_logdir / label).as_posix()
                    else:
                        step_logdir = (base_logdir / jobname / label / run_date / run_fs_id).as_posix()
                    workdirs_to_create.append(step_workdir)
                    logdirs_to_create.append(step_logdir)
                    start += chunk_size

        setup_script = self._render_setup_script(
            run_id,
            checkout_root,
            remote_workdir,
            setup_logdir,
            venv_path,
            req_path,
            python_bin,
            workdirs_to_create,
            logdirs_to_create,
            execution_source=selected_source_mode,
            git_origin_url=git_origin_url,
            git_commit_sha=git_commit_sha,
            source_bundle_path=source_bundle_remote,
            source_snapshot_path=source_snapshot_remote,
            allow_workspace_source=allow_workspace_source,
        )
        setup_jobid = self._submit_script(setup_script, run_id, label="setup", remote_dest_dir=remote_workdir)
        prev_jobid = setup_jobid
        submission_records.append(setup_jobid)

        for batch_idx, batch in enumerate(batches):
            # Cap array size if needed (for now we treat batch as single job if size==1, else array)
            if len(batch) == 1:
                step_indices = [batch[0][0]]
                steps = [batch[0][1]]
                step_name = getattr(steps[0], "name", f"step{step_indices[0]}")
                foreach_count = self._foreach_count_from_pipeline(steps[0], pipeline)
                if foreach_count and foreach_count > 1:
                    foreach_array_max_parallel = self._foreach_array_max_concurrency(steps[0])
                    chunk_size = min(self.array_task_limit, int(foreach_count))
                    start = 0
                    while start < int(foreach_count):
                        chunk_n = min(chunk_size, int(foreach_count) - start)
                        label = f"{step_name}_foreach_chunk{start}"
                        step_workdir = (remote_workdir_root / step_name).as_posix()
                        if use_pipeline_logdir:
                            step_logdir = (base_logdir / step_name).as_posix()
                        else:
                            step_logdir = (base_logdir / jobname / step_name / run_date / run_fs_id).as_posix()
                        batch_resources = self._resolve_batch_resources(steps)
                        script_text = self._render_batch_script(
                            run_id,
                            checkout_root,
                            pipeline_remote,
                            steps,
                            step_indices,
                            context_file,
                            remote_workdir,
                            plugins_remote,
                            step_logdir,
                            venv_path,
                            req_path,
                            python_bin,
                            project_id=context.get("project_id"),
                            resume_run_id=resume_run_id,
                            run_started_at=run_started_at,
                            global_config_path=global_config_remote,
                            projects_config_path=projects_config_remote,
                            environments_config_path=environments_config_remote,
                            commandline_vars=commandline_ns,
                            child_jobs_file=child_jobs_file,
                            sbatch_time=batch_resources.get("time"),
                            sbatch_cpus_per_task=batch_resources.get("cpus_per_task"),
                            sbatch_mem=batch_resources.get("mem"),
                            array_index=True,
                            array_count=chunk_n,
                            array_max_parallel=foreach_array_max_parallel,
                            foreach_from_array=True,
                            foreach_item_offset=start,
                        )
                        jobid = self._submit_script(
                            script_text,
                            run_id,
                            label=label,
                            prev_dependency=prev_jobid,
                            array_bounds=(0, chunk_n - 1),
                            remote_dest_dir=step_workdir,
                        )
                        prev_jobid = jobid
                        submission_records.append(jobid)
                        start += chunk_size
                    continue
                label = step_name
                step_workdir = (remote_workdir_root / step_name).as_posix()
                if use_pipeline_logdir:
                    step_logdir = (base_logdir / step_name).as_posix()
                else:
                    step_logdir = (base_logdir / jobname / step_name / run_date / run_fs_id).as_posix()
                batch_resources = self._resolve_batch_resources(steps)
                script_text = self._render_batch_script(
                    run_id,
                    checkout_root,
                    pipeline_remote,
                    steps,
                    step_indices,
                    context_file,
                    remote_workdir,
                    plugins_remote,
                    step_logdir,
                    venv_path,
                    req_path,
                    python_bin,
                    project_id=context.get("project_id"),
                    resume_run_id=resume_run_id,
                    run_started_at=run_started_at,
                    global_config_path=global_config_remote,
                    projects_config_path=projects_config_remote,
                    environments_config_path=environments_config_remote,
                    commandline_vars=commandline_ns,
                    child_jobs_file=child_jobs_file,
                    sbatch_time=batch_resources.get("time"),
                    sbatch_cpus_per_task=batch_resources.get("cpus_per_task"),
                    sbatch_mem=batch_resources.get("mem"),
                    array_index=False,
                )
                jobid = self._submit_script(script_text, run_id, label=label, prev_dependency=prev_jobid, remote_dest_dir=step_workdir)
                prev_jobid = jobid
                submission_records.append(jobid)
            else:
                # array: chunk if exceeds array_task_limit
                chunk_size = min(self.array_task_limit, len(batch))
                start = 0
                while start < len(batch):
                    chunk = batch[start:start+chunk_size]  # list of (idx, step)
                    chunk_steps = [s for _, s in chunk]
                    chunk_indices = [idx for idx, _ in chunk]
                    first_name = getattr(chunk_steps[0], "name", f"step{chunk_indices[0]}")
                    label = f"{first_name}_array{batch_idx}_chunk{start}"
                    step_workdir = (remote_workdir_root / label).as_posix()
                    if use_pipeline_logdir:
                        step_logdir = (base_logdir / label).as_posix()
                    else:
                        step_logdir = (base_logdir / jobname / label / run_date / run_fs_id).as_posix()
                    batch_resources = self._resolve_batch_resources(chunk_steps)
                    script_text = self._render_batch_script(
                        run_id,
                        checkout_root,
                        pipeline_remote,
                        chunk_steps,
                        chunk_indices,
                        context_file,
                        remote_workdir,
                        plugins_remote,
                        step_logdir,
                        venv_path,
                        req_path,
                        python_bin,
                        project_id=context.get("project_id"),
                        resume_run_id=resume_run_id,
                        run_started_at=run_started_at,
                        global_config_path=global_config_remote,
                        projects_config_path=projects_config_remote,
                        environments_config_path=environments_config_remote,
                        commandline_vars=commandline_ns,
                        child_jobs_file=child_jobs_file,
                        sbatch_time=batch_resources.get("time"),
                        sbatch_cpus_per_task=batch_resources.get("cpus_per_task"),
                        sbatch_mem=batch_resources.get("mem"),
                        array_index=True,
                    )
                    jobid = self._submit_script(script_text, run_id, label=label, prev_dependency=prev_jobid, array_bounds=(0, len(chunk)-1), remote_dest_dir=step_workdir)
                    prev_jobid = jobid
                    submission_records.append(jobid)
                    start += chunk_size

        if self.enable_controller_job:
            controller_script = self._render_controller_script(
                run_id=run_id,
                workdir=remote_workdir,
                logdir=setup_logdir,
                child_jobs_file=child_jobs_file,
                wait_children=self.controller_wait_children,
                poll_seconds=self.controller_poll_seconds,
            )
            controller_jobid = self._submit_script(
                controller_script,
                run_id,
                label="controller",
                prev_dependency=prev_jobid,
                remote_dest_dir=remote_workdir,
            )
            prev_jobid = controller_jobid
            submission_records.append(controller_jobid)

        status = RunStatus(run_id=run_id, state=RunState.QUEUED, message=f"submitted {len(submission_records)} jobs")
        self._statuses[run_id] = status
        upsert_run_status(
            run_id=run_id,
            pipeline=pipeline_path,
            project_id=context.get("project_id"),
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
        return SubmissionResult(run_id=run_id, backend_run_id=",".join(submission_records), job_ids=submission_records, message="submitted")

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
        array_index: bool = False,
    ) -> str:
        logdir = logdir or (Path(workdir) / "slurm_logs").as_posix()
        lines = ["#!/bin/bash --login"]
        if self.env.partition:
            lines.append(f"#SBATCH -p {self.env.partition}")
        if self.env.account:
            lines.append(f"#SBATCH -A {self.env.account}")
        if self.env.time:
            lines.append(f"#SBATCH -t {self.env.time}")
        if self.env.cpus_per_task:
            lines.append(f"#SBATCH -c {self.env.cpus_per_task}")
        if self.env.mem:
            lines.append(f"#SBATCH --mem={self.env.mem}")
        lines.append(f"#SBATCH -J etl-{run_id[:8]}")
        lines.append(f"#SBATCH -o {logdir}/etl-{run_id}-%j.%a.out" if array_index else f"#SBATCH -o {logdir}/etl-{run_id}-%j.out")
        if array_index:
            lines.append(f"#SBATCH --array=0-{len(steps)-1}")
        if self.env.sbatch_extra:
            for extra in self.env.sbatch_extra:
                lines.append(f"#SBATCH {extra}")

        lines.append("set -euo pipefail")
        lines.append(f"mkdir -p {logdir}")
        lines.append(f"mkdir -p {workdir}")
        lines.append(f"cd {checkout_root}")
        lines.append(f"export PYTHONPATH={checkout_root}:${{PYTHONPATH:-}}")
        lines.append("if ! python -c 'import etl.run_batch' >/dev/null 2>&1; then")
        lines.append("  python -m pip install --no-deps -e \"$ETL_REPO_ROOT\"")
        lines.append("fi")

        if self.env.modules:
            for mod in self.env.modules:
                lines.append(f"module load {mod}")
        if self.env.conda_env:
            lines.append(f"source activate {self.env.conda_env}")

        env_workdir = Path(workdir).as_posix()
        lines.append(f"cd {env_workdir}")
        if array_index:
            indices_str = " ".join(str(i) for i in step_indices)
            lines.append(f"step_indices=({indices_str})")
            step_arg = "${step_indices[$SLURM_ARRAY_TASK_ID]}"
        else:
            step_arg = ",".join(str(i) for i in step_indices)

        cmd = [
            "python",
            "-m",
            "etl.run_batch",
            pipeline_path,
            "--steps",
            step_arg,
            "--plugins-dir",
            plugins_dir,
            "--workdir",
            env_workdir,
        ]
        cmd += ["--context-file", context_file]
        cmd += ["--run-id", run_id]
        cmd += ["--max-retries", str(self.step_max_retries)]
        cmd += ["--retry-delay-seconds", str(self.step_retry_delay_seconds)]
        if self.global_config:
            gc_path = Path(self.global_config)
            gc_arg = ((Path(checkout_root) / gc_path).as_posix() if not gc_path.is_absolute() else gc_path.as_posix())
            cmd += ["--global-config", gc_arg]
        if self.projects_config:
            pc_path = Path(self.projects_config)
            pc_arg = ((Path(checkout_root) / pc_path).as_posix() if not pc_path.is_absolute() else pc_path.as_posix())
            cmd += ["--projects-config", pc_arg]
        if self.environments_config and self.env_name:
            ec_path = Path(self.environments_config)
            ec_arg = ((Path(checkout_root) / ec_path).as_posix() if not ec_path.is_absolute() else ec_path.as_posix())
            cmd += ["--environments-config", ec_arg, "--env", self.env_name]

        lines.append(" ".join(cmd))

        return "\n".join(lines)

    # Override render to add venv/provisioning and absolute python
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
        logdir = logdir or (Path(workdir) / "slurm_logs").as_posix()
        lines = ["#!/bin/bash --login"]
        eff_time = str(sbatch_time or self.env.time or "").strip() or None
        eff_cpus = sbatch_cpus_per_task if sbatch_cpus_per_task not in (None, 0) else self.env.cpus_per_task
        eff_mem = str(sbatch_mem or self.env.mem or "").strip() or None
        if self.env.partition:
            lines.append(f"#SBATCH -p {self.env.partition}")
        if self.env.account:
            lines.append(f"#SBATCH -A {self.env.account}")
        if eff_time:
            lines.append(f"#SBATCH -t {eff_time}")
        if eff_cpus:
            lines.append(f"#SBATCH -c {int(eff_cpus)}")
        if eff_mem:
            lines.append(f"#SBATCH --mem={eff_mem}")
        lines.append(f"#SBATCH -J etl-{run_id[:8]}")
        lines.append(f"#SBATCH -o {logdir}/etl-{run_id}-%j.%a.out" if array_index else f"#SBATCH -o {logdir}/etl-{run_id}-%j.out")
        if array_index:
            array_n = int(array_count or len(steps))
            array_upper = max(0, array_n - 1)
            if array_max_parallel not in (None, ""):
                try:
                    arr_cap = int(array_max_parallel)
                except (TypeError, ValueError):
                    arr_cap = 0
                if arr_cap > 0:
                    lines.append(f"#SBATCH --array=0-{array_upper}%{arr_cap}")
                else:
                    lines.append(f"#SBATCH --array=0-{array_upper}")
            else:
                lines.append(f"#SBATCH --array=0-{array_upper}")
        if self.env.sbatch_extra:
            for extra in self.env.sbatch_extra:
                lines.append(f"#SBATCH {extra}")

        lines.append("set -euo pipefail")
        if self.verbose:
            lines.append("ETL_VERBOSE=1")
            lines.append("log_step(){ [ \"$ETL_VERBOSE\" = \"1\" ] && echo \"[etl][$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1\"; }")
            lines.append("log_step 'batch bootstrap started'")
        if self.verbose:
            lines.append("log_step 'creating log and work directories'")
        lines.append(f"mkdir -p {logdir}")
        lines.append(f"cd {checkout_root}")
        if self.verbose:
            lines.append("log_step 'activating runtime environment'")
        lines.append(f"PYTHON={python_bin}")
        lines.append(f"VENV={venv_path}")
        lines.append(f"export ETL_REPO_ROOT={checkout_root}")
        self._append_db_tunnel_lines(lines)
        if child_jobs_file:
            lines.append(f"export ETL_CHILD_JOBS_FILE={child_jobs_file}")
        if self.load_secrets_file:
            if self.verbose:
                lines.append("log_step 'loading optional secrets file (values hidden)'")
            lines.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
        lines.append("source \"$VENV/bin/activate\"")
        lines.append(f"export PYTHONPATH={checkout_root}:${{PYTHONPATH:-}}")
        lines.append("if ! \"$VENV/bin/python\" -c 'import etl.run_batch' >/dev/null 2>&1; then")
        lines.append("  \"$VENV/bin/python\" -m pip install --no-deps -e \"$ETL_REPO_ROOT\"")
        lines.append("fi")

        if self.env.modules:
            for mod in self.env.modules:
                if self.verbose:
                    lines.append(f"log_step {shlex.quote(f'loading module: {mod}')}")
                lines.append(f"module load {mod}")
        if self.env.conda_env:
            if self.verbose:
                lines.append("log_step 'activating conda environment'")
            lines.append(f"source activate {self.env.conda_env}")

        env_workdir = Path(workdir).as_posix()
        if self.verbose:
            lines.append("log_step 'ensuring step workdir exists'")
        lines.append(f"mkdir -p {env_workdir}")
        if self.verbose:
            lines.append("log_step 'switching to step workdir'")
        lines.append(f"cd {env_workdir}")
        foreach_arg: Optional[str] = None
        if array_index and foreach_from_array:
            step_arg = ",".join(str(i) for i in step_indices)
            if int(foreach_item_offset or 0) > 0:
                foreach_arg = f"$((SLURM_ARRAY_TASK_ID+{int(foreach_item_offset)}))"
            else:
                foreach_arg = "${SLURM_ARRAY_TASK_ID}"
        elif array_index:
            indices_str = " ".join(str(i) for i in step_indices)
            lines.append(f"step_indices=({indices_str})")
            step_arg = "${step_indices[$SLURM_ARRAY_TASK_ID]}"
        else:
            step_arg = ",".join(str(i) for i in step_indices)
        run_started_expr = ""
        if run_started_at:
            # Backward-compatible runtime feature detection:
            # older checkouts may not yet expose --run-started-at on run_batch.py.
            lines.append("RUN_STARTED_OPT=''")
            lines.append("RUN_STARTED_VAL=''")
            lines.append("if \"$VENV/bin/python\" -m etl.run_batch -h 2>&1 | grep -q -- '--run-started-at'; then")
            lines.append("  RUN_STARTED_OPT='--run-started-at'")
            lines.append(f"  RUN_STARTED_VAL={shlex.quote(str(run_started_at))}")
            lines.append("fi")
            run_started_expr = "${RUN_STARTED_OPT:+$RUN_STARTED_OPT $RUN_STARTED_VAL}"

        cmd = [
            "$VENV/bin/python",
            "-m",
            "etl.run_batch",
            pipeline_path,
            "--steps",
            step_arg,
            "--plugins-dir",
            plugins_dir,
            "--workdir",
            env_workdir,
        ]
        cmd += ["--context-file", context_file]
        cmd += ["--run-id", run_id]
        if project_id:
            cmd += ["--project-id", str(project_id)]
        if resume_run_id:
            cmd += ["--resume-run-id", str(resume_run_id)]
        if run_started_expr:
            cmd += [run_started_expr]
        cmd += ["--max-retries", str(self.step_max_retries)]
        cmd += ["--retry-delay-seconds", str(self.step_retry_delay_seconds)]
        if global_config_path:
            cmd += ["--global-config", global_config_path]
        if projects_config_path:
            cmd += ["--projects-config", projects_config_path]
        if environments_config_path and self.env_name:
            cmd += ["--environments-config", environments_config_path, "--env", self.env_name]
        if foreach_arg:
            cmd += ["--foreach-item-index", foreach_arg]
        for key, value in _flatten_vars_for_cli(dict(commandline_vars or {})):
            cmd += ["--var", f"{key}={value}"]
        if self.verbose:
            cmd += ["--verbose"]

        if self.verbose:
            lines.append("log_step 'running etl.run_batch'")
        lines.append(" ".join(cmd))

        return "\n".join(lines)

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
        lines = ["#!/bin/bash --login"]
        if self.env.partition:
            lines.append(f"#SBATCH -p {self.env.partition}")
        if self.env.account:
            lines.append(f"#SBATCH -A {self.env.account}")
        lines.append("#SBATCH -t 00:20:00")
        lines.append("#SBATCH -c 1")
        lines.append("#SBATCH --mem=512M")
        lines.append(f"#SBATCH -J etl-ctl-{run_id[:8]}")
        lines.append(f"#SBATCH -o {logdir}/etl-controller-{run_id}-%j.out")
        if self.env.sbatch_extra:
            for extra in self.env.sbatch_extra:
                lines.append(f"#SBATCH {extra}")
        lines.append("set -euo pipefail")
        lines.append(f"mkdir -p {logdir}")
        lines.append(f"mkdir -p {workdir}")
        lines.append(f"CHILD_FILE={shlex.quote(child_jobs_file)}")
        lines.append(f"POLL={int(poll_seconds)}")
        lines.append(f"WAIT_CHILDREN={'1' if wait_children else '0'}")
        lines.append("echo '[controller] started'")
        lines.append("if [ \"$WAIT_CHILDREN\" != \"1\" ]; then")
        lines.append("  echo '[controller] wait_children disabled; exiting'")
        lines.append("  exit 0")
        lines.append("fi")
        lines.append("if [ ! -f \"$CHILD_FILE\" ]; then")
        lines.append("  echo '[controller] no child job file found; exiting'")
        lines.append("  exit 0")
        lines.append("fi")
        lines.append("mapfile -t RAW_IDS < <(grep -Eo '[0-9]+(_[0-9]+)?' \"$CHILD_FILE\" | awk '!seen[$0]++')")
        lines.append("if [ ${#RAW_IDS[@]} -eq 0 ]; then")
        lines.append("  echo '[controller] no child ids registered; exiting'")
        lines.append("  exit 0")
        lines.append("fi")
        lines.append("echo \"[controller] waiting on ${#RAW_IDS[@]} child job ids\"")
        lines.append("for jid in \"${RAW_IDS[@]}\"; do")
        lines.append("  while squeue -h -j \"$jid\" >/dev/null 2>&1 && [ -n \"$(squeue -h -j \"$jid\")\" ]; do")
        lines.append("    sleep \"$POLL\"")
        lines.append("  done")
        lines.append("done")
        lines.append("FAILED=0")
        lines.append("for jid in \"${RAW_IDS[@]}\"; do")
        lines.append("  if command -v sacct >/dev/null 2>&1; then")
        lines.append("    state=$(sacct -n -P -j \"$jid\" --format=State 2>/dev/null | head -n 1 | cut -d'|' -f1 | tr -d '[:space:]')")
        lines.append("    if [ -n \"$state\" ] && [[ \"$state\" != COMPLETED* ]]; then")
        lines.append("      echo \"[controller] child job $jid state=$state\"")
        lines.append("      FAILED=1")
        lines.append("    fi")
        lines.append("  fi")
        lines.append("done")
        lines.append("if [ \"$FAILED\" = \"1\" ]; then")
        lines.append("  echo '[controller] child job failures detected'")
        lines.append("  exit 1")
        lines.append("fi")
        lines.append("echo '[controller] all child jobs complete'")
        lines.append("exit 0")
        return "\n".join(lines)

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
    ) -> str:
        logdir = logdir or (Path(workdir) / "slurm_logs").as_posix()
        lines = ["#!/bin/bash --login"]
        setup_time = str(self.env.setup_time or self.env.time or "00:10:00").strip() or "00:10:00"
        setup_cpus = int(self.env.cpus_per_task or 0) or None
        setup_mem = str(self.env.mem or "").strip() or None
        max_time_min = _parse_slurm_time_to_minutes(str(self.env.max_time or ""))
        max_cpus = int(self.env.max_cpus_per_task or 0) or None
        max_mem_mb = _parse_mem_to_mb(str(self.env.max_mem or ""))
        if setup_time and max_time_min is not None:
            cur_min = _parse_slurm_time_to_minutes(setup_time)
            if cur_min is not None:
                setup_time = _format_minutes_as_slurm_time(min(cur_min, max_time_min))
        if setup_cpus and max_cpus is not None:
            setup_cpus = min(setup_cpus, max_cpus)
        if setup_mem and max_mem_mb is not None:
            cur_mem_mb = _parse_mem_to_mb(setup_mem)
            if cur_mem_mb is not None:
                setup_mem = _format_mb_as_slurm_mem(min(cur_mem_mb, max_mem_mb))
        if self.env.partition:
            lines.append(f"#SBATCH -p {self.env.partition}")
        if self.env.account:
            lines.append(f"#SBATCH -A {self.env.account}")
        if setup_time:
            lines.append(f"#SBATCH -t {setup_time}")
        if setup_cpus:
            lines.append(f"#SBATCH -c {setup_cpus}")
        if setup_mem:
            lines.append(f"#SBATCH --mem={setup_mem}")
        lines.append(f"#SBATCH -J etl-setup-{run_id[:6]}")
        lines.append(f"#SBATCH -o {logdir}/etl-setup-{run_id}-%j.out")
        if self.env.sbatch_extra:
            for extra in self.env.sbatch_extra:
                lines.append(f"#SBATCH {extra}")

        lines.append("set -euo pipefail")
        if self.verbose:
            lines.append("ETL_VERBOSE=1")
            lines.append("log_step(){ [ \"$ETL_VERBOSE\" = \"1\" ] && echo \"[etl][$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1\"; }")
            lines.append("log_step 'setup bootstrap started'")
        if self.verbose:
            lines.append("log_step 'creating setup directories'")
        lines.append(f"mkdir -p {logdir}")
        lines.append(f"mkdir -p {workdir}")
        for d in workdirs_to_create:
            lines.append(f"mkdir -p {d}")
        for d in logdirs_to_create:
            lines.append(f"mkdir -p {d}")
        if self.env.modules:
            for mod in self.env.modules:
                if self.verbose:
                    lines.append(f"log_step {shlex.quote(f'loading module: {mod}')}")
                lines.append(f"module load {mod}")
        if self.env.conda_env:
            if self.verbose:
                lines.append("log_step 'activating conda environment'")
            lines.append(f"source activate {self.env.conda_env}")
        if self.verbose:
            lines.append("log_step 'preparing execution source'")
        lines.append(f"CHECKOUT_ROOT={shlex.quote(checkout_root)}")
        lines.append(f"SOURCE_MODE={shlex.quote(execution_source)}")
        lines.append(f"ALLOW_WORKSPACE={'1' if allow_workspace_source else '0'}")
        lines.append(f"REPO_URL={shlex.quote(git_origin_url or '')}")
        lines.append(f"REPO_SHA={shlex.quote(git_commit_sha or '')}")
        lines.append(f"SOURCE_BUNDLE={shlex.quote(source_bundle_path or '')}")
        lines.append(f"SOURCE_SNAPSHOT={shlex.quote(source_snapshot_path or '')}")
        lines.append("mkdir -p \"$(dirname \\\"$CHECKOUT_ROOT\\\")\"")
        lines.append("prepare_git_remote(){")
        lines.append("  [ -n \"$REPO_URL\" ] && [ -n \"$REPO_SHA\" ] || { echo \"[etl][setup][source] missing REPO_URL or REPO_SHA for git_remote\" >&2; return 1; }")
        lines.append("  if [ -d \"$CHECKOUT_ROOT\" ] && [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then rm -rf \"$CHECKOUT_ROOT\"; fi")
        lines.append("  if [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then git clone --no-checkout \"$REPO_URL\" \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] git clone failed: $REPO_URL -> $CHECKOUT_ROOT\" >&2; return 1; }; fi")
        lines.append("  cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; return 1; }")
        lines.append("  git fetch --tags --prune origin || { echo \"[etl][setup][source] git fetch failed\" >&2; return 1; }")
        lines.append("  git checkout --detach \"$REPO_SHA\" || { echo \"[etl][setup][source] git checkout failed for SHA: $REPO_SHA\" >&2; return 1; }")
        lines.append("  git reset --hard \"$REPO_SHA\" || { echo \"[etl][setup][source] git reset failed for SHA: $REPO_SHA\" >&2; return 1; }")
        lines.append("}")
        lines.append("prepare_git_bundle(){")
        lines.append("  [ -n \"$SOURCE_BUNDLE\" ] && [ -n \"$REPO_SHA\" ] || { echo \"[etl][setup][source] missing SOURCE_BUNDLE or REPO_SHA for git_bundle\" >&2; return 1; }")
        lines.append("  if [ ! -f \"$SOURCE_BUNDLE\" ]; then echo \"[etl][setup][source] source bundle not found: $SOURCE_BUNDLE\" >&2; return 1; fi")
        lines.append("  if [ -d \"$CHECKOUT_ROOT\" ] && [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then rm -rf \"$CHECKOUT_ROOT\"; fi")
        lines.append("  if [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then git clone --no-checkout \"$SOURCE_BUNDLE\" \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] git clone from bundle failed\" >&2; return 1; }; fi")
        lines.append("  cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; return 1; }")
        lines.append("  git fetch \"$SOURCE_BUNDLE\" --tags || { echo \"[etl][setup][source] git fetch from bundle failed\" >&2; return 1; }")
        lines.append("  git checkout --detach \"$REPO_SHA\" || { echo \"[etl][setup][source] git checkout failed for SHA: $REPO_SHA\" >&2; return 1; }")
        lines.append("  git reset --hard \"$REPO_SHA\" || { echo \"[etl][setup][source] git reset failed for SHA: $REPO_SHA\" >&2; return 1; }")
        lines.append("}")
        lines.append("prepare_snapshot(){")
        lines.append("  [ -n \"$SOURCE_SNAPSHOT\" ] || { echo \"[etl][setup][source] missing SOURCE_SNAPSHOT for snapshot mode\" >&2; return 1; }")
        lines.append("  if [ ! -f \"$SOURCE_SNAPSHOT\" ]; then echo \"[etl][setup][source] source snapshot not found: $SOURCE_SNAPSHOT\" >&2; return 1; fi")
        lines.append("  rm -rf \"$CHECKOUT_ROOT\"")
        lines.append("  mkdir -p \"$CHECKOUT_ROOT\"")
        lines.append("  case \"$SOURCE_SNAPSHOT\" in")
        lines.append("    *.zip) unzip -q \"$SOURCE_SNAPSHOT\" -d \"$CHECKOUT_ROOT\" || return 1 ;;")
        lines.append("    *.tar|*.tar.gz|*.tgz|*.tar.bz2|*.tbz2|*.tar.xz|*.txz) tar -xf \"$SOURCE_SNAPSHOT\" -C \"$CHECKOUT_ROOT\" || return 1 ;;")
        lines.append("    *) echo \"[etl][setup][source] unsupported snapshot extension: $SOURCE_SNAPSHOT\" >&2; return 1 ;;")
        lines.append("  esac")
        lines.append("  cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; return 1; }")
        lines.append("}")
        lines.append("prepare_workspace(){")
        lines.append("  [ \"$ALLOW_WORKSPACE\" = \"1\" ] || { echo \"[etl][setup][source] workspace source not allowed\" >&2; return 1; }")
        lines.append("  [ -d \"$CHECKOUT_ROOT\" ] || { echo \"[etl][setup][source] workspace checkout missing: $CHECKOUT_ROOT\" >&2; return 1; }")
        lines.append("  cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; return 1; }")
        lines.append("}")
        lines.append("case \"$SOURCE_MODE\" in")
        lines.append("  git_remote) prepare_git_remote ;;")
        lines.append("  git_bundle) prepare_git_bundle ;;")
        lines.append("  snapshot) prepare_snapshot ;;")
        lines.append("  workspace) prepare_workspace ;;")
        lines.append("  auto)")
        lines.append("    if [ \"$ALLOW_WORKSPACE\" = \"1\" ]; then")
        lines.append("      prepare_git_remote || prepare_git_bundle || prepare_snapshot || prepare_workspace || { echo \"[etl][setup][source] auto source resolution failed\" >&2; exit 1; }")
        lines.append("    else")
        lines.append("      prepare_git_remote || prepare_git_bundle || prepare_snapshot || { echo \"[etl][setup][source] auto source resolution failed\" >&2; exit 1; }")
        lines.append("    fi")
        lines.append("    ;;")
        lines.append("  *) echo \"Unsupported execution_source: $SOURCE_MODE\" >&2; exit 1 ;;")
        lines.append("esac")
        if self.verbose:
            lines.append("log_step 'bootstrapping venv'")
        lines.append(f"PYTHON={python_bin}")
        lines.append(f"VENV={venv_path}")
        lines.append(f"export ETL_REPO_ROOT={checkout_root}")
        self._append_db_tunnel_lines(lines)
        if self.load_secrets_file:
            if self.verbose:
                lines.append("log_step 'loading optional secrets file (values hidden)'")
            lines.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
        lines.append("if [ ! -f \"$VENV/bin/activate\" ]; then")
        lines.append("  $PYTHON -m venv \"$VENV\"")
        lines.append("fi")
        lines.append("source \"$VENV/bin/activate\"")
        if self.verbose:
            lines.append("log_step 'installing requirements if present'")
        lines.append(f"if [ -f \"{req_path}\" ]; then pip install -r \"{req_path}\"; fi")
        lines.append(f"export PYTHONPATH={checkout_root}:${{PYTHONPATH:-}}")
        lines.append("if ! \"$VENV/bin/python\" -c 'import etl.run_batch' >/dev/null 2>&1; then")
        lines.append("  \"$VENV/bin/python\" -m pip install --no-deps -e \"$ETL_REPO_ROOT\"")
        lines.append("fi")
        lines.append(f"mkdir -p {workdir}")
        if self.verbose:
            lines.append("log_step 'setup complete'")
        lines.append("echo setup complete")
        return "\n".join(lines)

    def _stage_source_asset(self, asset_path: Optional[str], remote_workdir: str, run_id: str, label: str) -> Optional[str]:
        if not asset_path:
            return None
        if not self.env.ssh_host:
            return asset_path
        local_candidate = Path(asset_path).expanduser()
        if not local_candidate.exists() or not local_candidate.is_file():
            # Assume caller provided a remote-visible path.
            return asset_path

        target = f"{self.env.ssh_user + '@' if self.env.ssh_user else ''}{self.env.ssh_host}"
        remote_dir = f"{remote_workdir}/source"
        remote_file = f"{remote_dir}/{local_candidate.name}"
        mkdir_cmd = self._build_ssh_cmd(target, f"mkdir -p {remote_dir}")
        proc = self._run_cmd_with_retries(
            mkdir_cmd,
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
            op_name="ssh mkdir source dir",
        )
        if proc.returncode != 0:
            raise SlurmSubmitError(proc.stderr or proc.stdout)
        scp_cmd = self._build_scp_cmd(str(local_candidate), f"{target}:{remote_file}")
        proc2 = self._run_cmd_with_retries(
            scp_cmd,
            timeout=self.scp_timeout,
            retries=self.scp_retries,
            op_name="scp source asset",
        )
        if proc2.returncode != 0:
            raise SlurmSubmitError(proc2.stderr or proc2.stdout)
        return remote_file

    def _submit_script(self, script_text: str, run_id: str, label: str = "job", prev_dependency: Optional[str] = None, array_bounds: Optional[Tuple[int, int]] = None, remote_dest_dir: Optional[str] = None) -> str:
        dependency_arg = []
        if prev_dependency:
            dependency_arg = [f"--dependency=afterok:{prev_dependency}"]

        if self.dry_run:
            return "dry-run"

        if self.env.ssh_host:
            target = f"{self.env.ssh_user + '@' if self.env.ssh_user else ''}{self.env.ssh_host}"
            if self.propagate_db_secret:
                self._ensure_remote_secrets_file(target)
            remote_dir = remote_dest_dir or self.env.remote_repo or "/tmp"
            remote_file = f"{remote_dir}/etl-{run_id}-{label}.sbatch"
            # ensure remote dir
            mkdir_cmd = self._build_ssh_cmd(target, f"mkdir -p {remote_dir}")
            if self.verbose:
                print("SSH mkdir:", " ".join(mkdir_cmd))
            proc = self._run_cmd_with_retries(
                mkdir_cmd,
                timeout=self.ssh_timeout,
                retries=self.ssh_retries,
                op_name="ssh mkdir remote dir",
            )
            if proc.returncode != 0:
                raise SlurmSubmitError(proc.stderr or proc.stdout)
            # write temp file locally with LF and scp it
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sbatch", prefix="etl-", newline="\n") as tmp:
                tmp.write(script_text.replace("\r\n", "\n"))
                tmp_path = Path(tmp.name)
            scp_cmd = self._build_scp_cmd(str(tmp_path), f"{target}:{remote_file}")
            if self.verbose:
                print("SCP script:", " ".join(scp_cmd))
            proc_scp = self._run_cmd_with_retries(
                scp_cmd,
                timeout=self.scp_timeout,
                retries=self.scp_retries,
                op_name="scp sbatch script",
            )
            if proc_scp.returncode != 0:
                raise SlurmSubmitError(proc_scp.stderr or proc_scp.stdout)
            # submit remotely
            remote_cmd = self._build_ssh_cmd(target, "sbatch", *dependency_arg, remote_file)
            if self.verbose:
                print("SSH sbatch:", " ".join(remote_cmd))
            proc_submit = self._run_cmd_with_retries(
                remote_cmd,
                timeout=self.ssh_timeout,
                retries=self.ssh_retries,
                op_name="ssh sbatch submit",
            )
            if proc_submit.returncode != 0:
                raise SlurmSubmitError(proc_submit.stderr or proc_submit.stdout)
            if self.verbose:
                print("sbatch stdout:", proc_submit.stdout.strip())
                print("sbatch stderr:", proc_submit.stderr.strip())
            out = (proc_submit.stdout or "").strip().split()
            return out[-1] if out else "unknown"
        else:
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sbatch", prefix=f"etl-{label}-", dir=self.workdir, newline="\n") as tmp:
                tmp.write(script_text.replace("\r\n", "\n"))
                tmp_path = Path(tmp.name)
            cmd = ["sbatch"] + dependency_arg + [str(tmp_path)]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                raise SlurmSubmitError(proc.stderr or proc.stdout)
            out = (proc.stdout or "").strip().split()
            return out[-1] if out else "unknown"

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
        remote_script = "bash -lc " + shlex.quote("\n".join(remote_lines))
        cmd = self._build_ssh_cmd(target, remote_script)
        if self.verbose:
            print("SSH secrets init: ~/.secrets/etl")
        proc = self._run_cmd_with_retries(
            cmd,
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
            op_name="ssh secrets init",
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
        target = f"{self.env.ssh_user + '@' if self.env.ssh_user else ''}{self.env.ssh_host}"
        remote_path = f"{target}:{self.env.remote_repo}"
        # Create remote dir then copy
        mkdir_cmd = self._build_ssh_cmd(target, f"mkdir -p {self.env.remote_repo}")
        proc = self._run_cmd_with_retries(
            mkdir_cmd,
            timeout=self.ssh_timeout,
            retries=self.ssh_retries,
            op_name="ssh mkdir sync dir",
        )
        if proc.returncode != 0:
            raise SlurmSubmitError(proc.stderr or proc.stdout)
        # Use scp to sync repo (simple recursive copy)
        scp_cmd = self._build_scp_cmd("-r", str(Path(".").resolve()), remote_path)
        proc2 = self._run_cmd_with_retries(
            scp_cmd,
            timeout=self.scp_timeout,
            retries=self.scp_retries,
            op_name="scp sync repo",
        )
        if proc2.returncode != 0:
            raise SlurmSubmitError(proc2.stderr or proc2.stdout)

"""
HPCC direct executor: run batch worker directly over SSH on a dev node.

This bypasses SLURM scheduling and calls `python -m etl.run_batch` remotely.
Intended for fast development iteration only.
"""

from __future__ import annotations

import shlex
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import GitCheckoutError, repo_relative_path
from ..pipeline import parse_pipeline, PipelineError


class HpccDirectExecutor(Executor):
    name = "hpcc_direct"

    def __init__(
        self,
        env_config: Dict[str, Any],
        repo_root: Path,
        plugins_dir: Path = Path("plugins"),
        workdir: Path = Path(".runs"),
        global_config: Optional[Path] = None,
        environments_config: Optional[Path] = None,
        env_name: Optional[str] = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self.env_config = dict(env_config or {})
        self.repo_root = Path(repo_root).resolve()
        self.plugins_dir = Path(plugins_dir)
        self.workdir = Path(workdir)
        self.global_config = Path(global_config) if global_config else None
        self.environments_config = Path(environments_config) if environments_config else None
        self.env_name = env_name
        self.dry_run = bool(dry_run)
        self.verbose = bool(verbose)

        self.ssh_host = str(self.env_config.get("ssh_host") or "").strip()
        self.ssh_user = str(self.env_config.get("ssh_user") or "").strip()
        self.ssh_jump = str(self.env_config.get("ssh_jump") or "").strip()
        self.remote_repo = str(self.env_config.get("remote_repo") or "").strip()
        self.remote_python = str(self.env_config.get("python") or "python").strip() or "python"
        self.remote_venv = str(self.env_config.get("venv") or "").strip()
        self.remote_conda_env = str(self.env_config.get("conda_env") or "").strip()
        self.remote_modules = list(self.env_config.get("modules") or [])
        self.ssh_timeout = int(self.env_config.get("ssh_timeout", 120))
        self._statuses: Dict[str, RunStatus] = {}

    def _ssh_target(self) -> str:
        if not self.ssh_host:
            raise RuntimeError("hpcc_direct executor requires execution env field 'ssh_host'.")
        return f"{self.ssh_user + '@' if self.ssh_user else ''}{self.ssh_host}"

    def _ssh_common_args(self) -> list[str]:
        args: list[str] = []
        if self.ssh_jump:
            args += ["-J", self.ssh_jump]
        args += [
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=20",
            "-o",
            "ConnectionAttempts=1",
        ]
        return args

    def _map_repo_path(self, path: Path) -> str:
        if not self.remote_repo:
            return path.as_posix()
        try:
            rel = repo_relative_path(path, self.repo_root, "path")
        except GitCheckoutError:
            return path.as_posix()
        return (Path(self.remote_repo) / rel).as_posix()

    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        context = context or {}
        run_id = str(context.get("run_id") or "").strip() or uuid.uuid4().hex
        started_at = str(context.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
        cmdline_vars = dict(context.get("commandline_vars") or {})
        exec_env = dict(context.get("execution_env") or {})
        global_vars = dict(context.get("global_vars") or {})

        try:
            pipeline = parse_pipeline(
                Path(pipeline_path),
                global_vars=global_vars,
                env_vars=exec_env,
                context_vars=cmdline_vars,
            )
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc
        step_indices = ",".join(str(i) for i in range(len(pipeline.steps)))
        if not step_indices:
            self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message="No steps to run.")
            return SubmissionResult(run_id=run_id, message="No steps to run.")

        repo_root_remote = self.remote_repo or self.repo_root.as_posix()
        pipeline_remote = self._map_repo_path(Path(pipeline_path))
        plugins_remote = self._map_repo_path(self.plugins_dir.resolve())
        global_remote = self._map_repo_path(self.global_config.resolve()) if self.global_config else None
        env_cfg_remote = self._map_repo_path(self.environments_config.resolve()) if self.environments_config else None

        batch_cmd = [
            self.remote_python,
            "-m",
            "etl.run_batch",
            shlex.quote(pipeline_remote),
            "--steps",
            shlex.quote(step_indices),
            "--plugins-dir",
            shlex.quote(plugins_remote),
            "--workdir",
            shlex.quote(self.workdir.as_posix()),
            "--run-id",
            shlex.quote(run_id),
            "--run-started-at",
            shlex.quote(started_at),
            "--executor-type",
            "hpcc_direct",
            "--tracking-executor",
            "hpcc_direct",
        ]
        if self.global_config and global_remote:
            batch_cmd += ["--global-config", shlex.quote(global_remote)]
        if self.environments_config and env_cfg_remote:
            batch_cmd += ["--environments-config", shlex.quote(env_cfg_remote)]
        if self.env_name:
            batch_cmd += ["--env", shlex.quote(str(self.env_name))]
        project_id = str(context.get("project_id") or "").strip()
        if project_id:
            batch_cmd += ["--project-id", shlex.quote(project_id)]

        max_retries = exec_env.get("step_max_retries")
        retry_delay = exec_env.get("step_retry_delay_seconds")
        if max_retries is not None:
            batch_cmd += ["--max-retries", shlex.quote(str(int(max_retries)))]
        if retry_delay is not None:
            batch_cmd += ["--retry-delay-seconds", shlex.quote(str(float(retry_delay)))]

        if self.verbose:
            batch_cmd.append("--verbose")
        for key, value in sorted(cmdline_vars.items()):
            if isinstance(value, dict):
                continue
            batch_cmd += ["--var", shlex.quote(f"{key}={value}")]

        lines = ["set -euo pipefail", f"cd {shlex.quote(repo_root_remote)}"]
        for module_name in self.remote_modules:
            mod = str(module_name or "").strip()
            if mod:
                lines.append(f"module load {shlex.quote(mod)}")
        if self.remote_conda_env:
            lines.append(f"source activate {shlex.quote(self.remote_conda_env)}")
        if self.remote_venv:
            lines.append(f"source {shlex.quote(self.remote_venv)}/bin/activate")
        lines.append(" ".join(batch_cmd))
        remote_script = "\n".join(lines)

        ssh_cmd = ["ssh", *self._ssh_common_args(), self._ssh_target(), "bash", "-lc", remote_script]
        if self.dry_run:
            self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.QUEUED, message="dry-run")
            return SubmissionResult(run_id=run_id, message="dry-run")

        started_dt = datetime.utcnow()
        proc = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=self.ssh_timeout, check=False)
        ended_dt = datetime.utcnow()
        stdout = str(proc.stdout or "").strip()
        stderr = str(proc.stderr or "").strip()
        detail = stderr or stdout or ""
        if proc.returncode == 0:
            status = RunStatus(
                run_id=run_id,
                state=RunState.SUCCEEDED,
                message=detail[:4000],
                started_at=started_dt,
                ended_at=ended_dt,
            )
        else:
            status = RunStatus(
                run_id=run_id,
                state=RunState.FAILED,
                message=f"remote run_batch rc={proc.returncode}: {detail[:4000]}",
                started_at=started_dt,
                ended_at=ended_dt,
            )
        self._statuses[run_id] = status
        return SubmissionResult(run_id=run_id, message=status.message)

    def status(self, run_id: str) -> RunStatus:
        if run_id not in self._statuses:
            return RunStatus(run_id=run_id, state=RunState.FAILED, message="Unknown run_id")
        return self._statuses[run_id]

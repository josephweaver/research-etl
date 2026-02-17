"""
HPCC direct executor: run batch worker directly over SSH on a dev node.

This bypasses SLURM scheduling and calls `python -m etl.run_batch` remotely.
Intended for fast development iteration only.
"""

from __future__ import annotations

from dataclasses import replace
import tarfile
import tempfile
import shlex
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import (
    GitCheckoutError,
    infer_repo_name,
    repo_relative_path,
    resolve_execution_spec,
)
from ..pipeline import parse_pipeline, PipelineError


def _flatten_scalar_vars(prefix: str, obj: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in (obj or {}).items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(_flatten_scalar_vars(path, value))
        elif isinstance(value, (list, tuple)):
            # Keep commandline vars simple/scalar; complex structures are not used
            # by current hpcc_direct runtime templating needs.
            continue
        else:
            out[path] = str(value)
    return out


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
        self.remote_python = str(self.env_config.get("python") or "python3").strip() or "python3"
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

    def _map_repo_path_for_root(self, path: Path, remote_root: str, label: str = "path") -> str:
        try:
            rel = repo_relative_path(path, self.repo_root, label)
            return (Path(remote_root) / rel).as_posix()
        except GitCheckoutError:
            return path.as_posix()

    def _run_ssh_script(self, target: str, script: str) -> subprocess.CompletedProcess:
        ssh_cmd = [
            "ssh",
            *self._ssh_common_args(),
            target,
            f"bash --login -lc {shlex.quote(script)}",
        ]
        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=self.ssh_timeout, check=False)

    def _scp_to_remote(self, target: str, local_path: Path, remote_path: str) -> subprocess.CompletedProcess:
        scp_cmd = [
            "scp",
            *self._ssh_common_args(),
            str(local_path),
            f"{target}:{remote_path}",
        ]
        return subprocess.run(scp_cmd, capture_output=True, text=True, timeout=self.ssh_timeout, check=False)

    def _collect_dirty_overlay_paths(self) -> tuple[list[Path], list[str]]:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(self.repo_root),
                "status",
                "--porcelain=1",
                "-z",
                "--untracked-files=all",
            ],
            capture_output=True,
            text=False,
            check=False,
        )
        if proc.returncode != 0:
            return [], []
        raw = bytes(proc.stdout or b"")
        if not raw:
            return [], []
        parts = raw.decode("utf-8", errors="replace").split("\0")
        uploads: list[Path] = []
        deletes: list[str] = []
        seen_uploads: set[str] = set()
        seen_deletes: set[str] = set()

        idx = 0
        while idx < len(parts):
            rec = parts[idx]
            idx += 1
            if not rec:
                continue
            if len(rec) < 3:
                continue
            x = rec[0]
            y = rec[1]
            path_from = rec[3:]
            path_to: Optional[str] = None
            if x in {"R", "C"} or y in {"R", "C"}:
                if idx >= len(parts):
                    break
                path_to = parts[idx]
                idx += 1

            def _safe_rel(text: str) -> Optional[str]:
                value = str(text or "").replace("\\", "/").strip()
                if not value:
                    return None
                rel = Path(value)
                if rel.is_absolute() or any(p == ".." for p in rel.parts):
                    return None
                return rel.as_posix()

            rel_from = _safe_rel(path_from)
            rel_to = _safe_rel(path_to or "")

            if rel_from and (x == "D" or y == "D" or x == "R" or y == "R"):
                if rel_from not in seen_deletes:
                    seen_deletes.add(rel_from)
                    deletes.append(rel_from)
            if rel_to and (x == "R" or y == "R" or x == "C" or y == "C"):
                local_rel = Path(rel_to)
                local_abs = (self.repo_root / local_rel).resolve()
                if local_abs.exists() and local_abs.is_file() and rel_to not in seen_uploads:
                    seen_uploads.add(rel_to)
                    uploads.append(local_rel)
                continue
            if rel_from and not (x == "D" or y == "D"):
                local_rel = Path(rel_from)
                local_abs = (self.repo_root / local_rel).resolve()
                if local_abs.exists() and local_abs.is_file() and rel_from not in seen_uploads:
                    seen_uploads.add(rel_from)
                    uploads.append(local_rel)
        return uploads, deletes

    def _stage_dirty_overlay(self, *, target: str, run_id: str) -> tuple[Optional[str], list[str]]:
        uploads, deletes = self._collect_dirty_overlay_paths()
        if not uploads and not deletes:
            return None, []

        remote_tar: Optional[str] = None
        if uploads:
            tar_path = Path(tempfile.gettempdir()) / f"hpcc_direct_overlay_{run_id}.tar"
            if tar_path.exists():
                tar_path.unlink()
            try:
                with tarfile.open(tar_path, "w") as tf:
                    for rel in uploads:
                        tf.add((self.repo_root / rel).resolve(), arcname=rel.as_posix(), recursive=False)

                remote_tar = f"/tmp/hpcc_direct_dirty_overlay_{run_id}.tar"
                scp_proc = self._scp_to_remote(target, tar_path, remote_tar)
                if scp_proc.returncode != 0:
                    detail = (scp_proc.stderr or scp_proc.stdout or "").strip()
                    raise RuntimeError(f"Failed to transfer dirty overlay: {detail[:2000]}")
            finally:
                try:
                    tar_path.unlink(missing_ok=True)
                except Exception:
                    pass
        return remote_tar, deletes

    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        context = context or {}
        target = self._ssh_target()
        run_id = str(context.get("run_id") or "").strip() or uuid.uuid4().hex
        started_at = str(context.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
        cmdline_vars = dict(context.get("commandline_vars") or {})
        exec_env = dict(context.get("execution_env") or {})
        global_vars = dict(context.get("global_vars") or {})
        provenance = dict(context.get("provenance") or {})

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

        require_clean = not bool(context.get("allow_dirty_git", False))
        try:
            spec = resolve_execution_spec(
                repo_root=self.repo_root,
                provenance=provenance,
                require_clean=require_clean,
                require_origin=True,
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Could not prepare git-pinned execution source: {exc}") from exc

        git_remote_override = str(exec_env.get("git_remote_url") or "").strip()
        if git_remote_override:
            spec = replace(spec, origin_url=git_remote_override, repo_name=infer_repo_name(git_remote_override))
        if not spec.origin_url:
            raise RuntimeError("hpcc_direct requires git origin URL for remote checkout.")

        remote_base = str(self.remote_repo or "").strip() or "~/.etl"
        repo_root_remote = (Path(remote_base) / f"{spec.repo_name}-{spec.commit_sha[:12]}").as_posix()
        pipeline_remote = self._map_repo_path_for_root(Path(pipeline_path), repo_root_remote, label="pipeline")
        plugins_remote = self._map_repo_path_for_root(self.plugins_dir.resolve(), repo_root_remote, label="plugins_dir")
        global_remote = (
            self._map_repo_path_for_root(self.global_config.resolve(), repo_root_remote, label="global_config")
            if self.global_config
            else None
        )

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
        ]
        if self.global_config and global_remote:
            batch_cmd += ["--global-config", shlex.quote(global_remote)]
        # hpcc_direct may run against older remote checkouts where the local
        # environment name does not exist. Pass resolved env values as --var
        # overrides instead of requiring remote environments config parity.
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
        env_override_vars = _flatten_scalar_vars("env", exec_env)
        for key, value in sorted(env_override_vars.items()):
            batch_cmd += ["--var", shlex.quote(f"{key}={value}")]
        for key, value in sorted(cmdline_vars.items()):
            if isinstance(value, dict):
                continue
            batch_cmd += ["--var", shlex.quote(f"{key}={value}")]

        lines = [
            "set -eo pipefail",
            # Common site profile often initializes MODULEPATH/Lmod on clusters.
            "set +u; [ -f /etc/profile ] && source /etc/profile || true; set -u",
            # Ensure Lmod function is available in non-interactive SSH shells.
            "if ! command -v module >/dev/null 2>&1; then "
            "  [ -f /usr/lmod/lmod/init/bash ] && source /usr/lmod/lmod/init/bash || true; "
            "fi",
            f"CHECKOUT_ROOT={shlex.quote(repo_root_remote)}",
            f"REPO_URL={shlex.quote(spec.origin_url)}",
            f"REPO_SHA={shlex.quote(spec.commit_sha)}",
            "mkdir -p \"$(dirname \\\"$CHECKOUT_ROOT\\\")\"",
            "rm -rf \"$CHECKOUT_ROOT\"",
            "git clone --no-checkout \"$REPO_URL\" \"$CHECKOUT_ROOT\"",
            "cd \"$CHECKOUT_ROOT\"",
            "git fetch --tags --prune origin",
            "git checkout --detach \"$REPO_SHA\"",
            "git reset --hard \"$REPO_SHA\"",
        ]
        overlay_tar_remote: Optional[str] = None
        overlay_deletes: list[str] = []
        if bool(context.get("allow_dirty_git", False)):
            overlay_tar_remote, overlay_deletes = self._stage_dirty_overlay(target=target, run_id=run_id)
            if overlay_tar_remote:
                lines.append(f"DIRTY_OVERLAY_TAR={shlex.quote(overlay_tar_remote)}")
                lines.append(
                    "if [ -f \"$DIRTY_OVERLAY_TAR\" ]; then "
                    "tar -xf \"$DIRTY_OVERLAY_TAR\" -C \"$CHECKOUT_ROOT\"; "
                    "rm -f -- \"$DIRTY_OVERLAY_TAR\"; "
                    "fi"
                )
            for rel in overlay_deletes:
                lines.append(f"rm -f -- {shlex.quote(rel)}")
        for module_name in self.remote_modules:
            mod = str(module_name or "").strip()
            if mod:
                lines.append(
                    "if command -v module >/dev/null 2>&1; then "
                    f"module load {shlex.quote(mod)}; "
                    "else "
                    f"echo '[hpcc_direct][WARN] module command not available; skipping {shlex.quote(mod)}' >&2; "
                    "fi"
                )
        if self.remote_conda_env:
            lines.append(f"source activate {shlex.quote(self.remote_conda_env)}")
        if self.remote_venv:
            lines.append(f"source {shlex.quote(self.remote_venv)}/bin/activate")
        else:
            lines.append(
                "VENV=\"$CHECKOUT_ROOT/.venv\"; "
                f"if [ ! -f \"$VENV/bin/activate\" ]; then {shlex.quote(self.remote_python)} -m venv \"$VENV\"; fi; "
                "source \"$VENV/bin/activate\""
            )
        lines.append(
            "if [ -f \"$CHECKOUT_ROOT/requirements.txt\" ]; then "
            f"{shlex.quote(self.remote_python)} -m pip install --ignore-installed -r \"$CHECKOUT_ROOT/requirements.txt\"; "
            "fi"
        )
        lines.append("export ETL_REPO_ROOT=\"$CHECKOUT_ROOT\"")
        lines.append("export PYTHONPATH=\"$CHECKOUT_ROOT:${PYTHONPATH:-}\"")
        lines.append(" ".join(batch_cmd))
        remote_script = "\n".join(lines)

        if self.dry_run:
            self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.QUEUED, message="dry-run")
            return SubmissionResult(run_id=run_id, message="dry-run")

        started_dt = datetime.utcnow()
        proc = self._run_ssh_script(target, remote_script)
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

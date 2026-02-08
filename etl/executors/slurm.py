"""
SLURM executor: submits the whole pipeline as a single SLURM job.

This first-cut executor runs the full pipeline inside one SLURM job using
the local runner on the compute node. It uses execution environment
settings provided via `--execution-config` / `--env`.

Future enhancement: expand batches/foreach into job arrays with dependencies.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..pipeline import Pipeline
from ..pipeline import parse_pipeline
from ..tracking import upsert_run_status


class SlurmSubmitError(RuntimeError):
    """Raised when sbatch submission fails."""


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


class SlurmExecutor(Executor):
    name = "slurm"

    def __init__(
        self,
        env_config: Dict[str, Any],
        repo_root: Path,
        plugins_dir: Path = Path("plugins"),
        workdir: Path = Path(".runs"),
        global_config: Optional[Path] = None,
        execution_config: Optional[Path] = None,
        env_name: Optional[str] = None,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        # filter known SlurmEnv fields
        env_kwargs = {k: v for k, v in env_config.items() if k in {
            "partition", "account", "time", "cpus_per_task", "mem",
            "logdir", "workdir", "modules", "conda_env", "sbatch_extra",
            "ssh_host", "ssh_user", "ssh_jump", "remote_repo", "sync",
            "venv", "requirements", "python", "step_max_retries", "step_retry_delay_seconds"
        }}
        self.env = SlurmEnv(**env_kwargs)
        # Limits/concurrency hints; used by future array/dependency planner.
        self.job_limit = int(env_config.get("job_limit", 1000))
        self.array_task_limit = int(env_config.get("array_task_limit", 1000))
        self.max_parallel = int(env_config.get("max_parallel", 50))
        self.ssh_timeout = int(env_config.get("ssh_timeout", 120))
        self.scp_timeout = int(env_config.get("scp_timeout", 300))
        self.step_max_retries = int(env_config.get("step_max_retries", 0))
        self.step_retry_delay_seconds = float(env_config.get("step_retry_delay_seconds", 0.0))
        self.local_repo_name = Path(repo_root).name
        self.remote_base = Path(env_config.get("remote_repo") or repo_root)
        self.repo_root = self.remote_base / self.local_repo_name
        self.plugins_dir = plugins_dir
        self.workdir = workdir
        self.global_config = global_config
        self.execution_config = execution_config
        self.env_name = env_name
        self.dry_run = dry_run
        self.verbose = verbose
        self.database_url = self._load_database_url()
        self._statuses: Dict[str, RunStatus] = {}

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

    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        pipeline_path = Path(pipeline_path).as_posix()
        resume_run_id = context.get("resume_run_id")
        provenance = context.get("provenance")
        run_id = context.get("run_id")
        if not run_id:
            import uuid

            run_id = uuid.uuid4().hex
        ts = datetime.utcnow()
        run_date = ts.strftime("%y%m%d")
        run_stamp = ts.strftime("%H%M%S")
        run_fs_id = f"{run_stamp}-{run_id[:8]}"
        pipeline = parse_pipeline(Path(pipeline_path))
        batches = self._group_steps_with_indices(pipeline.steps)
        submission_records = []
        prev_jobid = None
        jobname = str(pipeline.vars.get("jobname", "run"))
        remote_workdir_root = Path(self.env.workdir or self.workdir) / jobname / run_date / run_fs_id
        remote_workdir = remote_workdir_root.as_posix()
        context_file = f"{remote_workdir}/context.json"
        repo_posix = Path(self.repo_root).as_posix()
        checkout_root = (self.remote_base / self.local_repo_name).as_posix()
        venv_path = (Path(self.env.venv) if self.env.venv else Path(checkout_root) / ".venv").as_posix()
        req_path = (Path(self.env.requirements).as_posix() if self.env.requirements else (Path(checkout_root) / "requirements.txt").as_posix())
        python_bin = self.env.python or "python3"
        # resolve pipeline and plugins dir to POSIX paths on remote checkout
        if Path(pipeline_path).is_absolute():
            pipeline_remote = Path(pipeline_path).as_posix()
        else:
            pipeline_remote = (self.repo_root / Path(pipeline_path)).as_posix()
        plugins_remote = (self.repo_root / self.plugins_dir).as_posix() if not self.plugins_dir.is_absolute() else self.plugins_dir.as_posix()
        base_logdir = Path(self.env.logdir or (self.workdir / "slurm_logs"))

        if self.env.ssh_host and self.env.sync:
            self._sync_repo()

        # submit setup job to prep venv and work dirs
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
                label = step_name
                step_workdir = (remote_workdir_root / step_name).as_posix()
                step_logdir = (base_logdir / jobname / step_name / run_date / run_fs_id).as_posix()
                script_text = self._render_batch_script(run_id, checkout_root, pipeline_remote, steps, step_indices, context_file, step_workdir, plugins_remote, step_logdir, venv_path, req_path, python_bin, resume_run_id=resume_run_id, array_index=False)
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
                    step_logdir = (base_logdir / jobname / label / run_date / run_fs_id).as_posix()
                    script_text = self._render_batch_script(run_id, checkout_root, pipeline_remote, chunk_steps, chunk_indices, context_file, step_workdir, plugins_remote, step_logdir, venv_path, req_path, python_bin, resume_run_id=resume_run_id, array_index=True)
                    jobid = self._submit_script(script_text, run_id, label=label, prev_dependency=prev_jobid, array_bounds=(0, len(chunk)-1), remote_dest_dir=step_workdir)
                    prev_jobid = jobid
                    submission_records.append(jobid)
                    start += chunk_size

        status = RunStatus(run_id=run_id, state=RunState.QUEUED, message=f"submitted {len(submission_records)} jobs")
        self._statuses[run_id] = status
        upsert_run_status(
            run_id=run_id,
            pipeline=pipeline_path,
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
        lines.append(f"export PYTHONPATH={checkout_root}:$PYTHONPATH")

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

        module_path = (Path(checkout_root) / "etl" / "run_batch.py").as_posix()
        cmd = [
            "python",
            module_path,
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
        if self.execution_config and self.env_name:
            ec_path = Path(self.execution_config)
            ec_arg = ((Path(checkout_root) / ec_path).as_posix() if not ec_path.is_absolute() else ec_path.as_posix())
            cmd += ["--execution-config", ec_arg, "--env", self.env_name]

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
        resume_run_id: Optional[str] = None,
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
        lines.append(f"cd {checkout_root}")
        lines.append(f"PYTHON={python_bin}")
        lines.append(f"VENV={venv_path}")
        lines.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
        lines.append("source \"$VENV/bin/activate\"")
        lines.append(f"export PYTHONPATH={checkout_root}:$PYTHONPATH")

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

        module_path = (Path(checkout_root) / "etl" / "run_batch.py").as_posix()
        cmd = [
            "$VENV/bin/python",
            module_path,
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
        if resume_run_id:
            cmd += ["--resume-run-id", str(resume_run_id)]
        cmd += ["--max-retries", str(self.step_max_retries)]
        cmd += ["--retry-delay-seconds", str(self.step_retry_delay_seconds)]
        if self.global_config:
            gc_path = Path(self.global_config)
            gc_arg = ((Path(checkout_root) / gc_path).as_posix() if not gc_path.is_absolute() else gc_path.as_posix())
            cmd += ["--global-config", gc_arg]
        if self.execution_config and self.env_name:
            ec_path = Path(self.execution_config)
            ec_arg = ((Path(checkout_root) / ec_path).as_posix() if not ec_path.is_absolute() else ec_path.as_posix())
            cmd += ["--execution-config", ec_arg, "--env", self.env_name]

        lines.append(" ".join(cmd))

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
        lines.append(f"#SBATCH -J etl-setup-{run_id[:6]}")
        lines.append(f"#SBATCH -o {logdir}/etl-setup-{run_id}-%j.out")
        if self.env.sbatch_extra:
            for extra in self.env.sbatch_extra:
                lines.append(f"#SBATCH {extra}")

        lines.append("set -euo pipefail")
        lines.append(f"mkdir -p {logdir}")
        lines.append(f"mkdir -p {workdir}")
        for d in workdirs_to_create:
            lines.append(f"mkdir -p {d}")
        for d in logdirs_to_create:
            lines.append(f"mkdir -p {d}")
        lines.append(f"cd {checkout_root}")
        lines.append(f"PYTHON={python_bin}")
        lines.append(f"VENV={venv_path}")
        lines.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
        lines.append("if [ ! -f \"$VENV/bin/activate\" ]; then")
        lines.append("  $PYTHON -m venv \"$VENV\"")
        lines.append("fi")
        lines.append("source \"$VENV/bin/activate\"")
        lines.append(f"if [ -f \"{req_path}\" ]; then pip install -r \"{req_path}\"; fi")
        lines.append(f"export PYTHONPATH={checkout_root}:$PYTHONPATH")
        lines.append(f"mkdir -p {workdir}")
        lines.append("echo setup complete")
        return "\n".join(lines)

    def _submit_script(self, script_text: str, run_id: str, label: str = "job", prev_dependency: Optional[str] = None, array_bounds: Optional[Tuple[int, int]] = None, remote_dest_dir: Optional[str] = None) -> str:
        dependency_arg = []
        if prev_dependency:
            dependency_arg = [f"--dependency=afterok:{prev_dependency}"]

        if self.dry_run:
            return "dry-run"

        if self.env.ssh_host:
            target = f"{self.env.ssh_user + '@' if self.env.ssh_user else ''}{self.env.ssh_host}"
            self._ensure_remote_secrets_file(target)
            remote_dir = remote_dest_dir or self.env.remote_repo or "/tmp"
            remote_file = f"{remote_dir}/etl-{run_id}-{label}.sbatch"
            # ensure remote dir
            mkdir_cmd = ["ssh"] + (["-J", self.env.ssh_jump] if self.env.ssh_jump else []) + [target, f"mkdir -p {remote_dir}"]
            if self.verbose:
                print("SSH mkdir:", " ".join(mkdir_cmd))
            proc = subprocess.run(mkdir_cmd, capture_output=True, text=True, timeout=self.ssh_timeout)
            if proc.returncode != 0:
                raise SlurmSubmitError(proc.stderr or proc.stdout)
            # write temp file locally with LF and scp it
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sbatch", prefix="etl-", newline="\n") as tmp:
                tmp.write(script_text.replace("\r\n", "\n"))
                tmp_path = Path(tmp.name)
            scp_cmd = ["scp"] + (["-J", self.env.ssh_jump] if self.env.ssh_jump else []) + [str(tmp_path), f"{target}:{remote_file}"]
            if self.verbose:
                print("SCP script:", " ".join(scp_cmd))
            proc_scp = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=self.scp_timeout)
            if proc_scp.returncode != 0:
                raise SlurmSubmitError(proc_scp.stderr or proc_scp.stdout)
            # submit remotely
            remote_cmd = ["ssh"] + (["-J", self.env.ssh_jump] if self.env.ssh_jump else []) + [target, "sbatch"] + dependency_arg + [remote_file]
            if self.verbose:
                print("SSH sbatch:", " ".join(remote_cmd))
            proc_submit = subprocess.run(remote_cmd, capture_output=True, text=True, timeout=self.ssh_timeout)
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
        cmd = ["ssh"] + (["-J", self.env.ssh_jump] if self.env.ssh_jump else []) + [target, remote_script]
        if self.verbose:
            print("SSH secrets init: ~/.secrets/etl")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.ssh_timeout)
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
        mkdir_cmd = ["ssh", target, f"mkdir -p {self.env.remote_repo}"]
        proc = subprocess.run(mkdir_cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise SlurmSubmitError(proc.stderr or proc.stdout)
        # Use scp to sync repo (simple recursive copy)
        scp_cmd = ["scp", "-r", str(Path(".").resolve()), remote_path]
        proc2 = subprocess.run(scp_cmd, capture_output=True, text=True)
        if proc2.returncode != 0:
            raise SlurmSubmitError(proc2.stderr or proc2.stdout)

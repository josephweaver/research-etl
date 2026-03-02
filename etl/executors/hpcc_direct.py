# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
HPCC direct executor: run batch worker directly over SSH on a dev node.

This bypasses SLURM scheduling and calls `python -m etl.run_batch` remotely.
Intended for fast development iteration only.
"""

from __future__ import annotations

from dataclasses import replace
import logging
import os
import tarfile
import tempfile
import shlex
import subprocess
import threading
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import (
    GitExecutionSpec,
    infer_repo_name,
)
from ..pipeline import parse_pipeline, PipelineError
from ..source_control import SourceControlError, SourceExecutionSpec, make_git_source_provider
from ..subprocess_logging import run_logged_subprocess

_SOURCE_PROVIDER = make_git_source_provider()
_LOG = logging.getLogger("etl.executors.hpcc_direct")


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


DEFAULT_SECRET_ENV_KEYS = ("ETL_DATABASE_URL", "OPENAI_API_KEY", "GITHUB_TOKEN")


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


def _parse_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(x).strip() for x in value]
    else:
        raw_items = [x.strip() for x in str(value).replace(";", ",").split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


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
            raise RuntimeError(f"Invalid step index in context.step_indices: {text}") from exc
        if idx < 0 or idx >= int(step_count):
            raise RuntimeError(f"Step index out of range in context.step_indices: {idx} (step_count={step_count})")
        if idx in seen:
            continue
        seen.add(idx)
        out.append(idx)
    out.sort()
    return out


def _last_non_empty_text(value: Any) -> str:
    lines = str(value or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for line in reversed(lines):
        text = str(line or "").strip()
        if text:
            return text
    return ""


class HpccDirectExecutor(Executor):
    name = "hpcc_direct"

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
    ) -> None:
        self.env_config = dict(env_config or {})
        self.repo_root = Path(repo_root).resolve()
        self.plugins_dir = Path(plugins_dir)
        self.workdir = Path(workdir)
        self.global_config = Path(global_config) if global_config else None
        self.projects_config = Path(projects_config) if projects_config else None
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
        self.ssh_connect_timeout = max(1, int(self.env_config.get("ssh_connect_timeout", self.ssh_timeout)))
        self.stream_setup_output = bool(self.env_config.get("stream_setup_output", True))
        self.stream_run_batch_output = bool(self.env_config.get("stream_run_batch_output", True))
        self.propagate_secrets = bool(self.env_config.get("propagate_secrets", True))
        self.load_secrets_file = bool(self.env_config.get("load_secrets_file", True))
        self.database_url = str(self.env_config.get("database_url") or "").strip() or None
        self.db_tunnel_command = str(self.env_config.get("db_tunnel_command") or "").strip()
        self.secret_env_keys = _parse_str_list(self.env_config.get("secret_env_keys")) or list(DEFAULT_SECRET_ENV_KEYS)
        self.required_secret_keys = _parse_str_list(self.env_config.get("required_secret_keys"))
        self._statuses: Dict[str, RunStatus] = {}

    def capabilities(self) -> Dict[str, bool]:
        return {
            "cancel": False,
            "artifact_tree": False,
            "artifact_file": False,
            "query_data": False,
        }

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
            f"ConnectTimeout={self.ssh_connect_timeout}",
            "-o",
            "ConnectionAttempts=1",
        ]
        return args

    def _map_repo_path_for_root(
        self,
        path: Path,
        remote_root: str,
        label: str = "path",
        fallback_relative: Optional[str] = None,
    ) -> str:
        try:
            rel = repo_relative_path(path, self.repo_root, label)
            return (Path(remote_root) / rel).as_posix()
        except SourceControlError:
            if str(fallback_relative or "").strip():
                return (Path(remote_root) / Path(str(fallback_relative))).as_posix()
            return path.as_posix()

    def _run_ssh_script(self, target: str, script: str, *, stream_output: bool = False) -> subprocess.CompletedProcess:
        ssh_cmd = [
            "ssh",
            *self._ssh_common_args(),
            target,
            f"bash --login -lc {shlex.quote(script)}",
        ]
        if not stream_output:
            return run_logged_subprocess(
                ssh_cmd,
                logger=_LOG,
                action="hpcc_direct.ssh",
                timeout=self.ssh_timeout,
                check=False,
            )
        proc = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def _reader(stream, buf: list[str], stream_name: str) -> None:
            if stream is None:
                return
            try:
                for line in iter(stream.readline, ""):
                    buf.append(line)
                    text = line.rstrip("\r\n")
                    if text:
                        _LOG.info("[hpcc_direct][%s] %s", stream_name, text)
                        print(f"[hpcc_direct][{stream_name}] {text}", flush=True)
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout, stdout_parts, "stdout"), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_parts, "stderr"), daemon=True)
        t_out.start()
        t_err.start()
        try:
            rc = proc.wait(timeout=self.ssh_timeout)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass
            t_out.join(timeout=2)
            t_err.join(timeout=2)
            raise
        t_out.join(timeout=2)
        t_err.join(timeout=2)
        return subprocess.CompletedProcess(ssh_cmd, rc, "".join(stdout_parts), "".join(stderr_parts))

    def _build_remote_script(self, lines: list[str]) -> str:
        base = [
            "set -eo pipefail",
            # Common site profile often initializes MODULEPATH/Lmod on clusters.
            "set +u; [ -f /etc/profile ] && source /etc/profile || true; set -u",
            # Ensure Lmod function is available in non-interactive SSH shells.
            "if ! command -v module >/dev/null 2>&1; then "
            "  [ -f /usr/lmod/lmod/init/bash ] && source /usr/lmod/lmod/init/bash || true; "
            "fi",
        ]
        return "\n".join(base + list(lines or []))

    def _run_stage(
        self,
        *,
        target: str,
        stage_name: str,
        lines: list[str],
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess:
        proc = self._run_ssh_script(target, self._build_remote_script(lines), stream_output=stream_output)
        if proc.returncode == 0:
            return proc
        stderr = str(proc.stderr or "").strip()
        stdout = str(proc.stdout or "").strip()
        detail = stderr or stdout
        if not detail and stream_output:
            detail = "remote output streamed to console"
        raise RuntimeError(f"{stage_name} failed rc={proc.returncode}: {detail[:4000]}")

    def _scp_to_remote(self, target: str, local_path: Path, remote_path: str) -> subprocess.CompletedProcess:
        scp_cmd = [
            "scp",
            *self._ssh_common_args(),
            str(local_path),
            f"{target}:{remote_path}",
        ]
        return run_logged_subprocess(
            scp_cmd,
            logger=_LOG,
            action="hpcc_direct.scp",
            timeout=self.ssh_timeout,
            check=False,
        )

    def _collect_dirty_overlay_paths(self) -> tuple[list[Path], list[str]]:
        proc = run_logged_subprocess(
            [
                "git",
                "-C",
                str(self.repo_root),
                "status",
                "--porcelain=1",
                "-z",
                "--untracked-files=all",
            ],
            logger=_LOG,
            action="hpcc_direct.git_status",
            text=False,
            timeout=self.ssh_timeout,
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

    def _collect_secret_exports(self, context: Dict[str, Any]) -> Dict[str, str]:
        key_candidates = _parse_str_list(context.get("secret_env_keys")) or list(self.secret_env_keys)
        required = _parse_str_list(context.get("required_secret_keys")) or list(self.required_secret_keys)
        out: Dict[str, str] = {}
        for key in key_candidates:
            val = os.environ.get(key)
            if val is None:
                continue
            text = str(val).strip()
            if not text:
                continue
            out[key] = text
        if self.database_url:
            out["ETL_DATABASE_URL"] = self.database_url
        missing_required = [k for k in required if k not in out]
        if missing_required:
            raise RuntimeError(
                "Missing required local secrets for hpcc_direct: " + ", ".join(sorted(missing_required))
            )
        return out

    def _stage_secret_exports(self, *, target: str, run_id: str, secrets: Dict[str, str]) -> Optional[str]:
        if not secrets:
            return None
        local_tmp = Path(tempfile.gettempdir()) / f"hpcc_direct_secrets_{run_id}.env"
        remote_tmp = f"/tmp/hpcc_direct_secrets_{run_id}.env"
        lines = [f"export {key}={shlex.quote(value)}" for key, value in sorted(secrets.items())]
        try:
            local_tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
            scp_proc = self._scp_to_remote(target, local_tmp, remote_tmp)
            if scp_proc.returncode != 0:
                detail = (scp_proc.stderr or scp_proc.stdout or "").strip()
                raise RuntimeError(f"Failed to transfer staged secrets file: {detail[:2000]}")
        finally:
            try:
                local_tmp.unlink(missing_ok=True)
            except Exception:
                pass
        return remote_tmp

    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        context = context or {}
        target = self._ssh_target()
        run_id = str(context.get("run_id") or "").strip() or uuid.uuid4().hex
        _LOG.info("hpcc_direct submit start run_id=%s pipeline=%s", run_id, pipeline_path)
        started_at = str(context.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
        cmdline_vars = dict(context.get("commandline_vars") or {})
        exec_env = dict(context.get("execution_env") or {})
        project_vars = dict(context.get("project_vars") or {})
        global_vars = dict(context.get("global_vars") or {})
        provenance = dict(context.get("provenance") or {})

        try:
            pipeline = parse_pipeline(
                Path(pipeline_path),
                global_vars=global_vars,
                env_vars=exec_env,
                project_vars=project_vars,
                context_vars=cmdline_vars,
            )
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc
        requested_indices = _parse_step_indices(context.get("step_indices"), len(pipeline.steps))
        step_indices = ",".join(str(i) for i in (requested_indices or list(range(len(pipeline.steps)))))
        if not step_indices:
            _LOG.info("hpcc_direct no steps to execute run_id=%s", run_id)
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
        pipeline_remote = self._map_repo_path_for_root(
            Path(pipeline_path),
            repo_root_remote,
            label="pipeline",
            fallback_relative=str(context.get("pipeline_remote_hint") or "").strip() or None,
        )
        plugins_remote = self._map_repo_path_for_root(self.plugins_dir.resolve(), repo_root_remote, label="plugins_dir")
        global_remote = (
            self._map_repo_path_for_root(self.global_config.resolve(), repo_root_remote, label="global_config")
            if self.global_config
            else None
        )
        projects_remote = (
            self._map_repo_path_for_root(self.projects_config.resolve(), repo_root_remote, label="projects_config")
            if self.projects_config
            else None
        )

        # Execute run_batch with the interpreter from the activated runtime
        # environment (venv/conda), not the bootstrap interpreter.
        batch_cmd = [
            "python",
            "-u",
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
        context_file = str(context.get("context_file") or "").strip()
        if context_file:
            batch_cmd += ["--context-file", shlex.quote(context_file)]
        if self.global_config and global_remote:
            batch_cmd += ["--global-config", shlex.quote(global_remote)]
        if self.projects_config and projects_remote:
            batch_cmd += ["--projects-config", shlex.quote(projects_remote)]
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

        checkout_lines = [
            f"CHECKOUT_ROOT={shlex.quote(repo_root_remote)}",
            f"REPO_URL={shlex.quote(spec.origin_url)}",
            f"REPO_SHA={shlex.quote(spec.commit_sha)}",
            "mkdir -p \"$(dirname \\\"$CHECKOUT_ROOT\\\")\"",
            "if [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then git clone --no-checkout \"$REPO_URL\" \"$CHECKOUT_ROOT\"; fi",
            "cd \"$CHECKOUT_ROOT\"",
            "git remote set-url origin \"$REPO_URL\" || true",
            "git fetch --tags --prune origin",
            "git checkout --detach \"$REPO_SHA\"",
            "git reset --hard \"$REPO_SHA\"",
            "git clean -fd",
        ]
        staged_secrets_remote: Optional[str] = None
        if self.propagate_secrets and not self.dry_run:
            secret_exports = self._collect_secret_exports(context)
            staged_secrets_remote = self._stage_secret_exports(target=target, run_id=run_id, secrets=secret_exports)
        secrets_lines: list[str] = []
        if staged_secrets_remote:
            secrets_lines.extend(
                [
                    "mkdir -p \"$HOME/.secrets\"",
                    "chmod 700 \"$HOME/.secrets\"",
                    "touch \"$HOME/.secrets/etl\"",
                    "chmod 600 \"$HOME/.secrets/etl\"",
                    f"STAGED_SECRETS={shlex.quote(staged_secrets_remote)}",
                    "if [ -f \"$STAGED_SECRETS\" ]; then",
                    "  cp \"$HOME/.secrets/etl\" \"$HOME/.secrets/etl.tmp\" || true",
                    "  while IFS= read -r line; do",
                    "    key=$(printf '%s' \"$line\" | sed -n 's/^export[[:space:]]\\+\\([A-Za-z_][A-Za-z0-9_]*\\)=.*/\\1/p')",
                    "    [ -n \"$key\" ] || continue",
                    "    grep -v \"^export $key=\" \"$HOME/.secrets/etl.tmp\" > \"$HOME/.secrets/etl.tmp2\" || true",
                    "    mv \"$HOME/.secrets/etl.tmp2\" \"$HOME/.secrets/etl.tmp\"",
                    "    printf '%s\\n' \"$line\" >> \"$HOME/.secrets/etl.tmp\"",
                    "  done < \"$STAGED_SECRETS\"",
                    "  mv \"$HOME/.secrets/etl.tmp\" \"$HOME/.secrets/etl\"",
                    "  chmod 600 \"$HOME/.secrets/etl\"",
                    "  rm -f -- \"$STAGED_SECRETS\"",
                    "fi",
                ]
            )
        overlay_tar_remote: Optional[str] = None
        overlay_deletes: list[str] = []
        if bool(context.get("allow_dirty_git", False)):
            overlay_tar_remote, overlay_deletes = self._stage_dirty_overlay(target=target, run_id=run_id)
        overlay_lines: list[str] = [f"CHECKOUT_ROOT={shlex.quote(repo_root_remote)}"]
        if overlay_tar_remote:
            overlay_lines.append(f"DIRTY_OVERLAY_TAR={shlex.quote(overlay_tar_remote)}")
            overlay_lines.append(
                "if [ -f \"$DIRTY_OVERLAY_TAR\" ]; then "
                "tar -xf \"$DIRTY_OVERLAY_TAR\" -C \"$CHECKOUT_ROOT\"; "
                "rm -f -- \"$DIRTY_OVERLAY_TAR\"; "
                "fi"
            )
        for rel in overlay_deletes:
            overlay_lines.append(f"rm -f -- {shlex.quote(rel)}")
        runtime_lines: list[str] = [f"CHECKOUT_ROOT={shlex.quote(repo_root_remote)}", "cd \"$CHECKOUT_ROOT\""]
        for module_name in self.remote_modules:
            mod = str(module_name or "").strip()
            if mod:
                runtime_lines.append(
                    "if command -v module >/dev/null 2>&1; then "
                    f"module load {shlex.quote(mod)}; "
                    "else "
                    f"echo '[hpcc_direct][WARN] module command not available; skipping {shlex.quote(mod)}' >&2; "
                    "fi"
                )
        if self.remote_conda_env:
            runtime_lines.append(f"source activate {shlex.quote(self.remote_conda_env)}")
        if self.remote_venv:
            runtime_lines.append(f"source {shlex.quote(self.remote_venv)}/bin/activate")
        else:
            runtime_lines.append(
                "VENV=\"$CHECKOUT_ROOT/.venv\"; "
                f"if [ ! -f \"$VENV/bin/activate\" ]; then {shlex.quote(self.remote_python)} -m venv \"$VENV\"; fi; "
                "source \"$VENV/bin/activate\""
            )
        runtime_lines.append(
            "if [ -f \"$CHECKOUT_ROOT/requirements.txt\" ]; then "
            "REQ_STAMP=\"$CHECKOUT_ROOT/.venv/.requirements.lock\"; "
            "if [ ! -f \"$REQ_STAMP\" ] || ! cmp -s \"$CHECKOUT_ROOT/requirements.txt\" \"$REQ_STAMP\"; then "
            "python -m pip install --ignore-installed -r \"$CHECKOUT_ROOT/requirements.txt\"; "
            "cp \"$CHECKOUT_ROOT/requirements.txt\" \"$REQ_STAMP\"; "
            "fi; "
            "fi"
        )
        runtime_lines.append(
            "if ! python -c 'import etl.run_batch' >/dev/null 2>&1; then "
            "python -m pip install --no-deps -e \"$CHECKOUT_ROOT\"; "
            "fi"
        )
        run_lines = [f"CHECKOUT_ROOT={shlex.quote(repo_root_remote)}", "cd \"$CHECKOUT_ROOT\""]
        for module_name in self.remote_modules:
            mod = str(module_name or "").strip()
            if mod:
                run_lines.append(
                    "if command -v module >/dev/null 2>&1; then "
                    f"module load {shlex.quote(mod)}; "
                    "else "
                    f"echo '[hpcc_direct][WARN] module command not available; skipping {shlex.quote(mod)}' >&2; "
                    "fi"
                )
        if self.remote_conda_env:
            run_lines.append(f"source activate {shlex.quote(self.remote_conda_env)}")
        if self.remote_venv:
            run_lines.append(f"source {shlex.quote(self.remote_venv)}/bin/activate")
        else:
            run_lines.append("source \"$CHECKOUT_ROOT/.venv/bin/activate\"")
        if self.db_tunnel_command:
            run_lines.append(self.db_tunnel_command)
        if self.load_secrets_file:
            run_lines.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
        db_mode = str(exec_env.get("db_mode") or "").strip()
        if db_mode:
            run_lines.append(f"export ETL_DB_MODE={shlex.quote(db_mode)}")
        if exec_env.get("db_verbose") is not None:
            run_lines.append(f"export ETL_DB_VERBOSE={'1' if _parse_bool(exec_env.get('db_verbose')) else '0'}")
        run_lines.append("export PYTHONUNBUFFERED=1")
        run_lines.append("export ETL_REPO_ROOT=\"$CHECKOUT_ROOT\"")
        run_lines.append("export PYTHONPATH=\"$CHECKOUT_ROOT:${PYTHONPATH:-}\"")
        context_seed = context.get("seed_context")
        if context_file and isinstance(context_seed, dict):
            context_json = json.dumps(context_seed, separators=(",", ":"), default=str)
            run_lines.extend(
                [
                    f"CONTEXT_FILE={shlex.quote(context_file)}",
                    "mkdir -p \"$(dirname \\\"$CONTEXT_FILE\\\")\"",
                    f"cat > \"$CONTEXT_FILE\" <<'ETL_CONTEXT_JSON'\n{context_json}\nETL_CONTEXT_JSON",
                ]
            )
        run_lines.append(" ".join(batch_cmd))

        if self.dry_run:
            _LOG.info("hpcc_direct dry-run run_id=%s", run_id)
            self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.QUEUED, message="dry-run")
            return SubmissionResult(run_id=run_id, message="dry-run")

        started_dt = datetime.utcnow()
        try:
            self._run_stage(
                target=target,
                stage_name="prepare_checkout",
                lines=checkout_lines,
                stream_output=(self.verbose or self.stream_setup_output),
            )
            if secrets_lines:
                self._run_stage(
                    target=target,
                    stage_name="apply_secrets",
                    lines=secrets_lines,
                    stream_output=(self.verbose or self.stream_setup_output),
                )
            if len(overlay_lines) > 1:
                self._run_stage(
                    target=target,
                    stage_name="apply_dirty_overlay",
                    lines=overlay_lines,
                    stream_output=(self.verbose or self.stream_setup_output),
                )
            self._run_stage(
                target=target,
                stage_name="setup_runtime",
                lines=runtime_lines,
                stream_output=(self.verbose or self.stream_setup_output),
            )
            proc = self._run_stage(
                target=target,
                stage_name="run_batch",
                lines=run_lines,
                stream_output=(self.verbose or self.stream_run_batch_output),
            )
        except Exception as exc:  # noqa: BLE001
            _LOG.exception("hpcc_direct submit failed run_id=%s: %s", run_id, exc)
            ended_dt = datetime.utcnow()
            status = RunStatus(
                run_id=run_id,
                state=RunState.FAILED,
                message=str(exc)[:4000],
                started_at=started_dt,
                ended_at=ended_dt,
            )
            self._statuses[run_id] = status
            return SubmissionResult(run_id=run_id, message=status.message)
        ended_dt = datetime.utcnow()
        stdout = str(proc.stdout or "").strip()
        stderr = str(proc.stderr or "").strip()
        detail = stderr or stdout or ("remote output streamed to console" if self.verbose else "")
        summary = _last_non_empty_text(detail) or detail
        if proc.returncode == 0:
            status = RunStatus(
                run_id=run_id,
                state=RunState.SUCCEEDED,
                message=summary[:4000],
                started_at=started_dt,
                ended_at=ended_dt,
            )
        else:
            status = RunStatus(
                run_id=run_id,
                state=RunState.FAILED,
                message=f"remote run_batch rc={proc.returncode}: {summary[:4000]}",
                started_at=started_dt,
                ended_at=ended_dt,
            )
        self._statuses[run_id] = status
        _LOG.info("hpcc_direct submit complete run_id=%s state=%s", run_id, status.state.value)
        return SubmissionResult(run_id=run_id, message=status.message)

    def status(self, run_id: str) -> RunStatus:
        if run_id not in self._statuses:
            return RunStatus(run_id=run_id, state=RunState.FAILED, message="Unknown run_id")
        return self._statuses[run_id]

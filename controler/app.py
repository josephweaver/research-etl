from __future__ import annotations

import csv
from collections import defaultdict
import fnmatch
import glob
import json
import io
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from etl.execution_config import (
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_config_path,
    resolve_execution_env_templates,
)
from etl.provisioners.base import ProvisionHandle, ProvisionState, WorkloadSpec
from etl.provisioners.slurm import SlurmProvisioner
from etl.transports import LocalProcessTransport, SshTransport


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"config must decode to a mapping: {path}")
    return data


def _format_map(value: str, mapping: dict[str, Any]) -> str:
    class _Default(dict):
        def __missing__(self, key: str) -> str:
            return "{" + key + "}"

    return str(value).format_map(_Default(mapping))


@dataclass
class ControllerPaths:
    state_path: Path
    local_wave_dir: Path
    remote_wave_dir: str
    remote_submit_dir: str
    remote_log_dir: str


@dataclass
class CountyRecord:
    fips: str
    checkpoint_path: str
    log_path: str | None
    status: str
    completed_iter: int | None
    target_iter: int | None
    state_reason: str
    checkpoint: dict[str, Any]


def _single_county_record(fips: str) -> CountyRecord:
    target = str(fips or "").strip()
    return CountyRecord(
        fips=target,
        checkpoint_path="",
        log_path=None,
        status="pending",
        completed_iter=None,
        target_iter=None,
        state_reason="manual_single",
        checkpoint={},
    )


class ControllerApp:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path).expanduser().resolve()
        self.raw = _load_yaml(self.config_path)
        self.controller_cfg = dict(self.raw.get("controller") or {})
        self.checkpoint_cfg = dict(self.raw.get("checkpoint") or {})
        self.slurm_cfg = dict(self.raw.get("slurm") or {})
        self.worker_cfg = dict(self.raw.get("worker") or {})
        self.paths = self._build_paths()
        self.env_name, self.exec_env = self._load_exec_env()
        self.transport = self._build_transport()
        self.provisioner = SlurmProvisioner(self.transport)

    def _build_paths(self) -> ControllerPaths:
        state_path = Path(str(self.controller_cfg.get("state_path") or "controler/state.json")).expanduser().resolve()
        local_wave_dir = Path(str(self.controller_cfg.get("local_wave_dir") or "controler/waves")).expanduser().resolve()
        remote_wave_dir = str(self.controller_cfg.get("remote_wave_dir") or local_wave_dir.as_posix()).strip()
        remote_submit_dir = str(self.controller_cfg.get("remote_submit_dir") or remote_wave_dir).strip()
        remote_log_dir = str(self.controller_cfg.get("remote_log_dir") or f"{remote_wave_dir}/logs").strip()
        return ControllerPaths(
            state_path=state_path,
            local_wave_dir=local_wave_dir,
            remote_wave_dir=remote_wave_dir,
            remote_submit_dir=remote_submit_dir,
            remote_log_dir=remote_log_dir,
        )

    def _load_exec_env(self) -> tuple[str | None, dict[str, Any]]:
        env_name = str(self.slurm_cfg.get("environment") or "").strip() or None
        env_cfg_path_raw = self.slurm_cfg.get("environments_config")
        if not env_name or not env_cfg_path_raw:
            return None, {}
        cfg_path = resolve_execution_config_path(Path(str(env_cfg_path_raw)))
        if cfg_path is None:
            raise ValueError(f"could not resolve environments config: {env_cfg_path_raw}")
        envs = load_execution_config(cfg_path)
        if env_name not in envs:
            raise ValueError(f"environment not found in execution config: {env_name}")
        env = apply_execution_env_overrides(envs[env_name])
        resolved = resolve_execution_env_templates(env)
        return env_name, resolved

    def _build_transport(self):
        ssh_host = str(self.exec_env.get("ssh_host") or self.slurm_cfg.get("ssh_host") or "").strip()
        if ssh_host:
            return SshTransport(
                ssh_host=ssh_host,
                ssh_user=str(self.exec_env.get("ssh_user") or self.slurm_cfg.get("ssh_user") or "").strip() or None,
                ssh_jump=str(self.exec_env.get("ssh_jump") or "").strip() or None,
                ssh_connect_timeout=int(self.exec_env.get("ssh_connect_timeout") or 30),
                ssh_strict_host_key_checking=str(self.exec_env.get("ssh_strict_host_key_checking") or "accept-new"),
                timeout_seconds=int(self.exec_env.get("ssh_timeout") or 120),
                retries=int(self.exec_env.get("ssh_retries") or 0),
                retry_delay_seconds=float(self.exec_env.get("remote_retry_delay_seconds") or 0.0),
                verbose=bool(self.slurm_cfg.get("verbose", False)),
            )
        return LocalProcessTransport()

    def _load_state(self) -> dict[str, Any]:
        if not self.paths.state_path.exists():
            return {"submissions": []}
        try:
            payload = _read_json(self.paths.state_path)
        except Exception:
            return {"submissions": []}
        submissions = payload.get("submissions")
        if not isinstance(submissions, list):
            payload["submissions"] = []
        return payload

    def _save_state(self, state: dict[str, Any]) -> None:
        _write_json(self.paths.state_path, state)

    def _remote_python_bin(self) -> str:
        return str(self.worker_cfg.get("python_bin") or "python3").strip() or "python3"

    def _is_remote_controller(self) -> bool:
        return isinstance(self.transport, SshTransport)

    def _remote_run_json(self, script_body: str) -> Any:
        py = self._remote_python_bin()
        text = (
            "set -euo pipefail\n"
            f"{shlex.quote(py)} - <<'PY'\n"
            f"{script_body}\n"
            "PY\n"
        )
        result = self.transport.run_text(text, check=True)
        return json.loads(str(result.stdout or "").strip() or "null")

    def _path_exists(self, path_text: str) -> bool:
        path_value = str(path_text or "").strip()
        if not path_value:
            return False
        if self._is_remote_controller():
            payload = self._remote_run_json(
                "from pathlib import Path\n"
                "import json\n"
                f"path = Path({path_value!r})\n"
                "print(json.dumps(path.exists()))"
            )
            return bool(payload)
        return Path(path_value).expanduser().exists()

    def _read_text(self, path_text: str) -> str:
        path_value = str(path_text or "").strip()
        if not path_value:
            return ""
        if self._is_remote_controller():
            payload = self._remote_run_json(
                "from pathlib import Path\n"
                "import json\n"
                f"path = Path({path_value!r})\n"
                "text = path.read_text(encoding='utf-8', errors='replace') if path.exists() else ''\n"
                "print(json.dumps(text))"
            )
            return str(payload or "")
        return Path(path_value).expanduser().read_text(encoding="utf-8", errors="replace")

    def _checkpoint_paths(self) -> list[str]:
        pattern = str(self.controller_cfg.get("checkpoints_glob") or "").strip()
        if not pattern:
            raise ValueError("controller.checkpoints_glob is required")
        if self._is_remote_controller():
            payload = self._remote_run_json(
                "import glob, json\n"
                f"paths = sorted(glob.glob({pattern!r}))\n"
                "print(json.dumps(paths))"
            )
            return [str(x) for x in list(payload or [])]
        return [str(Path(p).expanduser().resolve().as_posix()) for p in glob.glob(pattern)]

    def _seed_counties(self) -> list[CountyRecord]:
        seed_dir = str(self.controller_cfg.get("seed_county_dir") or "").strip()
        if seed_dir:
            rows = self._seed_counties_from_dir(seed_dir)
            if rows:
                return rows
        manifest_path = str(self.controller_cfg.get("seed_manifest_csv") or "").strip()
        if not manifest_path:
            return []
        if not self._path_exists(manifest_path):
            return []
        fips_key = str(self.controller_cfg.get("seed_fips_key") or "focal_fips").strip() or "focal_fips"
        text = self._read_text(manifest_path)
        if not text.strip():
            return []
        reader = csv.DictReader(io.StringIO(text))
        rows: list[CountyRecord] = []
        seen: set[str] = set()
        for row in reader:
            fips = str((row or {}).get(fips_key) or "").strip()
            if not fips or fips in seen:
                continue
            seen.add(fips)
            rows.append(
                CountyRecord(
                    fips=fips,
                    checkpoint_path="",
                    log_path=None,
                    status="pending",
                    completed_iter=0,
                    target_iter=None,
                    state_reason="seed_manifest",
                    checkpoint={"seed_manifest_csv": manifest_path},
                )
            )
        return rows

    def _seed_counties_from_dir(self, root_dir: str) -> list[CountyRecord]:
        path_value = str(root_dir or "").strip()
        if not path_value or not self._path_exists(path_value):
            return []
        if self._is_remote_controller():
            payload = self._remote_run_json(
                "from pathlib import Path\n"
                "import json\n"
                f"root = Path({path_value!r})\n"
                "items = []\n"
                "if root.exists():\n"
                "    for child in sorted(root.iterdir()):\n"
                "        if child.is_dir():\n"
                "            items.append(child.name)\n"
                "print(json.dumps(items))"
            )
            names = [str(x).strip() for x in list(payload or [])]
        else:
            names = [child.name for child in sorted(Path(path_value).expanduser().iterdir()) if child.is_dir()]
        rows: list[CountyRecord] = []
        seen: set[str] = set()
        for name in names:
            fips = str(name or "").strip()
            if not fips or fips in seen:
                continue
            seen.add(fips)
            rows.append(
                CountyRecord(
                    fips=fips,
                    checkpoint_path="",
                    log_path=None,
                    status="pending",
                    completed_iter=0,
                    target_iter=None,
                    state_reason="seed_dir",
                    checkpoint={"seed_county_dir": path_value},
                )
            )
        return rows

    def _read_log_state(self, log_path: str | None) -> tuple[bool, bool]:
        if log_path is None or not self._path_exists(log_path):
            return False, False
        text = self._read_text(log_path)
        complete_markers = [str(x).lower() for x in list(self.controller_cfg.get("complete_markers") or ["process complete."])]
        continue_markers = [str(x).lower() for x in list(self.controller_cfg.get("continue_markers") or ["resume to process the next batch"])]
        lower = text.lower()
        return any(m in lower for m in complete_markers), any(m in lower for m in continue_markers)

    def _county_from_checkpoint(self, path: str) -> CountyRecord:
        payload = json.loads(self._read_text(path))
        fips_key = str(self.checkpoint_cfg.get("fips_key") or "fips")
        log_path_key = str(self.checkpoint_cfg.get("log_path_key") or "log_path")
        status_key = str(self.checkpoint_cfg.get("status_key") or "status")
        completed_iter_key = str(self.checkpoint_cfg.get("completed_iter_key") or "completed_iter")
        target_iter_key = str(self.checkpoint_cfg.get("target_iter_key") or "target_iter")
        fips = str(payload.get(fips_key) or Path(path).parent.name).strip()
        log_path_raw = str(payload.get(log_path_key) or "").strip()
        log_path = log_path_raw or None
        status = str(payload.get(status_key) or "").strip().lower()
        completed_iter = payload.get(completed_iter_key)
        target_iter = payload.get(target_iter_key)
        if completed_iter not in (None, ""):
            completed_iter = int(completed_iter)
        else:
            completed_iter = None
        if target_iter not in (None, ""):
            target_iter = int(target_iter)
        else:
            target_iter = None
        log_complete, log_continue = self._read_log_state(log_path)
        reason = "checkpoint"
        effective_status = status or "pending"
        if log_complete:
            effective_status = "complete"
            reason = "log_complete"
        elif log_continue:
            effective_status = "needs_more"
            reason = "log_continue"
        elif target_iter is not None and completed_iter is not None and completed_iter < target_iter:
            effective_status = "needs_more"
            reason = "iter_remaining"
        elif target_iter is not None and completed_iter is not None and completed_iter >= target_iter:
            effective_status = "complete"
            reason = "iter_target_met"
        return CountyRecord(
            fips=fips,
            checkpoint_path=path,
            log_path=log_path,
            status=effective_status,
            completed_iter=completed_iter,
            target_iter=target_iter,
            state_reason=reason,
            checkpoint=payload,
        )

    def counties(self) -> list[CountyRecord]:
        seeded = {county.fips: county for county in self._seed_counties()}
        for path in self._checkpoint_paths():
            county = self._county_from_checkpoint(path)
            seeded[county.fips] = county
        return [seeded[fips] for fips in sorted(seeded.keys())]

    def _filter_counties(self, counties: list[CountyRecord], fips_glob: str | None = None) -> list[CountyRecord]:
        pattern = str(fips_glob or "").strip()
        if not pattern:
            return counties
        return [county for county in counties if fnmatch.fnmatch(county.fips, pattern)]

    def _county_by_fips(self, fips: str) -> CountyRecord:
        target = str(fips or "").strip()
        for county in self.counties():
            if county.fips == target:
                return county
        raise KeyError(f"county not found for fips={target}")

    def _submission_status(self, job_id: str) -> ProvisionState:
        handle = ProvisionHandle(provisioner="slurm", backend_run_id=job_id, job_ids=[job_id])
        return self.provisioner.status(handle).state

    def _submission_statuses(self, job_ids: list[str]) -> dict[str, ProvisionState]:
        targets = [str(job_id or "").strip() for job_id in job_ids if str(job_id or "").strip()]
        if not targets:
            return {}
        unique_targets = sorted(set(targets))
        if len(unique_targets) == 1:
            only = unique_targets[0]
            return {only: self._submission_status(only)}
        result = self.transport.run(
            ["squeue", "-h", "-j", ",".join(unique_targets), "-o", "%i|%T"],
            check=False,
        )
        states_by_job: dict[str, list[ProvisionState]] = defaultdict(list)
        for raw_line in str(result.stdout or "").splitlines():
            line = raw_line.strip()
            if not line or "|" not in line:
                continue
            raw_job_id, raw_state = line.split("|", 1)
            base_job_id = raw_job_id.strip().split("_", 1)[0]
            if not base_job_id:
                continue
            states_by_job[base_job_id].append(self._map_provision_state(raw_state.strip()))
        resolved: dict[str, ProvisionState] = {}
        priority = [
            ProvisionState.RUNNING,
            ProvisionState.QUEUED,
            ProvisionState.PROVISIONING,
            ProvisionState.PENDING,
            ProvisionState.SUCCEEDED,
            ProvisionState.CANCELLED,
            ProvisionState.FAILED,
            ProvisionState.UNKNOWN,
        ]
        for job_id in unique_targets:
            seen_states = states_by_job.get(job_id, [])
            if not seen_states:
                resolved[job_id] = ProvisionState.UNKNOWN
                continue
            for candidate in priority:
                if candidate in seen_states:
                    resolved[job_id] = candidate
                    break
            else:
                resolved[job_id] = seen_states[0]
        return resolved

    @staticmethod
    def _map_provision_state(raw_state: str) -> ProvisionState:
        text = str(raw_state or "").strip().upper()
        if not text:
            return ProvisionState.UNKNOWN
        if text in {"PENDING", "CONFIGURING", "RESIZING", "SUSPENDED"}:
            return ProvisionState.QUEUED
        if text in {"RUNNING", "COMPLETING", "STAGE_OUT"}:
            return ProvisionState.RUNNING
        if text.startswith("COMPLETED"):
            return ProvisionState.SUCCEEDED
        if text.startswith("CANCELLED"):
            return ProvisionState.CANCELLED
        if text in {"FAILED", "BOOT_FAIL", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT"}:
            return ProvisionState.FAILED
        return ProvisionState.UNKNOWN

    def _active_fips(self, state: dict[str, Any]) -> set[str]:
        active: set[str] = set()
        changed = False
        submissions = list(state.get("submissions") or [])
        statuses = self._submission_statuses([str(item.get("job_id") or "") for item in submissions])
        for item in submissions:
            job_id = str(item.get("job_id") or "").strip()
            if not job_id:
                continue
            status = statuses.get(job_id, ProvisionState.UNKNOWN)
            item["scheduler_state"] = str(status.value)
            item["checked_at"] = _utc_now()
            if status in {ProvisionState.QUEUED, ProvisionState.RUNNING}:
                active.update(str(x) for x in list(item.get("fips") or []))
            changed = True
        if changed:
            self._save_state(state)
        return active

    def status_rows(self, fips_glob: str | None = None) -> list[dict[str, Any]]:
        state = self._load_state()
        active = self._active_fips(state)
        rows: list[dict[str, Any]] = []
        for county in self._filter_counties(self.counties(), fips_glob):
            rows.append(
                {
                    "fips": county.fips,
                    "status": county.status,
                    "reason": county.state_reason,
                    "active_slurm": county.fips in active,
                    "completed_iter": county.completed_iter,
                    "target_iter": county.target_iter,
                    "checkpoint_path": county.checkpoint_path,
                    "log_path": county.log_path or "",
                }
            )
        return rows

    def preview(self, fips: str) -> dict[str, Any]:
        target = str(fips or "").strip()
        try:
            county = self._county_by_fips(target)
            checkpoint_path = county.checkpoint_path
            log_path = county.log_path or ""
            status = county.status
            reason = county.state_reason
        except KeyError:
            county = None
            checkpoint_path = ""
            log_path = ""
            status = "unknown"
            reason = "preview_only"
        row = {
            "array_index": 0,
            "fips": target,
            "checkpoint_path": checkpoint_path,
            "log_path": log_path,
        }
        return {
            "fips": target,
            "status": status,
            "reason": reason,
            "checkpoint_path": checkpoint_path,
            "log_path": log_path,
            "worker_command": self._render_worker_command(row),
        }

    def doctor(self, fips: str | None = None, fips_glob: str | None = None) -> dict[str, Any]:
        counties = self._filter_counties(self.counties(), fips_glob)
        if fips:
            counties = [self._county_by_fips(fips)]
        rows: list[dict[str, Any]] = []
        for county in counties:
            checkpoint = county.checkpoint or {}
            expected_keys = {
                "fips_key": str(self.checkpoint_cfg.get("fips_key") or "fips"),
                "log_path_key": str(self.checkpoint_cfg.get("log_path_key") or "log_path"),
                "status_key": str(self.checkpoint_cfg.get("status_key") or "status"),
                "completed_iter_key": str(self.checkpoint_cfg.get("completed_iter_key") or "completed_iter"),
                "target_iter_key": str(self.checkpoint_cfg.get("target_iter_key") or "target_iter"),
            }
            missing_keys = [value for value in expected_keys.values() if value not in checkpoint]
            log_complete, log_continue = self._read_log_state(county.log_path)
            rows.append(
                {
                    "fips": county.fips,
                    "checkpoint_exists": self._path_exists(county.checkpoint_path),
                    "checkpoint_path": county.checkpoint_path,
                    "missing_checkpoint_keys": missing_keys,
                    "log_path": county.log_path or "",
                    "log_exists": bool(county.log_path and self._path_exists(county.log_path)),
                    "log_complete_marker": log_complete,
                    "log_continue_marker": log_continue,
                    "status": county.status,
                    "reason": county.state_reason,
                    "completed_iter": county.completed_iter,
                    "target_iter": county.target_iter,
                }
            )
        return {
            "config_path": self.config_path.as_posix(),
            "checkpoints_glob": str(self.controller_cfg.get("checkpoints_glob") or ""),
            "county_count": len(rows),
            "rows": rows,
        }

    def _eligible(self, fips_glob: str | None = None) -> list[CountyRecord]:
        state = self._load_state()
        active = self._active_fips(state)
        eligible: list[CountyRecord] = []
        for county in self._filter_counties(self.counties(), fips_glob):
            if county.status == "complete":
                continue
            if county.fips in active:
                continue
            eligible.append(county)
        max_submit = int(self.controller_cfg.get("max_submit") or 0)
        if max_submit > 0:
            eligible = eligible[:max_submit]
        return eligible

    def _wave_id(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _render_worker_command(self, row: dict[str, Any]) -> str:
        mode = str(self.worker_cfg.get("mode") or "template").strip().lower()
        if mode == "etl_pipeline":
            return self._render_etl_pipeline_command(row)
        mapping: dict[str, Any] = {}
        mapping.update(self.exec_env)
        mapping.update(self.worker_cfg)
        mapping.update(row)
        cmd = str(self.worker_cfg.get("command_template") or "").strip()
        if not cmd:
            raise ValueError("worker.command_template is required")
        return self._maybe_wrap_timeout(_format_map(cmd, mapping))

    def _bootstrap_submission_environment(self) -> dict[str, Any]:
        repo_root = str(self.worker_cfg.get("repo_root") or "").strip()
        python_bin = str(self.worker_cfg.get("python_bin") or "").strip()
        git_remote_url = str(self.exec_env.get("git_remote_url") or self.worker_cfg.get("git_remote_url") or "").strip()
        pipeline_repo_root = str(self.worker_cfg.get("pipeline_repo_root") or "").strip()
        pipeline_git_remote_url = str(self.worker_cfg.get("pipeline_git_remote_url") or "").strip()
        if not repo_root or not python_bin:
            return {"prepared": False, "reason": "missing_repo_root_or_python_bin"}
        normalized_python_bin = python_bin.replace("\\", "/")
        if "/" in normalized_python_bin:
            venv_dir = normalized_python_bin.rsplit("/", 2)[0]
        else:
            venv_dir = ""
        requirements_path = f"{repo_root.rstrip('/')}/requirements.txt"
        lines = [
            "set -euo pipefail",
            f"REPO_ROOT={shlex.quote(repo_root)}",
            f"VENV_DIR={shlex.quote(venv_dir)}",
            f"PYTHON_BIN={shlex.quote(python_bin)}",
            f"GIT_REMOTE_URL={shlex.quote(git_remote_url)}",
            "update_repo() {",
            "  local repo=\"$1\"",
            "  local remote=\"$2\"",
            "  if [ ! -d \"$repo/.git\" ]; then",
            "    if [ -z \"$remote\" ]; then",
            "      echo \"missing git_remote_url and repo checkout: $repo\" >&2",
            "      exit 1",
            "    fi",
            "    mkdir -p \"$(dirname \"$repo\")\"",
            "    git clone \"$remote\" \"$repo\"",
            "    return 0",
            "  fi",
            "  if [ -n \"$remote\" ]; then",
            "    (cd \"$repo\" && git fetch --all --prune && git pull --ff-only)",
            "  fi",
            "}",
            "update_repo \"$REPO_ROOT\" \"$GIT_REMOTE_URL\"",
            "if [ ! -x \"$PYTHON_BIN\" ]; then",
            "  if [ -z \"$VENV_DIR\" ]; then",
            "    echo \"could not derive venv dir from python_bin: $PYTHON_BIN\" >&2",
            "    exit 1",
            "  fi",
            "  mkdir -p \"$VENV_DIR\"",
            "  python3 -m venv \"$VENV_DIR\"",
            "  \"$PYTHON_BIN\" -m pip install --upgrade pip",
            f"  if [ -f {shlex.quote(requirements_path)} ]; then",
            f"    \"$PYTHON_BIN\" -m pip install -r {shlex.quote(requirements_path)}",
            "  fi",
            "fi",
        ]
        if pipeline_repo_root and pipeline_git_remote_url:
            lines.extend(
                [
                    f"PIPELINE_REPO_ROOT={shlex.quote(pipeline_repo_root)}",
                    f"PIPELINE_GIT_REMOTE_URL={shlex.quote(pipeline_git_remote_url)}",
                    "update_repo \"$PIPELINE_REPO_ROOT\" \"$PIPELINE_GIT_REMOTE_URL\"",
                ]
            )
        self.transport.run_text("\n".join(lines), check=True)
        return {
            "prepared": True,
            "repo_root": repo_root,
            "python_bin": python_bin,
            "pipeline_repo_root": pipeline_repo_root,
            "pipeline_present": bool(pipeline_repo_root and pipeline_git_remote_url),
        }

    def _maybe_wrap_timeout(self, command: str) -> str:
        timeout_seconds_raw = self.worker_cfg.get("timeout_seconds")
        if timeout_seconds_raw in (None, ""):
            return command
        timeout_seconds = int(timeout_seconds_raw)
        if timeout_seconds <= 0:
            return command
        kill_after_seconds = int(self.worker_cfg.get("timeout_kill_after_seconds") or 300)
        if kill_after_seconds < 0:
            kill_after_seconds = 0
        timeout_cmd = (
            f"timeout --signal=TERM --kill-after={kill_after_seconds}s {timeout_seconds}s "
        )
        return timeout_cmd + command

    def _render_etl_pipeline_command(self, row: dict[str, Any]) -> str:
        python_bin = str(self.worker_cfg.get("python_bin") or "python").strip()
        cli_path = str(self.worker_cfg.get("cli_path") or "cli.py").strip()
        pipeline_path = str(self.worker_cfg.get("pipeline_path") or "").strip()
        if not pipeline_path:
            raise ValueError("worker.pipeline_path is required for mode=etl_pipeline")
        argv: list[str] = [python_bin, cli_path, "run", pipeline_path]
        executor = str(self.worker_cfg.get("executor") or "").strip()
        if executor:
            argv += ["--executor", executor]
        env_cfg = str(self.worker_cfg.get("environments_config") or "").strip()
        if env_cfg:
            argv += ["--environments-config", env_cfg]
        env_name = str(self.worker_cfg.get("env") or "").strip()
        if env_name:
            argv += ["--env", env_name]
        project_id = str(self.worker_cfg.get("project_id") or "").strip()
        if project_id:
            argv += ["--project-id", project_id]
        pipeline_path = str(self.worker_cfg.get("pipeline_path") or "").strip()
        if pipeline_path:
            argv[3] = pipeline_path
        plugins_dir = str(self.worker_cfg.get("plugins_dir") or "").strip()
        if plugins_dir:
            argv += ["--plugins-dir", plugins_dir]
        workdir = str(self.worker_cfg.get("workdir") or "").strip()
        if workdir:
            argv += ["--workdir", _format_map(workdir, {**self.exec_env, **self.worker_cfg, **row})]
        for flag in list(self.worker_cfg.get("flags") or []):
            text = str(flag).strip()
            if text:
                argv.append(text)
        runtime_vars = dict(self.worker_cfg.get("vars") or {})
        runtime_vars.setdefault("county_fips", "{fips}")
        for key, raw_value in runtime_vars.items():
            value = _format_map(str(raw_value), {**self.exec_env, **self.worker_cfg, **row})
            argv += ["--var", f"{key}={value}"]
        return self._maybe_wrap_timeout(" ".join(shlex.quote(part) for part in argv))

    def _render_sbatch(self, *, wave_id: str, remote_manifest_path: str, remote_config_path: str, item_count: int) -> str:
        if item_count <= 0:
            raise ValueError("item_count must be > 0")
        job_name_prefix = str(self.slurm_cfg.get("job_name_prefix") or "etl-controler-wave").strip()
        time_limit = str(self.slurm_cfg.get("time") or self.exec_env.get("time") or "04:00:00").strip()
        cpus = int(self.slurm_cfg.get("cpus_per_task") or self.exec_env.get("cpus_per_task") or 1)
        mem = str(self.slurm_cfg.get("mem") or self.exec_env.get("mem") or "4G").strip()
        array_max_parallel = int(self.slurm_cfg.get("array_max_parallel") or self.exec_env.get("max_parallel") or 0)
        repo_root = str(self.worker_cfg.get("repo_root") or "").strip()
        python_bin = str(self.worker_cfg.get("python_bin") or "python").strip()
        bootstrap_lines = [str(x) for x in list(self.worker_cfg.get("bootstrap_lines") or []) if str(x).strip()]
        lines = ["#!/bin/bash --login"]
        if self.exec_env.get("partition"):
            lines.append(f"#SBATCH -p {self.exec_env['partition']}")
        if self.exec_env.get("account"):
            lines.append(f"#SBATCH -A {self.exec_env['account']}")
        lines.append(f"#SBATCH -t {time_limit}")
        lines.append(f"#SBATCH -c {cpus}")
        lines.append(f"#SBATCH --mem={mem}")
        lines.append(f"#SBATCH -J {job_name_prefix}")
        lines.append(f"#SBATCH -o {self.paths.remote_log_dir}/{job_name_prefix}-{wave_id}-%A_%a.out")
        if array_max_parallel > 0:
            lines.append(f"#SBATCH --array=0-{item_count - 1}%{array_max_parallel}")
        else:
            lines.append(f"#SBATCH --array=0-{item_count - 1}")
        for extra in list(self.exec_env.get("sbatch_extra") or []):
            lines.append(f"#SBATCH {extra}")
        lines.append("set -euo pipefail")
        lines.append(f"mkdir -p {shlex.quote(self.paths.remote_log_dir)}")
        if repo_root:
            lines.append(f"cd {shlex.quote(repo_root)}")
        for module in list(self.exec_env.get("modules") or []):
            lines.append(f"module load {shlex.quote(str(module))}")
        for raw in bootstrap_lines:
            lines.append(str(raw))
        lines.append(f"MANIFEST={shlex.quote(remote_manifest_path)}")
        lines.append(f"CONFIG={shlex.quote(remote_config_path)}")
        lines.append(
            f"{shlex.quote(python_bin)} -m controler.main run-item --config \"$CONFIG\" --manifest \"$MANIFEST\" --index \"$SLURM_ARRAY_TASK_ID\""
        )
        return "\n".join(lines) + "\n"

    def _write_manifest_local(self, rows: list[dict[str, Any]], wave_id: str) -> Path:
        self.paths.local_wave_dir.mkdir(parents=True, exist_ok=True)
        wave_dir = self.paths.local_wave_dir / wave_id
        wave_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = wave_dir / "manifest.csv"
        with manifest_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["array_index", "fips", "checkpoint_path", "log_path", "worker_command"],
            )
            writer.writeheader()
            writer.writerows(rows)
        return manifest_path

    def run_once(self, fips_glob: str | None = None) -> dict[str, Any]:
        state = self._load_state()
        eligible = self._eligible(fips_glob=fips_glob)
        if not eligible:
            return {"submitted": False, "eligible_count": 0, "job_id": "", "wave_id": "", "fips_glob": str(fips_glob or "")}
        wave_id = self._wave_id()
        rows: list[dict[str, Any]] = []
        for idx, county in enumerate(eligible):
            row = {
                "array_index": idx,
                "fips": county.fips,
                "checkpoint_path": county.checkpoint_path,
                "log_path": county.log_path or "",
            }
            row["worker_command"] = self._render_worker_command(row)
            rows.append(row)
        local_manifest = self._write_manifest_local(rows, wave_id)
        remote_wave_dir = f"{self.paths.remote_wave_dir.rstrip('/')}/{wave_id}"
        remote_manifest_path = f"{remote_wave_dir}/manifest.csv"
        remote_config_path = f"{remote_wave_dir}/controller_config.yml"
        self.transport.put_text(local_manifest.read_text(encoding="utf-8"), remote_manifest_path)
        self.transport.put_text(self.config_path.read_text(encoding="utf-8"), remote_config_path)
        sbatch_text = self._render_sbatch(
            wave_id=wave_id,
            remote_manifest_path=remote_manifest_path,
            remote_config_path=remote_config_path,
            item_count=len(rows),
        )
        spec = WorkloadSpec(
            name=f"controler-wave-{wave_id}",
            script_text=sbatch_text,
            backend_options={
                "destination_dir": remote_wave_dir if remote_wave_dir else self.paths.remote_submit_dir,
                "file_name": "wave.sbatch",
            },
        )
        handle = self.provisioner.submit(spec)
        state.setdefault("submissions", []).append(
            {
                "wave_id": wave_id,
                "job_id": str(handle.backend_run_id or ""),
                "fips": [row["fips"] for row in rows],
                "manifest_local_path": local_manifest.as_posix(),
                "manifest_remote_path": remote_manifest_path,
                "submitted_at": _utc_now(),
                "scheduler_state": "submitted",
            }
        )
        self._save_state(state)
        return {
            "submitted": True,
            "eligible_count": len(rows),
            "job_id": str(handle.backend_run_id or ""),
            "wave_id": wave_id,
            "fips_glob": str(fips_glob or ""),
            "manifest_local_path": local_manifest.as_posix(),
            "manifest_remote_path": remote_manifest_path,
        }

    def run_one(self, fips: str) -> dict[str, Any]:
        county = _single_county_record(fips)
        wave_id = self._wave_id()
        row = {
            "array_index": 0,
            "fips": county.fips,
            "checkpoint_path": "",
            "log_path": "",
        }
        row["worker_command"] = self._render_worker_command(row)
        local_manifest = self._write_manifest_local([row], wave_id)
        remote_wave_dir = f"{self.paths.remote_wave_dir.rstrip('/')}/{wave_id}"
        remote_manifest_path = f"{remote_wave_dir}/manifest.csv"
        remote_config_path = f"{remote_wave_dir}/controller_config.yml"
        self.transport.put_text(local_manifest.read_text(encoding="utf-8"), remote_manifest_path)
        self.transport.put_text(self.config_path.read_text(encoding="utf-8"), remote_config_path)
        sbatch_text = self._render_sbatch(
            wave_id=wave_id,
            remote_manifest_path=remote_manifest_path,
            remote_config_path=remote_config_path,
            item_count=1,
        )
        spec = WorkloadSpec(
            name=f"controler-wave-{wave_id}",
            script_text=sbatch_text,
            backend_options={
                "destination_dir": remote_wave_dir if remote_wave_dir else self.paths.remote_submit_dir,
                "file_name": "wave.sbatch",
            },
        )
        handle = self.provisioner.submit(spec)
        return {
            "submitted": True,
            "eligible_count": 1,
            "job_id": str(handle.backend_run_id or ""),
            "wave_id": wave_id,
            "fips": county.fips,
            "manifest_local_path": local_manifest.as_posix(),
            "manifest_remote_path": remote_manifest_path,
        }

    def bootstrap(self) -> dict[str, Any]:
        return self._bootstrap_submission_environment()

    def run_item(self, manifest_path: str | Path, index: int) -> dict[str, Any]:
        manifest = Path(manifest_path).expanduser().resolve()
        with manifest.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        idx = int(index)
        if idx < 0 or idx >= len(rows):
            raise IndexError(f"manifest index out of range: {idx}")
        row = dict(rows[idx])
        command = str(row.get("worker_command") or "").strip()
        if not command:
            raise ValueError(f"manifest row missing worker_command at index {idx}")
        import subprocess

        proc = subprocess.run(shlex.split(command), check=False)
        return {
            "index": idx,
            "fips": str(row.get("fips") or ""),
            "returncode": int(proc.returncode),
            "command": command,
        }

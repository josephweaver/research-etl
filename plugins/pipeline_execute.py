# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


meta = {
    "name": "pipeline_execute",
    "version": "0.1.0",
    "description": "Execute a child pipeline via CLI in synchronized or fire-and-forget mode.",
    "inputs": [],
    "outputs": [
        "mode",
        "submitted",
        "status",
        "child_run_id",
        "return_code",
        "pid",
        "command",
        "pipeline_path",
        "child_log_path",
    ],
    "params": {
        "pipeline_path": {"type": "str", "default": ""},
        "mode": {"type": "str", "default": "synchronized"},
        "executor": {"type": "str", "default": ""},
        "plugins_dir": {"type": "str", "default": "plugins"},
        "workdir": {"type": "str", "default": ""},
        "global_config": {"type": "str", "default": ""},
        "projects_config": {"type": "str", "default": ""},
        "environments_config": {"type": "str", "default": ""},
        "env": {"type": "str", "default": ""},
        "project_id": {"type": "str", "default": ""},
        "allow_dirty_git": {"type": "bool", "default": False},
        "dry_run": {"type": "bool", "default": False},
        "vars_json": {"type": "str", "default": ""},
        "vars_kv": {"type": "str", "default": ""},
    },
    "idempotent": False,
}


_RUN_LINE_RE = re.compile(r"Run\s+([A-Za-z0-9_-]+)\s*->\s*([A-Za-z_]+)")


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    return (ctx.workdir / p).resolve()


def _flatten_vars(value: Any, prefix: str = "") -> List[Tuple[str, Any]]:
    if not isinstance(value, dict):
        return []
    out: List[Tuple[str, Any]] = []
    for k, v in value.items():
        key = str(k).strip()
        if not key:
            continue
        dotted = f"{prefix}.{key}" if prefix else key
        if isinstance(v, dict):
            out.extend(_flatten_vars(v, dotted))
            continue
        out.append((dotted, v))
    return out


def _parse_vars_kv(raw: str) -> List[Tuple[str, str]]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: List[Tuple[str, str]] = []
    for token in re.split(r"[,\n;]+", text):
        item = str(token or "").strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"vars_kv token must be key=value, got: {item}")
        k, v = item.split("=", 1)
        key = str(k).strip()
        if not key:
            raise ValueError(f"vars_kv token has empty key: {item}")
        out.append((key, str(v)))
    return out


def _normalize_mode(raw: str) -> str:
    mode = str(raw or "synchronized").strip().lower()
    if mode in {"sync", "synchronized", "wait"}:
        return "synchronized"
    if mode in {"async", "fire_and_forget", "fire-and-forget", "submit_only"}:
        return "fire_and_forget"
    raise ValueError("mode must be one of: synchronized|fire_and_forget")


def _extract_child_run_id(text: str) -> str:
    for line in str(text or "").splitlines():
        m = _RUN_LINE_RE.search(line)
        if m:
            return str(m.group(1))
    return ""


def _build_cmd(args: Dict[str, Any], pipeline_path: Path) -> List[str]:
    cmd: List[str] = [sys.executable, "-m", "cli", "run", pipeline_path.as_posix()]

    def _add_opt(flag: str, value: Any) -> None:
        text = str(value or "").strip()
        if text:
            cmd.extend([flag, text])

    _add_opt("--executor", args.get("executor"))
    _add_opt("--plugins-dir", args.get("plugins_dir") or "plugins")
    _add_opt("--workdir", args.get("workdir"))
    _add_opt("--global-config", args.get("global_config"))
    _add_opt("--projects-config", args.get("projects_config"))
    _add_opt("--environments-config", args.get("environments_config"))
    _add_opt("--env", args.get("env"))
    _add_opt("--project-id", args.get("project_id"))
    if bool(args.get("allow_dirty_git", False)):
        cmd.append("--allow-dirty-git")
    if bool(args.get("dry_run", False)):
        cmd.append("--dry-run")

    vars_json = str(args.get("vars_json") or "").strip()
    if vars_json:
        try:
            payload = json.loads(vars_json)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"vars_json must be valid JSON object: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("vars_json must decode to a JSON object")
        for key, value in _flatten_vars(payload):
            cmd.extend(["--var", f"{key}={value}"])

    for key, value in _parse_vars_kv(str(args.get("vars_kv") or "")):
        cmd.extend(["--var", f"{key}={value}"])

    return cmd


def run(args, ctx):
    pipeline_text = str(args.get("pipeline_path") or "").strip()
    if not pipeline_text:
        raise ValueError("pipeline_path is required")
    mode = _normalize_mode(str(args.get("mode") or "synchronized"))
    pipeline_path = _resolve_path(pipeline_text, ctx)
    if not pipeline_path.exists():
        raise FileNotFoundError(f"pipeline_path not found: {pipeline_path}")

    cmd = _build_cmd(args, pipeline_path)
    cmd_text = " ".join(shlex.quote(part) for part in cmd)
    ctx.log(f"[pipeline_execute] mode={mode} pipeline={pipeline_path.as_posix()}")
    ctx.log(f"[pipeline_execute] cmd={cmd_text}")

    if mode == "synchronized":
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        child_run_id = _extract_child_run_id((proc.stdout or "") + "\n" + (proc.stderr or ""))
        status = "succeeded" if int(proc.returncode) == 0 else "failed"
        if proc.returncode != 0:
            detail = (proc.stderr or "").strip() or (proc.stdout or "").strip() or "unknown error"
            raise RuntimeError(f"child pipeline execution failed (exit {proc.returncode}): {detail}")
        return {
            "mode": mode,
            "submitted": True,
            "status": status,
            "child_run_id": child_run_id,
            "return_code": int(proc.returncode),
            "pid": 0,
            "command": cmd_text,
            "pipeline_path": pipeline_path.as_posix(),
            "child_log_path": "",
        }

    child_log = ctx.workdir / "child_pipeline_fire_and_forget.log"
    child_log.parent.mkdir(parents=True, exist_ok=True)
    log_f = child_log.open("a", encoding="utf-8")
    proc2 = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT, text=True)  # noqa: S603
    log_f.close()
    return {
        "mode": mode,
        "submitted": True,
        "status": "submitted",
        "child_run_id": "",
        "return_code": 0,
        "pid": int(proc2.pid or 0),
        "command": cmd_text,
        "pipeline_path": pipeline_path.as_posix(),
        "child_log_path": child_log.as_posix(),
    }

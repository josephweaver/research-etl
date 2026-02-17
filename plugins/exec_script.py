from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path


meta = {
    "name": "exec_script",
    "version": "0.1.0",
    "description": "Execute a local Python script with arguments and capture outputs.",
    "inputs": [],
    "outputs": ["script", "command", "cwd", "return_code", "stdout", "stderr"],
    "params": {
        "script": {"type": "str", "default": ""},
        "script_args": {"type": "str", "default": ""},
        "python_bin": {"type": "str", "default": ""},
        "cwd": {"type": "str", "default": ""},
        "timeout_seconds": {"type": "int", "default": 0},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_root_env = str(os.environ.get("ETL_REPO_ROOT") or "").strip()
    if repo_root_env:
        repo_rel = (Path(repo_root_env).expanduser().resolve() / p).resolve()
        if repo_rel.exists():
            return repo_rel
    cwd_rel = (Path(".").resolve() / p).resolve()
    if cwd_rel.exists():
        return cwd_rel
    return (ctx.workdir / p).resolve()


def run(args, ctx):
    script_text = str(args.get("script") or "").strip()
    if not script_text:
        raise ValueError("script is required")
    script_path = _resolve_path(script_text, ctx)
    if not script_path.exists() or not script_path.is_file():
        raise FileNotFoundError(f"script not found: {script_path}")

    python_bin = str(args.get("python_bin") or "").strip() or sys.executable
    script_args = str(args.get("script_args") or "").strip()
    timeout_seconds = int(args.get("timeout_seconds") or 0)
    verbose = bool(args.get("verbose", False))

    cwd_text = str(args.get("cwd") or "").strip()
    cwd = _resolve_path(cwd_text, ctx) if cwd_text else ctx.workdir
    cwd.mkdir(parents=True, exist_ok=True)

    cmd = [python_bin, script_path.resolve().as_posix()]
    if script_args:
        cmd.extend(shlex.split(script_args))

    ctx.log(f"[exec_script] running: {shlex.join(cmd)} cwd={cwd.as_posix()}")
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds if timeout_seconds > 0 else None,
        env=dict(os.environ),
    )
    if verbose and (proc.stdout or "").strip():
        ctx.log(f"[exec_script] stdout: {(proc.stdout or '').strip()[:4000]}")
    if verbose and (proc.stderr or "").strip():
        ctx.log(f"[exec_script] stderr: {(proc.stderr or '').strip()[:4000]}")
    if proc.returncode != 0:
        raise RuntimeError(
            f"script failed rc={proc.returncode}: {(proc.stderr or proc.stdout or '').strip()[:1000]}"
        )

    return {
        "script": script_path.resolve().as_posix(),
        "command": shlex.join(cmd),
        "cwd": cwd.resolve().as_posix(),
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or ""),
        "stderr": str(proc.stderr or ""),
    }

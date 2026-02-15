from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path


meta = {
    "name": "gdrive_upload",
    "version": "0.1.0",
    "description": "Upload files to Google Drive using rclone.",
    "inputs": [],
    "outputs": ["remote_spec", "uploaded_files", "uploaded_count", "output_path"],
    "params": {
        "rclone_bin": {"type": "str", "default": "rclone"},
        "remote": {"type": "str", "default": "gdrive"},
        "dst": {"type": "str", "default": "Data/ETL/Meta"},
        "input_path": {"type": "str", "default": ""},
        "email": {"type": "str", "default": ""},
        "cache_dir": {"type": "str", "default": ""},
        "shared_drive": {"type": "str", "default": ""},
        "shared_drive_id": {"type": "str", "default": ""},
        "dry_run": {"type": "bool", "default": False},
        "no_clobber": {"type": "bool", "default": False},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": False,
}


_REMOTE_SPEC_RE = re.compile(r"^[^\\/:\s]+:.+")


def _is_remote_spec(value: str) -> bool:
    return bool(_REMOTE_SPEC_RE.match(value.strip()))


def _build_remote_spec(
    *,
    remote: str,
    dst: str,
    shared_drive: str,
    shared_drive_id: str,
) -> str:
    dst_clean = dst.strip().replace("\\", "/").lstrip("/")
    if _is_remote_spec(dst_clean):
        return dst_clean
    if shared_drive.strip() and not shared_drive_id.strip():
        dst_clean = f"{shared_drive.strip().strip('/')}/{dst_clean}"
    remote_name = (remote or "gdrive").strip()
    if not remote_name:
        raise ValueError("remote must be non-empty")
    return f"{remote_name}:{dst_clean}"


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    return (ctx.workdir / p).resolve()


def run(args, ctx):
    rclone_arg = str(args.get("rclone_bin") or "").strip()
    rclone_env = str(os.environ.get("ETL_RCLONE_BIN") or "").strip()
    rclone_bin = rclone_arg or rclone_env or "rclone"

    input_path_text = str(args.get("input_path") or "").strip()
    if not input_path_text:
        raise ValueError("input_path is required")
    input_path = _resolve_path(input_path_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_path not found: {input_path}")

    remote_spec = _build_remote_spec(
        remote=str(args.get("remote") or "gdrive").strip(),
        dst=str(args.get("dst") or "Data/ETL/Meta").strip(),
        shared_drive=str(args.get("shared_drive") or "").strip(),
        shared_drive_id=str(args.get("shared_drive_id") or "").strip(),
    )
    verbose = bool(args.get("verbose", False))
    dry_run = bool(args.get("dry_run", False))
    no_clobber = bool(args.get("no_clobber", False))

    if input_path.is_file():
        dst_file = f"{remote_spec.rstrip('/')}/{input_path.name}"
        cmd = [rclone_bin, "copyto", str(input_path), dst_file]
        output_path = dst_file
    else:
        cmd = [rclone_bin, "copy", str(input_path), remote_spec]
        output_path = remote_spec

    if dry_run:
        cmd.append("--dry-run")
    if no_clobber:
        cmd.append("--ignore-existing")

    email = str(args.get("email") or "").strip()
    if email:
        cmd.extend(["--drive-impersonate", email])
    cache_dir = str(args.get("cache_dir") or "").strip()
    if cache_dir:
        cmd.extend(["--cache-dir", cache_dir])
    shared_drive_id = str(args.get("shared_drive_id") or "").strip()
    if shared_drive_id:
        cmd.extend(["--drive-team-drive", shared_drive_id])

    ctx.log(
        f"[gdrive_upload] start input={input_path.as_posix()} remote={remote_spec} dry_run={dry_run} no_clobber={no_clobber}"
    )
    ctx.log(f"[gdrive_upload] running: {shlex.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"gdrive upload failed: {detail}")
    if verbose and (proc.stdout or "").strip():
        ctx.log(f"[gdrive_upload] rclone stdout: {(proc.stdout or '').strip()[:2000]}")

    uploaded = [output_path]
    ctx.log(f"[gdrive_upload] done uploaded_count={len(uploaded)}")
    return {
        "remote_spec": remote_spec,
        "uploaded_files": uploaded,
        "uploaded_count": len(uploaded),
        "output_path": output_path,
    }

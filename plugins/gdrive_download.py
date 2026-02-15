from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Optional


meta = {
    "name": "gdrive_download",
    "version": "0.2.0",
    "description": "Download files from Google Drive using rclone (standalone, no R dependency)",
    "inputs": [],
    "outputs": ["output_dir", "downloaded_files", "downloaded_count", "remote_spec"],
    "params": {
        "rclone_bin": {"type": "str", "default": "rclone"},
        "remote": {"type": "str", "default": "gdrive"},
        "src": {"type": "str", "default": "Data"},
        "out": {"type": "str", "default": ".runs/cache/gdrive"},
        "glob": {"type": "str", "default": "*"},
        "recursive": {"type": "bool", "default": False},
        "email": {"type": "str", "default": ""},
        "cache_dir": {"type": "str", "default": ""},
        "shared_drive": {"type": "str", "default": ""},
        "shared_drive_id": {"type": "str", "default": ""},
        "dry_run": {"type": "bool", "default": False},
        "no_clobber": {"type": "bool", "default": False},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


_REMOTE_SPEC_RE = re.compile(r"^[^\\/:\s]+:.+")


def _is_remote_spec(value: str) -> bool:
    return bool(_REMOTE_SPEC_RE.match(value.strip()))


def _build_remote_spec(
    *,
    remote: str,
    src: str,
    shared_drive: str,
    shared_drive_id: str,
) -> str:
    src_clean = src.strip().replace("\\", "/").lstrip("/")
    if _is_remote_spec(src_clean):
        return src_clean

    if shared_drive.strip() and not shared_drive_id.strip():
        src_clean = f"{shared_drive.strip().strip('/')}/{src_clean}"

    remote_name = (remote or "gdrive").strip()
    if not remote_name:
        raise ValueError("remote must be non-empty")
    return f"{remote_name}:{src_clean}"


def run(args, ctx):
    rclone_arg = str(args.get("rclone_bin") or "").strip()
    rclone_env = str(os.environ.get("ETL_RCLONE_BIN") or "").strip()
    rclone_bin = rclone_arg or rclone_env or "rclone"
    remote = str(args.get("remote") or "gdrive").strip()
    src = str(args.get("src") or "Data").strip()
    out = Path(str(args.get("out") or ".runs/cache/gdrive")).expanduser()
    glob = str(args.get("glob") or "*").strip()

    shared_drive = str(args.get("shared_drive") or "").strip()
    shared_drive_id = str(args.get("shared_drive_id") or "").strip()
    verbose = bool(args.get("verbose", False))
    remote_spec = _build_remote_spec(
        remote=remote,
        src=src,
        shared_drive=shared_drive,
        shared_drive_id=shared_drive_id,
    )

    cmd = [rclone_bin, "copy", remote_spec, str(out)]
    if glob and glob != "*":
        cmd.extend(["--include", glob])
    if not bool(args.get("recursive", False)):
        cmd.extend(["--max-depth", "1"])
    if bool(args.get("dry_run", False)):
        cmd.append("--dry-run")
    if bool(args.get("no_clobber", False)):
        cmd.append("--ignore-existing")

    email = str(args.get("email") or "").strip()
    if email:
        cmd.extend(["--drive-impersonate", email])
    cache_dir = str(args.get("cache_dir") or "").strip()
    if cache_dir:
        cmd.extend(["--cache-dir", cache_dir])
    if shared_drive_id:
        cmd.extend(["--drive-team-drive", shared_drive_id])

    out.mkdir(parents=True, exist_ok=True)
    ctx.log(
        f"[gdrive_download] start remote_spec={remote_spec} out={out.as_posix()} "
        f"recursive={bool(args.get('recursive', False))} dry_run={bool(args.get('dry_run', False))}"
    )
    ctx.log(f"[gdrive_download] running: {shlex.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"gdrive download failed: {detail}")
    if verbose and (proc.stdout or "").strip():
        ctx.log(f"[gdrive_download] rclone stdout: {(proc.stdout or '').strip()[:2000]}")

    files = sorted(str(p.as_posix()) for p in out.rglob("*") if p.is_file())
    ctx.log(f"[gdrive_download] done downloaded_count={len(files)}")
    return {
        "output_dir": str(out.as_posix()),
        "downloaded_files": files,
        "downloaded_count": len(files),
        "remote_spec": remote_spec,
    }

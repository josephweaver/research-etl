from __future__ import annotations

import shlex
import subprocess
from pathlib import Path


meta = {
    "name": "gdrive_download",
    "version": "0.1.0",
    "description": "Download files from Google Drive by invoking tools/gdrv/download.R",
    "inputs": [],
    "outputs": ["output_dir", "downloaded_files", "downloaded_count"],
    "params": {
        "rscript_path": {"type": "str", "default": "tools/gdrv/download.R"},
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
    },
    "idempotent": True,
}


def run(args, ctx):
    rscript_path = str(args.get("rscript_path") or "tools/gdrv/download.R")
    src = str(args.get("src") or "Data")
    out = Path(str(args.get("out") or ".runs/cache/gdrive")).expanduser()
    glob = str(args.get("glob") or "*")

    cmd = ["Rscript", rscript_path, "--src", src, "--out", str(out), "--glob", glob]
    if bool(args.get("recursive", False)):
        cmd.append("--recursive")
    email = str(args.get("email") or "").strip()
    if email:
        cmd.extend(["--email", email])
    cache_dir = str(args.get("cache_dir") or "").strip()
    if cache_dir:
        cmd.extend(["--cache-dir", cache_dir])
    shared_drive = str(args.get("shared_drive") or "").strip()
    if shared_drive:
        cmd.extend(["--shared-drive", shared_drive])
    shared_drive_id = str(args.get("shared_drive_id") or "").strip()
    if shared_drive_id:
        cmd.extend(["--shared-drive-id", shared_drive_id])
    if bool(args.get("dry_run", False)):
        cmd.append("--dry-run")
    if bool(args.get("no_clobber", False)):
        cmd.append("--no-clobber")

    out.mkdir(parents=True, exist_ok=True)
    ctx.log(f"[gdrive_download] running: {shlex.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"gdrive download failed: {detail}")

    files = sorted(str(p.as_posix()) for p in out.rglob("*") if p.is_file())
    return {
        "output_dir": str(out.as_posix()),
        "downloaded_files": files,
        "downloaded_count": len(files),
    }

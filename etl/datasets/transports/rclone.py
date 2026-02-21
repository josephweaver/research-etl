# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict


def _to_rclone_target(target_uri: str) -> str:
    text = str(target_uri or "").strip()
    if text.lower().startswith("gdrive://"):
        return "gdrive:" + text[len("gdrive://") :].lstrip("/")
    if ":" in text and "://" not in text:
        return text
    raise ValueError(f"rclone target must be gdrive://... or remote:path, got: {target_uri}")


def transfer_rclone(
    *,
    source_path: str,
    target_uri: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    opts = dict(options or {})
    rclone_bin = str(opts.get("rclone_bin") or "rclone").strip() or "rclone"
    team_drive_id = str(opts.get("shared_drive_id") or "").strip()
    src = Path(source_path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"source_path not found: {src}")
    remote_target = _to_rclone_target(target_uri)

    if src.is_file():
        remote_file = f"{remote_target.rstrip('/')}/{src.name}"
        cmd = [rclone_bin, "copyto", str(src), remote_file]
        final_target = remote_file
    else:
        cmd = [rclone_bin, "copy", str(src), remote_target]
        final_target = remote_target

    if dry_run:
        cmd.append("--dry-run")
    if team_drive_id:
        cmd.extend(["--drive-team-drive", team_drive_id])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() or (proc.stdout or "").strip() or "unknown rclone error"
        raise RuntimeError(f"rclone transfer failed: {detail}")

    return {
        "transport": "rclone",
        "target_uri": final_target,
        "command": shlex.join(cmd),
        "dry_run": bool(dry_run),
    }


def fetch_rclone(
    *,
    source_uri: str,
    target_path: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    opts = dict(options or {})
    rclone_bin = str(opts.get("rclone_bin") or "rclone").strip() or "rclone"
    team_drive_id = str(opts.get("shared_drive_id") or "").strip()
    remote_source = _to_rclone_target(source_uri)
    out = Path(target_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [rclone_bin, "copy", remote_source, str(out.parent)]
    if dry_run:
        cmd.append("--dry-run")
    if team_drive_id:
        cmd.extend(["--drive-team-drive", team_drive_id])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() or (proc.stdout or "").strip() or "unknown rclone error"
        raise RuntimeError(f"rclone fetch failed: {detail}")

    return {
        "transport": "rclone",
        "target_path": str(out),
        "command": shlex.join(cmd),
        "dry_run": bool(dry_run),
    }

from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any

from .common.parsing import parse_bool


def shared_umask_from_config(config: dict[str, Any] | None) -> str:
    raw = str((config or {}).get("shared_umask") or "").strip()
    if not raw:
        return ""
    text = raw[2:] if raw.lower().startswith("0o") else raw
    if not text or any(ch not in "01234567" for ch in text):
        raise ValueError(f"Invalid shared_umask: {raw}")
    return text.zfill(4)[-4:]


def apply_process_umask(config: dict[str, Any] | None) -> None:
    text = shared_umask_from_config(config)
    if not text:
        return
    os.umask(int(text, 8))


def chmod_group_writable_enabled(config: dict[str, Any] | None) -> bool:
    return parse_bool((config or {}).get("chmod_group_writable"), default=bool(shared_umask_from_config(config)))


def setgid_dirs_enabled(config: dict[str, Any] | None) -> bool:
    return parse_bool((config or {}).get("setgid_dirs"), default=bool(shared_umask_from_config(config)))


def shared_group_from_config(config: dict[str, Any] | None) -> str:
    return str((config or {}).get("shared_group") or "").strip()


def shell_permissions_prelude(config: dict[str, Any] | None) -> list[str]:
    umask_text = shared_umask_from_config(config)
    group = shared_group_from_config(config)
    chmod_group = chmod_group_writable_enabled(config)
    setgid_dirs = setgid_dirs_enabled(config)
    if not any([umask_text, group, chmod_group, setgid_dirs]):
        return []

    lines: list[str] = []
    lines.append(f"ETL_SHARED_UMASK={shlex.quote(umask_text or '')}")
    lines.append(f"ETL_SHARED_GROUP={shlex.quote(group)}")
    lines.append(f"ETL_CHMOD_GROUP_WRITABLE={'1' if chmod_group else '0'}")
    lines.append(f"ETL_SETGID_DIRS={'1' if setgid_dirs else '0'}")
    lines.append('if [ -n "$ETL_SHARED_UMASK" ]; then umask "$ETL_SHARED_UMASK"; fi')
    lines.append("etl_fix_permissions(){")
    lines.append("  target=\"$1\"")
    lines.append("  [ -n \"$target\" ] || return 0")
    lines.append("  [ -e \"$target\" ] || return 0")
    lines.append("  if [ -n \"$ETL_SHARED_GROUP\" ]; then chgrp -R \"$ETL_SHARED_GROUP\" \"$target\" 2>/dev/null || true; fi")
    lines.append("  if [ \"$ETL_CHMOD_GROUP_WRITABLE\" = \"1\" ]; then chmod -R g+rwX \"$target\" 2>/dev/null || true; fi")
    lines.append("  if [ \"$ETL_SETGID_DIRS\" = \"1\" ] && [ -d \"$target\" ]; then find \"$target\" -type d -exec chmod g+s {} + 2>/dev/null || true; fi")
    lines.append("}")
    return lines


def fix_path_permissions(path: str | Path, config: dict[str, Any] | None) -> None:
    target = Path(path).expanduser()
    if not target.exists():
        return
    group = shared_group_from_config(config)
    if group:
        try:
            import grp

            gid = grp.getgrnam(group).gr_gid
            for cur in [target, *(target.rglob("*") if target.is_dir() else [])]:
                try:
                    os.chown(cur, -1, gid)
                except OSError:
                    pass
        except Exception:
            pass
    if chmod_group_writable_enabled(config):
        for cur in [target, *(target.rglob("*") if target.is_dir() else [])]:
            try:
                mode = cur.stat().st_mode
                cur.chmod(mode | 0o060)
            except OSError:
                pass
    if setgid_dirs_enabled(config) and target.is_dir():
        for cur in [target, *target.rglob("*")]:
            if cur.is_dir():
                try:
                    cur.chmod(cur.stat().st_mode | 0o2000)
                except OSError:
                    pass

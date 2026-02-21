# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import shutil
from pathlib import Path
from urllib.parse import unquote, urlparse


def _resolve_filesystem_target(target_uri: str) -> Path:
    text = str(target_uri or "").strip()
    if "://" not in text:
        return Path(text).expanduser().resolve()
    parsed = urlparse(text)
    if parsed.scheme != "file":
        raise ValueError(f"local_fs requires filesystem target path or file:// URI, got: {target_uri}")
    path_text = unquote(parsed.path or "")
    if len(path_text) >= 3 and path_text[0] == "/" and path_text[2] == ":":
        path_text = path_text[1:]
    if not path_text:
        raise ValueError(f"Invalid file:// URI target: {target_uri}")
    return Path(path_text).expanduser().resolve()


def transfer_local_fs(*, source_path: str, target_uri: str, dry_run: bool = False) -> dict:
    src = Path(source_path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"source_path not found: {src}")
    dst = _resolve_filesystem_target(target_uri)
    if dry_run:
        return {
            "transport": "local_fs",
            "target_uri": str(dst),
            "copied": False,
            "dry_run": True,
        }
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if dst.exists() and dst.is_file():
            raise ValueError(f"Cannot copy directory '{src}' to existing file '{dst}'")
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)
    return {
        "transport": "local_fs",
        "target_uri": str(dst),
        "copied": True,
        "dry_run": False,
    }


def fetch_local_fs(*, source_uri: str, target_path: str, dry_run: bool = False) -> dict:
    src = _resolve_filesystem_target(source_uri)
    if not src.exists():
        raise FileNotFoundError(f"source_uri not found: {src}")
    dst = Path(target_path).expanduser().resolve()
    if dry_run:
        return {
            "transport": "local_fs",
            "target_path": str(dst),
            "copied": False,
            "dry_run": True,
        }
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if dst.exists() and dst.is_file():
            raise ValueError(f"Cannot copy directory '{src}' to existing file '{dst}'")
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)
    return {
        "transport": "local_fs",
        "target_path": str(dst),
        "copied": True,
        "dry_run": False,
    }

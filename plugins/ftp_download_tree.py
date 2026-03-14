# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import fnmatch
import ftplib
import json
import os
import posixpath
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib import parse


meta = {
    "name": "ftp_download_tree",
    "version": "0.1.0",
    "description": "Download files from an FTP URL, with optional recursive traversal and filename glob filters.",
    "inputs": [],
    "outputs": [
        "output_dir",
        "manifest_file",
        "downloaded_files",
        "downloaded_count",
        "skipped_count",
        "failed_count",
        "failed_files",
        "source_url",
    ],
    "params": {
        "url": {"type": "str", "default": ""},
        "out": {"type": "str", "default": ".runs/cache/ftp_downloads"},
        "recursive": {"type": "bool", "default": True},
        "filename_glob": {"type": "str", "default": "*"},
        "glob": {"type": "str", "default": ""},
        "include_glob": {"type": "str", "default": ""},
        "overwrite": {"type": "bool", "default": False},
        "timeout_seconds": {"type": "int", "default": 120},
        "username": {"type": "str", "default": ""},
        "password": {"type": "str", "default": ""},
        "passive": {"type": "bool", "default": True},
        "fail_on_error": {"type": "bool", "default": True},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_root = str(os.environ.get("ETL_REPO_ROOT", "") or "").strip()
    if repo_root:
        return (Path(repo_root).expanduser() / p).resolve()
    return (ctx.workdir / p).resolve()


def _parse_globs(primary: str, alias_glob: str, alias_include: str) -> List[str]:
    for raw in [str(primary or "").strip(), str(alias_glob or "").strip(), str(alias_include or "").strip()]:
        if not raw:
            continue
        parts = [v.strip() for v in raw.replace(",", "\n").splitlines() if v.strip()]
        if parts:
            return parts
    return ["*"]


def _match_globs(rel_path: str, filename: str, patterns: List[str]) -> bool:
    for pat in patterns:
        if fnmatch.fnmatch(rel_path, pat) or fnmatch.fnmatch(filename, pat):
            return True
    return False


def _normalize_remote(path_text: str) -> str:
    text = str(path_text or "").strip()
    if not text:
        return "/"
    parts = [p for p in text.replace("\\", "/").split("/") if p]
    return "/" + "/".join(parts)


def _is_dir(ftp: ftplib.FTP, remote_path: str) -> bool:
    prior = ftp.pwd()
    try:
        ftp.cwd(remote_path)
        return True
    except ftplib.all_errors:
        return False
    finally:
        try:
            ftp.cwd(prior)
        except ftplib.all_errors:
            pass


def _list_children(ftp: ftplib.FTP, remote_dir: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    try:
        for name, facts in ftp.mlsd(remote_dir):
            if name in {".", ".."}:
                continue
            kind = str((facts or {}).get("type") or "").strip().lower()
            if kind in {"file", "dir"}:
                out.append((name, kind))
            else:
                full = posixpath.join(remote_dir, name)
                out.append((name, "dir" if _is_dir(ftp, full) else "file"))
        return out
    except (AttributeError, ftplib.error_perm):
        pass

    # Fallback when MLSD is unavailable.
    names = ftp.nlst(remote_dir)
    for raw in names:
        full = _normalize_remote(raw)
        if full in {remote_dir, remote_dir.rstrip("/") or "/"}:
            continue
        name = posixpath.basename(full.rstrip("/"))
        if not name or name in {".", ".."}:
            continue
        out.append((name, "dir" if _is_dir(ftp, full) else "file"))
    return out


def _walk_files(ftp: ftplib.FTP, start: str, recursive: bool) -> List[str]:
    if not _is_dir(ftp, start):
        return [_normalize_remote(start)]
    files: List[str] = []
    stack = [start]
    seen_dirs = set()
    while stack:
        current = stack.pop()
        if current in seen_dirs:
            continue
        seen_dirs.add(current)
        for name, kind in _list_children(ftp, current):
            full = _normalize_remote(posixpath.join(current, name))
            if kind == "dir":
                if recursive:
                    stack.append(full)
                continue
            files.append(full)
    return sorted(set(files))


def _to_relative(remote_file: str, root_dir: str) -> str:
    r = _normalize_remote(remote_file)
    root = _normalize_remote(root_dir)
    if r == root:
        return posixpath.basename(r)
    prefix = root.rstrip("/") + "/"
    if r.startswith(prefix):
        return r[len(prefix) :]
    return posixpath.basename(r)


def _download_file(ftp: ftplib.FTP, remote_file: str, local_file: Path) -> int:
    total = 0

    def _write(chunk: bytes) -> None:
        nonlocal total
        local_file_handle.write(chunk)
        total += len(chunk)

    local_file.parent.mkdir(parents=True, exist_ok=True)
    with local_file.open("wb") as local_file_handle:
        ftp.retrbinary(f"RETR {remote_file}", _write, blocksize=1024 * 1024)
    return total


def run(args, ctx):
    raw_url = str(args.get("url") or "").strip()
    if not raw_url:
        raise ValueError("url is required")
    parsed = parse.urlparse(raw_url)
    if parsed.scheme.lower() != "ftp":
        raise ValueError(f"Only ftp URLs are supported: {raw_url}")
    if not parsed.hostname:
        raise ValueError(f"FTP URL must include a host: {raw_url}")

    out_dir = _resolve_path(str(args.get("out") or ".runs/cache/ftp_downloads"), ctx)
    out_dir.mkdir(parents=True, exist_ok=True)
    recursive = bool(args.get("recursive", True))
    overwrite = bool(args.get("overwrite", False))
    fail_on_error = bool(args.get("fail_on_error", True))
    timeout_seconds = max(1, int(args.get("timeout_seconds", 120)))
    passive = bool(args.get("passive", True))
    verbose = bool(args.get("verbose", False))
    globs = _parse_globs(args.get("filename_glob"), args.get("glob"), args.get("include_glob"))

    username = str(args.get("username") or parsed.username or "").strip() or "anonymous"
    password = str(args.get("password") or parsed.password or "").strip() or "anonymous@"
    host = str(parsed.hostname)
    port = int(parsed.port or 21)
    start_path = _normalize_remote(parse.unquote(parsed.path or "/"))

    ctx.log(
        f"[ftp_download_tree] start host={host}:{port} path={start_path} out={out_dir.as_posix()} "
        f"recursive={recursive} overwrite={overwrite} globs={globs}"
    )

    ftp = ftplib.FTP()
    ftp.connect(host=host, port=port, timeout=timeout_seconds)
    ftp.login(user=username, passwd=password)
    ftp.set_pasv(passive)

    try:
        remote_files = _walk_files(ftp, start_path, recursive=recursive)
        targets: List[Tuple[str, str, Path]] = []
        for remote_file in remote_files:
            rel = _to_relative(remote_file, start_path).replace("\\", "/").lstrip("/")
            if not rel:
                rel = posixpath.basename(remote_file)
            if not _match_globs(rel_path=rel, filename=posixpath.basename(rel), patterns=globs):
                continue
            targets.append((remote_file, rel, out_dir / rel))
        if not targets:
            ctx.log("[ftp_download_tree] no files matched filter")

        downloaded_files: List[str] = []
        failed_files: List[str] = []
        skipped_count = 0
        manifest: List[Dict[str, object]] = []
        for idx, (remote_file, rel, local_file) in enumerate(targets, start=1):
            if local_file.exists() and not overwrite:
                skipped_count += 1
                manifest.append({"remote_file": remote_file, "path": local_file.resolve().as_posix(), "status": "skipped_existing"})
                if verbose:
                    ctx.log(f"[ftp_download_tree] {idx}/{len(targets)} skipped existing: {rel}")
                continue
            try:
                started = time.time()
                size = _download_file(ftp, remote_file, local_file)
                elapsed = round(time.time() - started, 2)
                downloaded_files.append(local_file.resolve().as_posix())
                manifest.append(
                    {
                        "remote_file": remote_file,
                        "path": local_file.resolve().as_posix(),
                        "status": "downloaded",
                        "size_bytes": size,
                    }
                )
                if verbose:
                    ctx.log(f"[ftp_download_tree] {idx}/{len(targets)} downloaded: {rel} ({size} bytes in {elapsed}s)")
            except Exception as exc:  # noqa: BLE001
                failed_files.append(remote_file)
                manifest.append(
                    {
                        "remote_file": remote_file,
                        "path": local_file.resolve().as_posix(),
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                ctx.log(f"[ftp_download_tree] failed: {remote_file} ({exc})", "ERROR")
                if fail_on_error:
                    raise RuntimeError(f"download failed for {remote_file}: {exc}") from exc

        manifest_file = out_dir / "download_manifest.json"
        manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        ctx.log(
            f"[ftp_download_tree] files={len(targets)} downloaded={len(downloaded_files)} "
            f"skipped={skipped_count} failed={len(failed_files)}"
        )
        return {
            "output_dir": out_dir.resolve().as_posix(),
            "manifest_file": manifest_file.resolve().as_posix(),
            "downloaded_files": downloaded_files,
            "downloaded_count": len(downloaded_files),
            "skipped_count": skipped_count,
            "failed_count": len(failed_files),
            "failed_files": failed_files,
            "source_url": raw_url,
        }
    finally:
        try:
            ftp.quit()
        except ftplib.all_errors:
            pass

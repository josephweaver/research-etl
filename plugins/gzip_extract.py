# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import glob
import gzip
from pathlib import Path
from typing import List


meta = {
    "name": "gzip_extract",
    "version": "0.1.0",
    "description": "Extract one or more .gz files and return extracted file paths.",
    "inputs": [],
    "outputs": ["output_dir", "archives", "extracted_files", "extracted_count", "format"],
    "params": {
        "archive": {"type": "str", "default": ""},
        "archive_glob": {"type": "str", "default": ""},
        "out": {"type": "str", "default": ".runs/cache/gzip_extract"},
        "overwrite": {"type": "bool", "default": True},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _expand_archive_glob(pattern: str, ctx) -> List[Path]:
    text = str(pattern or "").strip()
    if not text:
        return []
    expanded = Path(text).expanduser()
    matches: List[Path] = []
    if not glob.has_magic(text):
        candidate = _resolve_path(text, ctx)
        return [candidate] if candidate.exists() and candidate.is_file() else []

    patterns: List[str] = []
    if expanded.is_absolute():
        patterns.append(str(expanded))
    else:
        patterns.append(str(Path(text)))
        patterns.append(str(ctx.workdir / text))

    seen: set[str] = set()
    for pat in patterns:
        for item in glob.glob(pat, recursive=True):
            p = Path(item)
            if not p.exists() or not p.is_file():
                continue
            key = p.resolve().as_posix()
            if key in seen:
                continue
            seen.add(key)
            matches.append(p)
    return sorted(matches)


def _target_path(archive: Path, out_dir: Path) -> Path:
    if archive.suffix.lower() != ".gz":
        raise ValueError(f"Unsupported gzip archive: {archive}")
    name = archive.name[:-3] if archive.name.lower().endswith(".gz") else archive.stem
    if not name:
        raise ValueError(f"Could not determine output filename for archive: {archive}")
    return out_dir / name


def _extract_one(archive: Path, out_dir: Path, *, overwrite: bool) -> Path:
    target = _target_path(archive, out_dir)
    if target.exists() and not overwrite:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive, mode="rb") as src, target.open("wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)
    return target


def run(args, ctx):
    archive_text = str(args.get("archive") or "").strip()
    archive_glob = str(args.get("archive_glob") or "").strip()
    if not archive_text and not archive_glob:
        raise ValueError("archive or archive_glob is required")

    out_dir = _resolve_path(str(args.get("out") or ".runs/cache/gzip_extract"), ctx)
    out_dir.mkdir(parents=True, exist_ok=True)

    archive_paths: List[Path] = []
    if archive_text:
        archive_paths.append(_resolve_path(archive_text, ctx))
    if archive_glob:
        archive_paths.extend(_expand_archive_glob(archive_glob, ctx))
    archive_paths = [p for p in archive_paths if p.exists()]
    if not archive_paths:
        raise FileNotFoundError("No archive files found for provided archive/archive_glob")

    overwrite = bool(args.get("overwrite", True))
    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[gzip_extract] start archives={len(archive_paths)} out={out_dir.resolve().as_posix()} "
        f"overwrite={overwrite}"
    )

    extracted_files: List[str] = []
    archive_list: List[str] = []
    for archive in archive_paths:
        archive_list.append(archive.resolve().as_posix())
        if verbose:
            ctx.log(f"[gzip_extract] processing archive={archive.resolve().as_posix()}")
        target = _extract_one(archive, out_dir, overwrite=overwrite)
        extracted_files.append(target.resolve().as_posix())

    extracted_files = sorted(set(extracted_files))
    ctx.log(f"[gzip_extract] done extracted_count={len(extracted_files)}")
    if verbose and extracted_files:
        ctx.log(f"[gzip_extract] extracted_preview={extracted_files[:5]}")
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "archives": archive_list,
        "extracted_files": extracted_files,
        "extracted_count": len(extracted_files),
        "format": "gz",
    }

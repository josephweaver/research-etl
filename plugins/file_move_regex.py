from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import List


meta = {
    "name": "file_move_regex",
    "version": "0.1.0",
    "description": "Move files under a source tree when relative paths match a regex; preserve subdirectories.",
    "inputs": [],
    "outputs": [
        "source_dir",
        "dest_dir",
        "pattern",
        "matched_count",
        "moved_count",
        "skipped_existing_count",
        "matched_files",
        "moved_files",
        "skipped_existing",
    ],
    "params": {
        "src": {"type": "str", "default": ""},
        "dst": {"type": "str", "default": ""},
        "pattern": {"type": "str", "default": ""},
        "flags": {"type": "str", "default": "i"},
        "match_on": {"type": "str", "default": "relative_path"},
        "overwrite": {"type": "bool", "default": False},
        "dry_run": {"type": "bool", "default": False},
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


def _regex_flags(text: str) -> int:
    out = 0
    for ch in str(text or "").lower():
        if ch == "i":
            out |= re.IGNORECASE
        elif ch == "m":
            out |= re.MULTILINE
        elif ch == "s":
            out |= re.DOTALL
    return out


def _match_text(path: Path, rel: Path, *, mode: str) -> str:
    mode_norm = str(mode or "relative_path").strip().lower()
    if mode_norm == "filename":
        return path.name
    if mode_norm == "absolute_path":
        return path.resolve().as_posix()
    return rel.as_posix()


def _remove_empty_parents(start: Path, stop: Path) -> None:
    cur = start
    stop_resolved = stop.resolve()
    while True:
        try:
            cur_resolved = cur.resolve()
        except Exception:
            break
        if cur_resolved == stop_resolved:
            break
        try:
            next(cur.iterdir())
            break
        except StopIteration:
            cur.rmdir()
            cur = cur.parent
            continue
        except Exception:
            break


def run(args, ctx):
    src_text = str(args.get("src") or "").strip()
    dst_text = str(args.get("dst") or "").strip()
    pattern = str(args.get("pattern") or "").strip()
    if not src_text:
        raise ValueError("src is required")
    if not dst_text:
        raise ValueError("dst is required")
    if not pattern:
        raise ValueError("pattern is required")

    src_root = _resolve_path(src_text, ctx)
    dst_root = _resolve_path(dst_text, ctx)
    if not src_root.exists() or not src_root.is_dir():
        raise FileNotFoundError(f"Source directory not found: {src_root}")
    dst_root.mkdir(parents=True, exist_ok=True)

    flags = _regex_flags(str(args.get("flags") or "i"))
    regex = re.compile(pattern, flags=flags)
    overwrite = bool(args.get("overwrite", False))
    dry_run = bool(args.get("dry_run", False))
    match_on = str(args.get("match_on") or "relative_path")

    matched_files: List[str] = []
    moved_files: List[str] = []
    skipped_existing: List[str] = []

    files = sorted(p for p in src_root.rglob("*") if p.is_file())
    for src_file in files:
        rel = src_file.relative_to(src_root)
        text = _match_text(src_file, rel, mode=match_on)
        if not regex.search(text):
            continue
        matched_files.append(rel.as_posix())
        target = dst_root / rel
        if target.exists() and not overwrite:
            skipped_existing.append(rel.as_posix())
            continue
        if not dry_run:
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and overwrite:
                target.unlink()
            shutil.move(str(src_file), str(target))
            _remove_empty_parents(src_file.parent, src_root)
        moved_files.append(rel.as_posix())

    ctx.log(
        f"[file_move_regex] matched={len(matched_files)} moved={len(moved_files)} "
        f"skipped_existing={len(skipped_existing)} pattern={pattern!r}"
    )
    return {
        "source_dir": src_root.resolve().as_posix(),
        "dest_dir": dst_root.resolve().as_posix(),
        "pattern": pattern,
        "matched_count": len(matched_files),
        "moved_count": len(moved_files),
        "skipped_existing_count": len(skipped_existing),
        "matched_files": matched_files,
        "moved_files": moved_files,
        "skipped_existing": skipped_existing,
    }


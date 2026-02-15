from __future__ import annotations

import re
from pathlib import Path
from typing import List


meta = {
    "name": "file_delete_regex",
    "version": "0.1.0",
    "description": "Delete files under a source tree when relative paths match a regex.",
    "inputs": [],
    "outputs": [
        "source_dir",
        "pattern",
        "matched_count",
        "deleted_count",
        "matched_files",
        "deleted_files",
    ],
    "params": {
        "src": {"type": "str", "default": ""},
        "pattern": {"type": "str", "default": ""},
        "flags": {"type": "str", "default": "i"},
        "match_on": {"type": "str", "default": "relative_path"},
        "dry_run": {"type": "bool", "default": False},
        "remove_empty_dirs": {"type": "bool", "default": True},
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


def _remove_empty_dirs(root: Path) -> None:
    # Deepest first so child empties are removed before parents.
    dirs = sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True)
    for d in dirs:
        try:
            next(d.iterdir())
        except StopIteration:
            d.rmdir()
        except Exception:
            continue


def run(args, ctx):
    src_text = str(args.get("src") or "").strip()
    pattern = str(args.get("pattern") or "").strip()
    if not src_text:
        raise ValueError("src is required")
    if not pattern:
        raise ValueError("pattern is required")

    src_root = _resolve_path(src_text, ctx)
    if not src_root.exists() or not src_root.is_dir():
        raise FileNotFoundError(f"Source directory not found: {src_root}")

    flags = _regex_flags(str(args.get("flags") or "i"))
    regex = re.compile(pattern, flags=flags)
    dry_run = bool(args.get("dry_run", False))
    remove_empty_dirs = bool(args.get("remove_empty_dirs", True))
    verbose = bool(args.get("verbose", False))
    match_on = str(args.get("match_on") or "relative_path")
    ctx.log(
        f"[file_delete_regex] start src={src_root.resolve().as_posix()} pattern={pattern!r} "
        f"dry_run={dry_run} remove_empty_dirs={remove_empty_dirs}"
    )

    matched_files: List[str] = []
    deleted_files: List[str] = []
    files = sorted(p for p in src_root.rglob("*") if p.is_file())
    for src_file in files:
        rel = src_file.relative_to(src_root)
        text = _match_text(src_file, rel, mode=match_on)
        if not regex.search(text):
            continue
        rel_text = rel.as_posix()
        matched_files.append(rel_text)
        if not dry_run:
            src_file.unlink()
        deleted_files.append(rel_text)

    if not dry_run and remove_empty_dirs:
        _remove_empty_dirs(src_root)

    ctx.log(
        f"[file_delete_regex] matched={len(matched_files)} deleted={len(deleted_files)} pattern={pattern!r}"
    )
    if verbose and deleted_files:
        ctx.log(f"[file_delete_regex] deleted_preview={deleted_files[:10]}")
    return {
        "source_dir": src_root.resolve().as_posix(),
        "pattern": pattern,
        "matched_count": len(matched_files),
        "deleted_count": len(deleted_files),
        "matched_files": matched_files,
        "deleted_files": deleted_files,
    }

from __future__ import annotations

import os
import shlex
import subprocess
import zipfile
import glob
import fnmatch
import re
from pathlib import Path
from typing import List

try:
    import py7zr  # type: ignore
except Exception:  # noqa: BLE001
    py7zr = None


meta = {
    "name": "archive_extract",
    "version": "0.1.0",
    "description": "Extract .zip and .7z archives and return extracted file paths",
    "inputs": [],
    "outputs": ["output_dir", "archives", "extracted_files", "extracted_count", "format"],
    "params": {
        "archive": {"type": "str", "default": ""},
        "archive_glob": {"type": "str", "default": ""},
        "out": {"type": "str", "default": ".runs/cache/extracted"},
        "format": {"type": "str", "default": "auto"},
        "include_glob": {"type": "str", "default": ""},
        "overwrite": {"type": "bool", "default": True},
        "seven_zip_bin": {"type": "str", "default": "7z"},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    # Prefer repository-relative resolution so configured paths like ".out/..."
    # do not get unintentionally rooted under step work dirs.
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    # If the path explicitly looks repo-relative (dot-prefix or path segments),
    # keep it anchored to repository root even when it doesn't exist yet.
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _all_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*") if p.is_file())


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
        patterns.append(str((ctx.workdir / text)))

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


def _norm_members(members: List[str]) -> List[str]:
    clean: List[str] = []
    for item in members:
        rel = str(item or "").replace("\\", "/").strip().lstrip("/")
        if not rel or rel.endswith("/"):
            continue
        clean.append(rel)
    return sorted(set(clean))


def _parse_include_globs(value) -> List[str]:
    raw = value
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = [str(x or "").strip() for x in raw]
        return [x for x in items if x]
    text = str(raw).strip()
    if not text:
        return []
    # Accept comma/semicolon/newline separated patterns.
    parts = [p.strip() for p in re.split(r"[,\n;]+", text)]
    return [p for p in parts if p]


def _match_member(rel: str, include_globs: List[str]) -> bool:
    if not include_globs:
        return True
    norm = str(rel or "").replace("\\", "/")
    for pat in include_globs:
        if fnmatch.fnmatch(norm, pat):
            return True
    return False


def _extract_zip(archive: Path, out_dir: Path, *, overwrite: bool, include_globs: List[str]) -> List[str]:
    extracted: List[str] = []
    with zipfile.ZipFile(archive, mode="r") as zf:
        for info in zf.infolist():
            rel = str(info.filename).replace("\\", "/").strip().lstrip("/")
            if not rel or rel.endswith("/"):
                continue
            if not _match_member(rel, include_globs):
                continue
            target = out_dir / rel
            if target.exists() and not overwrite:
                extracted.append(rel)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, mode="r") as src, target.open("wb") as dst:
                dst.write(src.read())
            extracted.append(rel)
    return sorted(set(extracted))


def _extract_7z(
    archive: Path,
    out_dir: Path,
    *,
    overwrite: bool,
    seven_zip_bin: str,
    include_globs: List[str],
    ctx,
) -> List[str]:
    if py7zr is not None:
        with py7zr.SevenZipFile(archive, mode="r") as zf:
            members = [m for m in _norm_members(list(zf.getnames())) if _match_member(m, include_globs)]
            if not members:
                return []
            if overwrite:
                zf.extract(path=out_dir, targets=members)
            else:
                for rel in members:
                    target = out_dir / rel
                    if target.exists():
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                zf.extract(path=out_dir, targets=[m for m in members if not (out_dir / m).exists()])
            return members

    seven_zip_env = str(os.environ.get("ETL_7Z_BIN") or "").strip()
    seven_zip = str(seven_zip_bin or "").strip() or seven_zip_env or "7z"
    before = {p.resolve().as_posix() for p in _all_files(out_dir)}
    cmd = [seven_zip, "x", str(archive), f"-o{out_dir}", "-y"]
    for pat in include_globs:
        cmd.append(f"-ir!{pat}")
    if not overwrite:
        cmd.append("-aos")
    ctx.log(f"[archive_extract] running: {shlex.join(cmd)}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"7z binary not found: {seven_zip}. Install 7-Zip, set seven_zip_bin/ETL_7Z_BIN, or install py7zr."
        ) from exc
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        if stdout:
            ctx.log(f"[archive_extract] 7z stdout: {stdout[:2000]}", "WARN")
        if stderr:
            ctx.log(f"[archive_extract] 7z stderr: {stderr[:2000]}", "WARN")
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"7z extraction failed (exit {proc.returncode}): {detail}")
    after = [p for p in _all_files(out_dir) if p.resolve().as_posix() not in before]
    return sorted(str(p.relative_to(out_dir)).replace("\\", "/") for p in after)


def run(args, ctx):
    archive_text = str(args.get("archive") or "").strip()
    archive_glob = str(args.get("archive_glob") or "").strip()
    if not archive_text and not archive_glob:
        raise ValueError("archive or archive_glob is required")

    out_dir = _resolve_path(str(args.get("out") or ".runs/cache/extracted"), ctx)
    out_dir.mkdir(parents=True, exist_ok=True)

    archive_paths: List[Path] = []
    if archive_text:
        archive_paths.append(_resolve_path(archive_text, ctx))
    if archive_glob:
        archive_paths.extend(_expand_archive_glob(archive_glob, ctx))
    archive_paths = [p for p in archive_paths if p.exists()]
    if not archive_paths:
        raise FileNotFoundError("No archive files found for provided archive/archive_glob")

    fmt = str(args.get("format") or "auto").strip().lower()
    include_globs = _parse_include_globs(args.get("include_glob"))
    overwrite = bool(args.get("overwrite", True))
    seven_zip_bin = str(args.get("seven_zip_bin") or "7z").strip()
    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[archive_extract] start archives={len(archive_paths)} out={out_dir.resolve().as_posix()} "
        f"overwrite={overwrite} format={fmt}"
    )
    if include_globs:
        ctx.log(f"[archive_extract] include_glob={include_globs}")

    extracted_all: List[str] = []
    archive_list: List[str] = []
    for archive in archive_paths:
        suffix = archive.suffix.lower()
        chosen = fmt if fmt != "auto" else ("zip" if suffix == ".zip" else "7z" if suffix == ".7z" else "")
        if chosen not in {"zip", "7z"}:
            raise ValueError(f"Unsupported archive format for {archive}: use .zip/.7z or set format=zip|7z")
        archive_list.append(archive.resolve().as_posix())
        if verbose:
            ctx.log(f"[archive_extract] processing archive={archive.resolve().as_posix()} as={chosen}")
        if chosen == "zip":
            members = _extract_zip(archive, out_dir, overwrite=overwrite, include_globs=include_globs)
        else:
            members = _extract_7z(
                archive,
                out_dir,
                overwrite=overwrite,
                seven_zip_bin=seven_zip_bin,
                include_globs=include_globs,
                ctx=ctx,
            )
        extracted_all.extend((out_dir / rel).resolve().as_posix() for rel in members)

    extracted_files = sorted(set(extracted_all))
    ctx.log(f"[archive_extract] done extracted_count={len(extracted_files)}")
    if verbose and extracted_files:
        preview = extracted_files[:5]
        ctx.log(f"[archive_extract] extracted_preview={preview}")
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "archives": archive_list,
        "extracted_files": extracted_files,
        "extracted_count": len(extracted_files),
        "format": fmt,
    }

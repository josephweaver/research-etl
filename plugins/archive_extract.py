from __future__ import annotations

import os
import shlex
import subprocess
import zipfile
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
        "overwrite": {"type": "bool", "default": True},
        "seven_zip_bin": {"type": "str", "default": "7z"},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(path_text).expanduser()
    if p.is_absolute():
        return p
    if p.exists():
        return p
    return (ctx.workdir / p)


def _all_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*") if p.is_file())


def _norm_members(members: List[str]) -> List[str]:
    clean: List[str] = []
    for item in members:
        rel = str(item or "").replace("\\", "/").strip().lstrip("/")
        if not rel or rel.endswith("/"):
            continue
        clean.append(rel)
    return sorted(set(clean))


def _extract_zip(archive: Path, out_dir: Path, *, overwrite: bool) -> List[str]:
    extracted: List[str] = []
    with zipfile.ZipFile(archive, mode="r") as zf:
        for info in zf.infolist():
            rel = str(info.filename).replace("\\", "/").strip().lstrip("/")
            if not rel or rel.endswith("/"):
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


def _extract_7z(archive: Path, out_dir: Path, *, overwrite: bool, seven_zip_bin: str, ctx) -> List[str]:
    if py7zr is not None:
        with py7zr.SevenZipFile(archive, mode="r") as zf:
            members = _norm_members(list(zf.getnames()))
            if overwrite:
                zf.extractall(path=out_dir)
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
    if not overwrite:
        cmd.append("-aos")
    ctx.log(f"[archive_extract] running: {shlex.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"7z extraction failed: {detail}")
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
        archive_paths.extend(sorted(Path(".").glob(archive_glob)))
    archive_paths = [p for p in archive_paths if p.exists()]
    if not archive_paths:
        raise FileNotFoundError("No archive files found for provided archive/archive_glob")

    fmt = str(args.get("format") or "auto").strip().lower()
    overwrite = bool(args.get("overwrite", True))
    seven_zip_bin = str(args.get("seven_zip_bin") or "7z").strip()

    extracted_all: List[str] = []
    archive_list: List[str] = []
    for archive in archive_paths:
        suffix = archive.suffix.lower()
        chosen = fmt if fmt != "auto" else ("zip" if suffix == ".zip" else "7z" if suffix == ".7z" else "")
        if chosen not in {"zip", "7z"}:
            raise ValueError(f"Unsupported archive format for {archive}: use .zip/.7z or set format=zip|7z")
        archive_list.append(archive.resolve().as_posix())
        if chosen == "zip":
            members = _extract_zip(archive, out_dir, overwrite=overwrite)
        else:
            members = _extract_7z(
                archive,
                out_dir,
                overwrite=overwrite,
                seven_zip_bin=seven_zip_bin,
                ctx=ctx,
            )
        extracted_all.extend((out_dir / rel).resolve().as_posix() for rel in members)

    extracted_files = sorted(set(extracted_all))
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "archives": archive_list,
        "extracted_files": extracted_files,
        "extracted_count": len(extracted_files),
        "format": fmt,
    }


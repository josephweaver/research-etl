# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import zipfile
from pathlib import Path


meta = {
    "name": "zip_create",
    "version": "0.1.0",
    "description": "Create a zip archive from one or more files/directories.",
    "inputs": [],
    "outputs": ["output_zip", "input_count", "file_count", "size_bytes"],
    "params": {
        "input_paths": {"type": "str", "default": ""},
        "output_zip": {"type": "str", "default": ""},
        "overwrite": {"type": "bool", "default": True},
        "compression": {"type": "str", "default": "deflated"},
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


def _parse_csv_values(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    row = next(csv.reader([text], skipinitialspace=True), [])
    return [str(v).strip() for v in row if str(v).strip()]


def _compression_mode(raw: str) -> int:
    token = str(raw or "deflated").strip().lower().replace("-", "_")
    if token in {"stored", "none"}:
        return zipfile.ZIP_STORED
    if token in {"deflated", "zip_deflated"}:
        return zipfile.ZIP_DEFLATED
    if token in {"bzip2", "zip_bzip2"} and hasattr(zipfile, "ZIP_BZIP2"):
        return zipfile.ZIP_BZIP2
    if token in {"lzma", "zip_lzma"} and hasattr(zipfile, "ZIP_LZMA"):
        return zipfile.ZIP_LZMA
    raise ValueError("compression must be one of: stored, deflated, bzip2, lzma")


def run(args, ctx):
    output_text = str(args.get("output_zip") or "").strip()
    if not output_text:
        raise ValueError("output_zip is required")
    input_tokens = _parse_csv_values(str(args.get("input_paths") or ""))
    if not input_tokens:
        raise ValueError("input_paths is required (CSV list)")

    output_zip = _resolve_path(output_text, ctx)
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    overwrite = bool(args.get("overwrite", True))
    if output_zip.exists() and not overwrite:
        raise FileExistsError(f"output_zip exists and overwrite=false: {output_zip}")
    if output_zip.exists() and overwrite:
        output_zip.unlink()

    inputs: list[Path] = []
    for token in input_tokens:
        p = _resolve_path(token, ctx)
        if not p.exists():
            raise FileNotFoundError(f"input path not found: {p}")
        inputs.append(p)

    compression = _compression_mode(str(args.get("compression") or "deflated"))
    verbose = bool(args.get("verbose", False))

    file_count = 0
    with zipfile.ZipFile(output_zip, mode="w", compression=compression) as zf:
        for src in inputs:
            if src.is_file():
                zf.write(src, arcname=src.name)
                file_count += 1
                if verbose:
                    ctx.log(f"[zip_create] add file {src.as_posix()} as {src.name}")
                continue

            root_name = src.name
            for child in sorted(src.rglob("*")):
                if not child.is_file():
                    continue
                rel = child.relative_to(src).as_posix()
                arcname = f"{root_name}/{rel}" if rel else root_name
                zf.write(child, arcname=arcname)
                file_count += 1
                if verbose and file_count <= 30:
                    ctx.log(f"[zip_create] add file {child.as_posix()} as {arcname}")

    size_bytes = int(output_zip.stat().st_size) if output_zip.exists() else 0
    ctx.log(
        f"[zip_create] output={output_zip.as_posix()} input_count={len(inputs)} "
        f"file_count={file_count} size_bytes={size_bytes}"
    )

    return {
        "output_zip": output_zip.resolve().as_posix(),
        "input_count": int(len(inputs)),
        "file_count": int(file_count),
        "size_bytes": int(size_bytes),
    }

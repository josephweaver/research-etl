# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import glob
from pathlib import Path

try:
    import rasterio  # type: ignore
    from rasterio.merge import merge  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    merge = None


meta = {
    "name": "geo_raster_combine",
    "version": "0.1.0",
    "description": "Merge one or more rasters into a single mosaic raster.",
    "inputs": [],
    "outputs": ["output_path", "input_count", "input_files"],
    "params": {
        "input_glob": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "band": {"type": "int", "default": 1},
        "compress": {"type": "str", "default": "LZW"},
        "overwrite": {"type": "bool", "default": False},
        "allow_empty": {"type": "bool", "default": False},
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


def run(args, ctx):
    if rasterio is None or merge is None:
        raise RuntimeError("geo_raster_combine requires rasterio. Install requirements.txt in the active environment.")

    pattern = str(args.get("input_glob") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    if not pattern:
        raise ValueError("input_glob is required")
    if not output_text:
        raise ValueError("output_path is required")

    output_path = _resolve_path(output_text, ctx)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    raw_matches = sorted(glob.glob(pattern, recursive=True))
    if not raw_matches:
        raw_matches = sorted(glob.glob(str((ctx.workdir / pattern).as_posix()), recursive=True))
    input_paths = [Path(p).resolve() for p in raw_matches if Path(p).is_file()]

    allow_empty = bool(args.get("allow_empty", False))
    overwrite = bool(args.get("overwrite", False))
    verbose = bool(args.get("verbose", False))
    if not input_paths:
        if allow_empty:
            return {
                "output_path": output_path.resolve().as_posix(),
                "input_count": 0,
                "input_files": [],
            }
        raise FileNotFoundError(f"no rasters matched input_glob: {pattern}")

    if output_path.exists() and not overwrite:
        return {
            "output_path": output_path.resolve().as_posix(),
            "input_count": len(input_paths),
            "input_files": [p.as_posix() for p in input_paths],
        }

    srcs = [rasterio.open(path) for path in input_paths]
    try:
        mosaic, transform = merge(srcs)
        profile = srcs[0].profile.copy()
        profile.update(
            {
                "driver": "GTiff",
                "height": int(mosaic.shape[1]),
                "width": int(mosaic.shape[2]),
                "transform": transform,
                "count": int(mosaic.shape[0]),
                "compress": str(args.get("compress") or "LZW").strip() or "LZW",
            }
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(mosaic)
    finally:
        for src in srcs:
            src.close()

    if verbose:
        ctx.log(
            f"[geo_raster_combine] inputs={len(input_paths)} output={output_path.as_posix()} first_input={input_paths[0].as_posix()}"
        )

    return {
        "output_path": output_path.resolve().as_posix(),
        "input_count": len(input_paths),
        "input_files": [p.as_posix() for p in input_paths],
        "_artifacts": [
            {
                "uri": output_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": True,
            }
        ],
    }

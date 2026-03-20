# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import numpy as np  # type: ignore
except Exception:  # noqa: BLE001
    np = None

try:
    import rasterio  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None


meta = {
    "name": "geo_raster_multiply",
    "version": "0.1.0",
    "description": "Multiply one raster or a raster directory by a single weight raster on the same grid.",
    "inputs": [],
    "outputs": ["output_dir", "generated_count", "generated_files", "weight_raster_path"],
    "params": {
        "input_raster": {"type": "str", "default": ""},
        "input_dir": {"type": "str", "default": ""},
        "filename_glob": {"type": "str", "default": "*.tif"},
        "weight_raster": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ""},
        "band": {"type": "int", "default": 1},
        "weight_band": {"type": "int", "default": 1},
        "output_dtype": {"type": "str", "default": "float32"},
        "nodata": {"type": "str", "default": ""},
        "compress": {"type": "str", "default": "LZW"},
        "overwrite": {"type": "bool", "default": False},
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


def _same_grid(a, b) -> bool:
    return (
        int(a.width) == int(b.width)
        and int(a.height) == int(b.height)
        and str(a.crs or "") == str(b.crs or "")
        and tuple(a.transform) == tuple(b.transform)
    )


def _write_product(input_path: Path, output_path: Path, *, weight_path: Path, band: int, weight_band: int, output_dtype: str, nodata_text: str, compress: str) -> None:
    with rasterio.open(input_path) as src, rasterio.open(weight_path) as wgt:
        if not _same_grid(src, wgt):
            raise ValueError(f"weight raster does not share grid with input raster: {input_path.name} vs {weight_path.name}")
        arr = src.read(band, masked=True).astype("float32")
        warr = wgt.read(weight_band, masked=True).astype("float32")
        combined_mask = np.ma.getmaskarray(arr) | np.ma.getmaskarray(warr)
        out = np.ma.array(np.asarray(arr.filled(0), dtype="float32") * np.asarray(warr.filled(0), dtype="float32"), mask=combined_mask)
        nodata_value = float(nodata_text) if str(nodata_text or "").strip() else None
        profile = src.profile.copy()
        profile.update(driver="GTiff", count=1, dtype=output_dtype, compress=compress)
        if nodata_value is not None:
            profile.update(nodata=nodata_value)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(out.filled(nodata_value if nodata_value is not None else 0).astype(output_dtype), 1)


def run(args, ctx):
    if rasterio is None or np is None:
        raise RuntimeError("geo_raster_multiply requires rasterio and numpy. Install requirements.txt.")

    input_raster = str(args.get("input_raster") or "").strip()
    input_dir = str(args.get("input_dir") or "").strip()
    if bool(input_raster) == bool(input_dir):
        raise ValueError("exactly one of input_raster or input_dir is required")
    weight_raster = str(args.get("weight_raster") or "").strip()
    if not weight_raster:
        raise ValueError("weight_raster is required")

    filename_glob = str(args.get("filename_glob") or "*.tif").strip() or "*.tif"
    output_path_text = str(args.get("output_path") or "").strip()
    output_dir_text = str(args.get("output_dir") or "").strip()
    if input_raster and not output_path_text:
        raise ValueError("output_path is required when input_raster is used")
    if input_dir and not output_dir_text:
        raise ValueError("output_dir is required when input_dir is used")

    weight_path = _resolve_path(weight_raster, ctx)
    if not weight_path.exists():
        raise FileNotFoundError(f"weight_raster not found: {weight_path}")

    band = int(args.get("band") or 1)
    weight_band = int(args.get("weight_band") or 1)
    output_dtype = str(args.get("output_dtype") or "float32").strip() or "float32"
    nodata_text = str(args.get("nodata") or "").strip()
    compress = str(args.get("compress") or "LZW").strip() or "LZW"
    overwrite = bool(args.get("overwrite", False))

    generated_files: list[str] = []
    if input_raster:
        src_path = _resolve_path(input_raster, ctx)
        out_path = _resolve_path(output_path_text, ctx)
        if not src_path.exists():
            raise FileNotFoundError(f"input_raster not found: {src_path}")
        if out_path.exists() and not overwrite:
            return {
                "output_dir": out_path.parent.resolve().as_posix(),
                "generated_count": 0,
                "generated_files": [],
                "weight_raster_path": weight_path.resolve().as_posix(),
            }
        _write_product(src_path, out_path, weight_path=weight_path, band=band, weight_band=weight_band, output_dtype=output_dtype, nodata_text=nodata_text, compress=compress)
        generated_files.append(out_path.resolve().as_posix())
        return {
            "output_dir": out_path.parent.resolve().as_posix(),
            "generated_count": 1,
            "generated_files": generated_files,
            "weight_raster_path": weight_path.resolve().as_posix(),
        }

    in_dir = _resolve_path(input_dir, ctx)
    out_dir = _resolve_path(output_dir_text, ctx)
    if not in_dir.exists():
        raise FileNotFoundError(f"input_dir not found: {in_dir}")
    for src_path in sorted(p for p in in_dir.rglob(filename_glob) if p.is_file()):
        out_path = out_dir / src_path.name
        if out_path.exists() and not overwrite:
            continue
        _write_product(src_path, out_path, weight_path=weight_path, band=band, weight_band=weight_band, output_dtype=output_dtype, nodata_text=nodata_text, compress=compress)
        generated_files.append(out_path.resolve().as_posix())
    ctx.log(f"[geo_raster_multiply] input_dir={in_dir.as_posix()} generated={len(generated_files)}")
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "generated_count": int(len(generated_files)),
        "generated_files": generated_files,
        "weight_raster_path": weight_path.resolve().as_posix(),
    }

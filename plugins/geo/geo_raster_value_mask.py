# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import ast
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
    "name": "geo_raster_value_mask",
    "version": "0.1.0",
    "description": "Convert a raster band to a binary mask using explicit values and/or inclusive value ranges.",
    "inputs": [],
    "outputs": [
        "input_raster",
        "output_path",
        "band",
        "included_value_count",
        "included_range_count",
        "matched_pixel_count",
        "valid_pixel_count",
        "height",
        "width",
    ],
    "params": {
        "input_raster": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "band": {"type": "int", "default": 1},
        "include_values": {"type": "str", "default": ""},
        "include_ranges": {"type": "str", "default": ""},
        "invert": {"type": "bool", "default": False},
        "output_dtype": {"type": "str", "default": "uint8"},
        "mask_true_value": {"type": "int", "default": 1},
        "mask_false_value": {"type": "int", "default": 0},
        "mask_nodata_value": {"type": "int", "default": 255},
        "preserve_source_nodata": {"type": "bool", "default": True},
        "overwrite": {"type": "bool", "default": True},
        "compress": {"type": "str", "default": "LZW"},
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


def _parse_tokens(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return list(raw)
    text = str(raw).strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except Exception:
        parsed = None
    if isinstance(parsed, (list, tuple, set)):
        return list(parsed)
    return [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]


def _parse_include_values(raw: Any) -> list[float]:
    values: list[float] = []
    for token in _parse_tokens(raw):
        values.append(float(token))
    return values


def _parse_include_ranges(raw: Any) -> list[tuple[float, float]]:
    ranges: list[tuple[float, float]] = []
    for token in _parse_tokens(raw):
        if isinstance(token, (list, tuple)) and len(token) == 2:
            lo = float(token[0])
            hi = float(token[1])
        else:
            text = str(token).strip()
            if ".." in text:
                left, right = text.split("..", 1)
            elif ":" in text:
                left, right = text.split(":", 1)
            elif "-" in text[1:]:
                left, right = text.split("-", 1)
            else:
                raise ValueError(f"range token must look like min-max, min..max, or [min,max]; got: {text}")
            lo = float(left.strip())
            hi = float(right.strip())
        if hi < lo:
            lo, hi = hi, lo
        ranges.append((lo, hi))
    return ranges


def run(args, ctx):
    if rasterio is None or np is None:
        raise RuntimeError("geo_raster_value_mask requires rasterio and numpy. Install requirements.txt.")

    input_text = str(args.get("input_raster") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    if not input_text:
        raise ValueError("input_raster is required")
    if not output_text:
        raise ValueError("output_path is required")

    band = int(args.get("band") or 1)
    include_values = _parse_include_values(args.get("include_values"))
    include_ranges = _parse_include_ranges(args.get("include_ranges"))
    if not include_values and not include_ranges:
        raise ValueError("at least one of include_values or include_ranges is required")

    invert = bool(args.get("invert", False))
    output_dtype = str(args.get("output_dtype") or "uint8").strip() or "uint8"
    true_value = int(args.get("mask_true_value") or 1)
    false_value = int(args.get("mask_false_value") or 0)
    nodata_value = int(args.get("mask_nodata_value") or 255)
    preserve_source_nodata = bool(args.get("preserve_source_nodata", True))
    overwrite = bool(args.get("overwrite", True))
    compress = str(args.get("compress") or "LZW").strip() or "LZW"
    verbose = bool(args.get("verbose", False))

    input_path = _resolve_path(input_text, ctx)
    output_path = _resolve_path(output_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_raster not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output_path exists and overwrite=false: {output_path}")

    with rasterio.open(input_path) as ds:
        if band < 1 or band > ds.count:
            raise ValueError(f"band out of range: {band}; raster band count={ds.count}")
        arr = ds.read(band, masked=True)
        valid_mask = ~np.ma.getmaskarray(arr)
        data = np.asarray(arr.filled(0), dtype="float64")

        selected = np.zeros(data.shape, dtype=bool)
        for value in include_values:
            selected |= np.isclose(data, float(value), equal_nan=False)
        for lo, hi in include_ranges:
            selected |= ((data >= float(lo)) & (data <= float(hi)))
        if invert:
            selected = ~selected
        if preserve_source_nodata:
            selected &= valid_mask

        out = np.where(selected, true_value, false_value).astype(output_dtype)
        if preserve_source_nodata:
            out = np.where(valid_mask, out, nodata_value).astype(output_dtype)

        profile = ds.profile.copy()
        profile.update(
            {
                "driver": "GTiff",
                "count": 1,
                "dtype": output_dtype,
                "nodata": nodata_value if preserve_source_nodata else None,
                "compress": compress,
            }
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(out, 1)

    matched_pixel_count = int(np.count_nonzero(selected))
    valid_pixel_count = int(np.count_nonzero(valid_mask))
    ctx.log(
        f"[geo_raster_value_mask] input={input_path.as_posix()} output={output_path.as_posix()} "
        f"band={band} matched={matched_pixel_count} valid={valid_pixel_count}"
    )
    if verbose:
        ctx.log(
            f"[geo_raster_value_mask] include_values={include_values} include_ranges={include_ranges} "
            f"invert={invert} true={true_value} false={false_value} nodata={nodata_value}"
        )

    return {
        "input_raster": input_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "band": int(band),
        "included_value_count": int(len(include_values)),
        "included_range_count": int(len(include_ranges)),
        "matched_pixel_count": matched_pixel_count,
        "valid_pixel_count": valid_pixel_count,
        "height": int(out.shape[0]),
        "width": int(out.shape[1]),
    }

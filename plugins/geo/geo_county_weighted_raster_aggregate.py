# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import re
from pathlib import Path

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None

try:
    import numpy as np  # type: ignore
except Exception:  # noqa: BLE001
    np = None

try:
    import rasterio  # type: ignore
    from rasterio.mask import mask  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    mask = None

try:
    from shapely.geometry import box  # type: ignore
except Exception:  # noqa: BLE001
    box = None


meta = {
    "name": "geo_county_weighted_raster_aggregate",
    "version": "0.1.0",
    "description": "Aggregate weighted rasters by county using a same-grid denominator raster.",
    "inputs": [],
    "outputs": ["output_path", "row_count", "input_count", "county_count"],
    "params": {
        "input_raster": {"type": "str", "default": ""},
        "input_dir": {"type": "str", "default": ""},
        "filename_glob": {"type": "str", "default": "*.tif"},
        "weight_raster": {"type": "str", "default": ""},
        "county_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "county_id_field": {"type": "str", "default": "GEOID"},
        "county_name_field": {"type": "str", "default": "NAME"},
        "value_prefix": {"type": "str", "default": "value"},
        "band": {"type": "int", "default": 1},
        "weight_band": {"type": "int", "default": 1},
        "all_touched": {"type": "bool", "default": False},
        "day_from_filename_regex": {"type": "str", "default": ""},
        "day_from_filename_group": {"type": "int", "default": 1},
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


def _resolve_day(path: Path, pattern: str, group_idx: int) -> str:
    if not pattern:
        return ""
    m = re.search(pattern, path.name)
    if not m:
        return ""
    try:
        return str(m.group(group_idx) or "")
    except Exception:
        return ""


def _iter_input_paths(input_raster: str, input_dir: str, filename_glob: str, ctx) -> list[Path]:
    if bool(input_raster) == bool(input_dir):
        raise ValueError("exactly one of input_raster or input_dir is required")
    if input_raster:
        path = _resolve_path(input_raster, ctx)
        return [path]
    root = _resolve_path(input_dir, ctx)
    if not root.exists():
        raise FileNotFoundError(f"input_dir not found: {root}")
    return sorted(p for p in root.rglob(filename_glob) if p.is_file())


def run(args, ctx):
    if gpd is None or rasterio is None or mask is None or np is None or box is None:
        raise RuntimeError("geo_county_weighted_raster_aggregate requires geopandas, rasterio, numpy, and shapely.")

    inputs = _iter_input_paths(str(args.get("input_raster") or "").strip(), str(args.get("input_dir") or "").strip(), str(args.get("filename_glob") or "*.tif").strip() or "*.tif", ctx)
    if not inputs:
        raise ValueError("no input rasters matched")
    weight_path = _resolve_path(str(args.get("weight_raster") or "").strip(), ctx)
    county_path = _resolve_path(str(args.get("county_path") or "").strip(), ctx)
    output_path = _resolve_path(str(args.get("output_path") or "").strip(), ctx)
    if not weight_path.exists():
        raise FileNotFoundError(f"weight_raster not found: {weight_path}")
    if not county_path.exists():
        raise FileNotFoundError(f"county_path not found: {county_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    county_id_field = str(args.get("county_id_field") or "GEOID").strip() or "GEOID"
    county_name_field = str(args.get("county_name_field") or "NAME").strip()
    value_prefix = str(args.get("value_prefix") or "value").strip() or "value"
    band = int(args.get("band") or 1)
    weight_band = int(args.get("weight_band") or 1)
    all_touched = bool(args.get("all_touched", False))
    day_pattern = str(args.get("day_from_filename_regex") or "").strip()
    day_group = int(args.get("day_from_filename_group", 1) or 1)

    counties = gpd.read_file(county_path)
    counties = counties[counties.geometry.notna() & ~counties.geometry.is_empty].copy()
    if counties.empty:
        raise ValueError("county_path contains no features")
    if county_id_field not in counties.columns:
        raise ValueError(f"county_id_field '{county_id_field}' not found")
    if county_name_field and county_name_field not in counties.columns:
        raise ValueError(f"county_name_field '{county_name_field}' not found")

    rows: list[dict[str, object]] = []
    with rasterio.open(weight_path) as wgt_ds:
        if wgt_ds.crs is None:
            raise ValueError("weight_raster CRS is required")
        counties_ds = counties.to_crs(wgt_ds.crs)
        bounds_geom = box(float(wgt_ds.bounds.left), float(wgt_ds.bounds.bottom), float(wgt_ds.bounds.right), float(wgt_ds.bounds.top))
        counties_ds = counties_ds[counties_ds.geometry.intersects(bounds_geom)].copy()
        for input_path in inputs:
            with rasterio.open(input_path) as src_ds:
                if not _same_grid(src_ds, wgt_ds):
                    raise ValueError(f"input raster does not share grid with weight raster: {input_path.name}")
                for _, row in counties_ds.iterrows():
                    geom = row.geometry
                    day = _resolve_day(input_path, day_pattern, day_group)
                    rec: dict[str, object] = {
                        "county_id": row[county_id_field],
                        "raster_path": input_path.resolve().as_posix(),
                    }
                    if county_name_field:
                        rec["county_name"] = row[county_name_field]
                    if day:
                        rec["day"] = day
                    try:
                        num_masked, _ = mask(src_ds, [geom], crop=True, indexes=band, filled=False, all_touched=all_touched)
                        den_masked, _ = mask(wgt_ds, [geom], crop=True, indexes=weight_band, filled=False, all_touched=all_touched)
                        num = np.ma.asarray(num_masked).compressed()
                        den = np.ma.asarray(den_masked).compressed()
                        count = int(min(len(num), len(den)))
                        if count == 0:
                            weight_sum = 0.0
                            value_sum = 0.0
                            weighted_mean = None
                        else:
                            # numerator raster is already value * weight
                            value_sum = float(np.sum(num[:count]))
                            weight_sum = float(np.sum(den[:count]))
                            weighted_mean = float(value_sum / weight_sum) if weight_sum > 0 else None
                    except ValueError:
                        value_sum = 0.0
                        weight_sum = 0.0
                        weighted_mean = None
                        count = 0
                    rec[f"{value_prefix}_weighted_sum"] = value_sum
                    rec[f"{value_prefix}_weight_sum"] = weight_sum
                    rec[f"{value_prefix}_weighted_mean"] = weighted_mean
                    rec[f"{value_prefix}_count"] = count
                    rows.append(rec)

    fieldnames = ["county_id"]
    if county_name_field:
        fieldnames.append("county_name")
    if any("day" in r for r in rows):
        fieldnames.append("day")
    fieldnames.extend(
        [
            f"{value_prefix}_weighted_sum",
            f"{value_prefix}_weight_sum",
            f"{value_prefix}_weighted_mean",
            f"{value_prefix}_count",
            "raster_path",
        ]
    )
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    ctx.log(f"[geo_county_weighted_raster_aggregate] inputs={len(inputs)} rows={len(rows)} output={output_path.as_posix()}")
    return {
        "output_path": output_path.resolve().as_posix(),
        "row_count": int(len(rows)),
        "input_count": int(len(inputs)),
        "county_count": int(len(counties)),
    }

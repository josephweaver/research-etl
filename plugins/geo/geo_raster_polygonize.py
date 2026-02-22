# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Any, Iterable, List

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
    from rasterio.features import shapes as raster_shapes  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    raster_shapes = None

try:
    from shapely.geometry import shape as shapely_shape  # type: ignore
except Exception:  # noqa: BLE001
    shapely_shape = None


meta = {
    "name": "geo_raster_polygonize",
    "version": "0.1.0",
    "description": "Polygonize raster cells by value with optional value excludes and polygon mask filters.",
    "inputs": [],
    "outputs": [
        "input_raster",
        "output_path",
        "value_field",
        "source_crs",
        "target_crs",
        "source_feature_count",
        "output_feature_count",
        "excluded_value_count",
        "selector_count",
        "source_name_field",
    ],
    "params": {
        "input_raster": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "band": {"type": "int", "default": 1},
        "value_field": {"type": "str", "default": "value"},
        "source_name_field": {"type": "str", "default": "source_name"},
        "exclude_values": {"type": "str", "default": ""},
        "exclude_nodata": {"type": "bool", "default": True},
        "nodata_value": {"type": "str", "default": ""},
        "selector_path": {"type": "str", "default": ""},
        "selector_paths": {"type": "str", "default": ""},
        "selector_mode": {"type": "str", "default": "any"},
        "filter_predicate": {"type": "str", "default": "intersects"},
        "target_crs": {"type": "str", "default": ""},
        "driver": {"type": "str", "default": ""},
        "dissolve_by_value": {"type": "bool", "default": False},
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


def _remove_existing_output(output_path: Path) -> None:
    ext = output_path.suffix.lower()
    if ext == ".shp":
        for sidecar in [".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix"]:
            candidate = output_path.with_suffix(sidecar)
            if candidate.exists():
                candidate.unlink()
        return
    if output_path.exists():
        output_path.unlink()


def _driver_for_path(path: Path, override: str) -> str | None:
    if override.strip():
        return override.strip()
    ext = path.suffix.lower()
    if ext == ".gpkg":
        return "GPKG"
    if ext == ".shp":
        return "ESRI Shapefile"
    if ext in {".geojson", ".json"}:
        return "GeoJSON"
    return None


def _parse_csv_values(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    row = next(csv.reader([text], skipinitialspace=True), [])
    return [str(v).strip() for v in row if str(v).strip()]


def _parse_numeric_values(raw: str) -> list[float]:
    out: list[float] = []
    for token in _parse_csv_values(raw):
        out.append(float(token))
    return out


def _parse_selector_paths(selector_path: str, selector_paths: str) -> list[str]:
    out: list[str] = []
    for token in [str(selector_path or "").strip()] + _parse_csv_values(selector_paths):
        t = str(token or "").strip()
        if not t:
            continue
        if t not in out:
            out.append(t)
    return out


def _normalize_mode(mode: str) -> str:
    token = str(mode or "any").strip().lower()
    if token in {"any", "or"}:
        return "any"
    if token in {"all", "and"}:
        return "all"
    raise ValueError("selector_mode must be one of: any, all")


def _normalize_predicate(pred: str) -> str:
    token = str(pred or "intersects").strip().lower().replace("-", "_")
    if token in {"intersects", "within", "centroid_within"}:
        return token
    raise ValueError("filter_predicate must be one of: intersects, within, centroid_within")


def _values_equal(arr, value: float):
    if np is None:
        raise RuntimeError("numpy is required")
    if isinstance(value, float) and math.isnan(value):
        return np.isnan(arr.astype("float64"))
    return np.isclose(arr.astype("float64"), float(value), equal_nan=True)


def _apply_selector_filter(gdf, selectors: list[Any], mode: str, predicate: str):
    if np is None:
        raise RuntimeError("numpy is required")
    if not selectors:
        return gdf
    combined = np.ones(len(gdf), dtype=bool) if mode == "all" else np.zeros(len(gdf), dtype=bool)
    for geom in selectors:
        if predicate == "intersects":
            hits = gdf.geometry.intersects(geom).to_numpy()
        elif predicate == "within":
            hits = gdf.geometry.within(geom).to_numpy()
        else:
            hits = gdf.geometry.centroid.within(geom).to_numpy()
        combined = (combined & hits) if mode == "all" else (combined | hits)
    return gdf.loc[combined].copy()


def run(args, ctx):
    if gpd is None or rasterio is None or raster_shapes is None or shapely_shape is None or np is None:
        raise RuntimeError(
            "geo_raster_polygonize requires geopandas, rasterio, shapely, and numpy. Install requirements.txt."
        )

    input_text = str(args.get("input_raster") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    if not input_text:
        raise ValueError("input_raster is required")
    if not output_text:
        raise ValueError("output_path is required")

    band = int(args.get("band") or 1)
    value_field = str(args.get("value_field") or "value").strip() or "value"
    source_name_field = str(args.get("source_name_field") or "source_name").strip() or "source_name"
    exclude_values = _parse_numeric_values(str(args.get("exclude_values") or ""))
    exclude_nodata = bool(args.get("exclude_nodata", True))
    nodata_override_raw = str(args.get("nodata_value") or "").strip()
    nodata_override = float(nodata_override_raw) if nodata_override_raw else None
    selector_paths = _parse_selector_paths(str(args.get("selector_path") or ""), str(args.get("selector_paths") or ""))
    selector_mode = _normalize_mode(str(args.get("selector_mode") or "any"))
    predicate = _normalize_predicate(str(args.get("filter_predicate") or "intersects"))
    target_crs = str(args.get("target_crs") or "").strip()
    dissolve_by_value = bool(args.get("dissolve_by_value", False))
    overwrite = bool(args.get("overwrite", True))
    verbose = bool(args.get("verbose", False))

    input_path = _resolve_path(input_text, ctx)
    output_path = _resolve_path(output_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_raster not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output_path exists and overwrite=false: {output_path}")

    records: list[dict[str, Any]] = []
    source_name = input_path.name
    with rasterio.open(input_path) as ds:
        if ds.crs is None:
            raise ValueError("input raster CRS is required")
        if band < 1 or band > ds.count:
            raise ValueError(f"band out of range: {band}; raster band count={ds.count}")

        src_crs_text = str(ds.crs.to_string() if hasattr(ds.crs, "to_string") else ds.crs)
        data = ds.read(band, masked=False)
        valid = np.ones(data.shape, dtype=bool)

        nodata_value = nodata_override if nodata_override is not None else ds.nodata
        if exclude_nodata and nodata_value is not None:
            valid = valid & (~_values_equal(data, float(nodata_value)))
        for value in exclude_values:
            valid = valid & (~_values_equal(data, float(value)))

        for geom_json, raw_val in raster_shapes(data, mask=valid, transform=ds.transform):
            geom = shapely_shape(geom_json)
            if geom.is_empty:
                continue
            value = raw_val.item() if hasattr(raw_val, "item") else raw_val
            records.append({value_field: value, source_name_field: source_name, "geometry": geom})

    source_count = len(records)
    if records:
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs=src_crs_text)
    else:
        gdf = gpd.GeoDataFrame({value_field: [], source_name_field: []}, geometry=[], crs=src_crs_text)

    if target_crs:
        gdf = gdf.to_crs(target_crs)
    out_crs_text = str(gdf.crs.to_string() if hasattr(gdf.crs, "to_string") else gdf.crs)

    selector_geoms: list[Any] = []
    for selector_text in selector_paths:
        selector_path = _resolve_path(selector_text, ctx)
        if not selector_path.exists():
            raise FileNotFoundError(f"selector_path not found: {selector_path}")
        selector_gdf = gpd.read_file(selector_path)
        if selector_gdf.empty:
            continue
        if selector_gdf.crs is None:
            raise ValueError(f"selector has no CRS: {selector_path}")
        selector_proj = selector_gdf.to_crs(gdf.crs)
        try:
            geom = selector_proj.unary_union
        except Exception:  # noqa: BLE001
            geom = selector_proj.geometry.unary_union
        if geom is not None and not geom.is_empty:
            selector_geoms.append(geom)

    filtered = _apply_selector_filter(gdf, selector_geoms, selector_mode, predicate)

    if dissolve_by_value and len(filtered):
        grouped = filtered[[value_field, source_name_field, "geometry"]].dissolve(
            by=[value_field, source_name_field]
        ).reset_index()
        filtered = grouped

    if overwrite:
        _remove_existing_output(output_path)
    driver = _driver_for_path(output_path, str(args.get("driver") or ""))
    if driver:
        filtered.to_file(output_path, driver=driver)
    else:
        filtered.to_file(output_path)

    excluded_count = max(0, source_count - len(filtered))
    ctx.log(
        f"[geo_raster_polygonize] input={input_path.as_posix()} output={output_path.as_posix()} "
        f"source={source_count} output={len(filtered)} selectors={len(selector_geoms)} "
        f"exclude_values={exclude_values} exclude_nodata={exclude_nodata}"
    )
    if verbose and len(filtered):
        ctx.log(f"[geo_raster_polygonize] output_columns={list(filtered.columns)}")

    return {
        "input_raster": input_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "value_field": value_field,
        "source_crs": src_crs_text,
        "target_crs": out_crs_text,
        "source_feature_count": int(source_count),
        "output_feature_count": int(len(filtered)),
        "excluded_value_count": int(excluded_count),
        "selector_count": int(len(selector_geoms)),
        "source_name_field": source_name_field,
    }

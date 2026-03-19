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
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None

try:
    import rasterio  # type: ignore
    from rasterio.mask import mask  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    mask = None


meta = {
    "name": "geo_clip_raster_by_polygon",
    "version": "0.1.0",
    "description": "Clip a raster to one or more polygons selected from a vector layer and write a cropped raster.",
    "inputs": [],
    "outputs": [
        "input_raster_path",
        "selector_path",
        "output_path",
        "feature_count",
        "selector_crs",
        "raster_crs",
        "filter_key",
        "filter_value",
        "band_count",
        "height",
        "width",
    ],
    "params": {
        "raster_path": {"type": "str", "default": ""},
        "selector_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "key": {"type": "str", "default": ""},
        "value": {"type": "str", "default": ""},
        "values": {"type": "str", "default": ""},
        "where": {"type": "str", "default": ""},
        "crop": {"type": "bool", "default": True},
        "all_touched": {"type": "bool", "default": False},
        "overwrite": {"type": "bool", "default": False},
        "compress": {"type": "str", "default": "LZW"},
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


def _coerce_list(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except Exception:  # noqa: BLE001
        parsed = None
    if isinstance(parsed, (list, tuple, set)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]


def _parse_where(where: str) -> tuple[str, list[str]]:
    text = str(where or "").strip()
    if not text:
        return "", []
    lower = text.lower()
    marker = " in "
    idx = lower.find(marker)
    if idx <= 0:
        raise ValueError("where must use the form '<field> in (a, b)'")
    key = text[:idx].strip()
    raw_values = text[idx + len(marker) :].strip()
    if not key:
        raise ValueError("where is missing field name")
    if raw_values.startswith("(") and raw_values.endswith(")"):
        raw_values = raw_values[1:-1]
    values = _coerce_list(raw_values)
    if not values:
        raise ValueError("where matched no values")
    return key, values


def _filter_features(selector, *, key: str, values: list[str]):
    if key not in selector.columns:
        raise ValueError(f"filter key '{key}' not found in selector columns")
    selected = selector[selector[key].astype(str).isin([str(v) for v in values])].copy()
    if selected.empty:
        raise ValueError(f"selector filter matched 0 features for {key} in {values}")
    return selected


def run(args, ctx):
    if gpd is None:
        raise RuntimeError(
            "geo_clip_raster_by_polygon requires geopandas and shapely. Install requirements.txt in the active environment."
        )
    if rasterio is None or mask is None:
        raise RuntimeError(
            "geo_clip_raster_by_polygon requires rasterio. Install requirements.txt in the active environment."
        )

    raster_text = str(args.get("raster_path") or "").strip()
    selector_text = str(args.get("selector_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    key = str(args.get("key") or "").strip()
    value = str(args.get("value") or "").strip()
    values = _coerce_list(args.get("values"))
    where = str(args.get("where") or "").strip()
    crop = bool(args.get("crop", True))
    all_touched = bool(args.get("all_touched", False))
    overwrite = bool(args.get("overwrite", False))
    compress = str(args.get("compress") or "LZW").strip() or "LZW"

    if not raster_text:
        raise ValueError("raster_path is required")
    if not selector_text:
        raise ValueError("selector_path is required")
    if not output_text:
        raise ValueError("output_path is required")

    raster_path = _resolve_path(raster_text, ctx)
    selector_path = _resolve_path(selector_text, ctx)
    output_path = _resolve_path(output_text, ctx)

    if not raster_path.exists():
        raise FileNotFoundError(f"raster_path not found: {raster_path}")
    if not selector_path.exists():
        raise FileNotFoundError(f"selector_path not found: {selector_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        ctx.log(f"[geo_clip_raster_by_polygon] skip existing {output_path.as_posix()}")
        with rasterio.open(output_path) as existing:
            return {
                "input_raster_path": raster_path.resolve().as_posix(),
                "selector_path": selector_path.resolve().as_posix(),
                "output_path": output_path.resolve().as_posix(),
                "feature_count": 0,
                "selector_crs": "",
                "raster_crs": str(existing.crs or ""),
                "filter_key": key,
                "filter_value": value or ",".join(values),
                "band_count": int(existing.count),
                "height": int(existing.height),
                "width": int(existing.width),
            }

    if where:
        if key or value or values:
            raise ValueError("where may not be combined with key/value/values")
        key, values = _parse_where(where)
    elif key:
        if value:
            values = [value]
        if not values:
            raise ValueError("key requires value or values")
    else:
        raise ValueError("one of where or key+value(s) is required")

    selector = gpd.read_file(selector_path)
    if selector.empty:
        raise ValueError("selector_path contains no features")
    if selector.crs is None:
        raise ValueError("selector_path CRS is required")

    selected = _filter_features(selector, key=key, values=values)
    selected = selected[selected.geometry.notna() & ~selected.geometry.is_empty].copy()
    if selected.empty:
        raise ValueError("selector filter left no valid geometries")

    with rasterio.open(raster_path) as ds:
        if ds.crs is None:
            raise ValueError("raster_path CRS is required")
        selected_in_raster_crs = selected.to_crs(ds.crs)
        geom_values = [geom for geom in selected_in_raster_crs.geometry if geom is not None and not geom.is_empty]
        if not geom_values:
            raise ValueError("selector filter left no geometries after reprojection")
        clipped, transform = mask(
            ds,
            geom_values,
            crop=crop,
            all_touched=all_touched,
        )
        meta_out = ds.meta.copy()
        meta_out.update(
            {
                "driver": "GTiff",
                "height": int(clipped.shape[1]),
                "width": int(clipped.shape[2]),
                "transform": transform,
                "compress": compress,
            }
        )
        with rasterio.open(output_path, "w", **meta_out) as dst:
            dst.write(clipped)

        selector_crs = str(selector.crs or "")
        raster_crs = str(ds.crs or "")

    ctx.log(
        f"[geo_clip_raster_by_polygon] raster={raster_path.as_posix()} output={output_path.as_posix()} "
        f"features={len(selected)} key={key} values={values}"
    )

    return {
        "input_raster_path": raster_path.resolve().as_posix(),
        "selector_path": selector_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "feature_count": int(len(selected)),
        "selector_crs": selector_crs,
        "raster_crs": raster_crs,
        "filter_key": key,
        "filter_value": ",".join(values),
        "band_count": int(clipped.shape[0]),
        "height": int(clipped.shape[1]),
        "width": int(clipped.shape[2]),
        "_artifacts": [
            {
                "uri": output_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": True,
            }
        ],
    }

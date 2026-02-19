from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

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
    "name": "geo_county_raster_aggregate",
    "version": "0.1.0",
    "description": "Aggregate raster values by county polygon into a day-level table.",
    "inputs": [],
    "outputs": [
        "input_raster_path",
        "county_path",
        "output_path",
        "row_count",
        "county_count",
        "intersecting_county_count",
        "non_intersecting_county_count",
        "aggregations",
        "day",
    ],
    "params": {
        "raster_path": {"type": "str", "default": ""},
        "county_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "county_id_field": {"type": "str", "default": "GEOID"},
        "county_name_field": {"type": "str", "default": ""},
        "aggregations": {"type": "str", "default": "sum,mean,count"},
        "value_prefix": {"type": "str", "default": "ppt"},
        "band": {"type": "int", "default": 1},
        "all_touched": {"type": "bool", "default": False},
        "day": {"type": "str", "default": ""},
        "day_from_filename_regex": {"type": "str", "default": ""},
        "day_from_filename_group": {"type": "int", "default": 1},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}

_ALLOWED_AGGS = {
    "sum",
    "mean",
    "avg",
    "min",
    "max",
    "median",
    "med",
    "p5",
    "q1",
    "q3",
    "p95",
    "std",
    "count",
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


def _parse_aggs(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        agg = str(token or "").strip().lower()
        if not agg:
            continue
        if agg not in _ALLOWED_AGGS:
            allowed = ", ".join(sorted(_ALLOWED_AGGS))
            raise ValueError(f"Unsupported aggregation '{agg}'. Allowed: {allowed}")
        if agg in seen:
            continue
        seen.add(agg)
        out.append(agg)
    if not out:
        raise ValueError("aggregations must include at least one value")
    return out


def _resolve_day(args: dict[str, Any], raster_path: Path) -> str:
    explicit = str(args.get("day") or "").strip()
    if explicit:
        return explicit
    pattern = str(args.get("day_from_filename_regex") or "").strip()
    if not pattern:
        return ""
    m = re.search(pattern, raster_path.name)
    if not m:
        return ""
    group_idx = int(args.get("day_from_filename_group", 1) or 1)
    try:
        return str(m.group(group_idx) or "")
    except Exception:  # noqa: BLE001
        return ""


def _empty_agg_values(agg_list: list[str]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for agg in agg_list:
        if agg == "count":
            values[agg] = 0
        else:
            values[agg] = None
    return values


def _compute_aggs(values: Any, agg_list: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    arr = np.asarray(values)
    if arr.size == 0:
        return _empty_agg_values(agg_list)

    for agg in agg_list:
        if agg == "sum":
            out[agg] = float(np.sum(arr))
        elif agg in {"mean", "avg"}:
            out[agg] = float(np.mean(arr))
        elif agg == "min":
            out[agg] = float(np.min(arr))
        elif agg == "max":
            out[agg] = float(np.max(arr))
        elif agg in {"median", "med"}:
            out[agg] = float(np.median(arr))
        elif agg == "p5":
            out[agg] = float(np.percentile(arr, 5))
        elif agg == "q1":
            out[agg] = float(np.percentile(arr, 25))
        elif agg == "q3":
            out[agg] = float(np.percentile(arr, 75))
        elif agg == "p95":
            out[agg] = float(np.percentile(arr, 95))
        elif agg == "std":
            out[agg] = float(np.std(arr))
        elif agg == "count":
            out[agg] = int(arr.size)
    return out


def run(args, ctx):
    if gpd is None or box is None:
        raise RuntimeError(
            "geo_county_raster_aggregate requires geopandas and shapely. Install requirements.txt in the active environment."
        )
    if rasterio is None or mask is None or np is None:
        raise RuntimeError(
            "geo_county_raster_aggregate requires rasterio and numpy. Install requirements.txt in the active environment."
        )

    raster_text = str(args.get("raster_path") or "").strip()
    county_text = str(args.get("county_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    county_id_field = str(args.get("county_id_field") or "GEOID").strip()
    county_name_field = str(args.get("county_name_field") or "").strip()
    value_prefix = str(args.get("value_prefix") or "ppt").strip() or "ppt"
    band = int(args.get("band", 1) or 1)
    all_touched = bool(args.get("all_touched", False))
    verbose = bool(args.get("verbose", False))

    if not raster_text:
        raise ValueError("raster_path is required")
    if not county_text:
        raise ValueError("county_path is required")

    raster_path = _resolve_path(raster_text, ctx)
    county_path = _resolve_path(county_text, ctx)
    output_path = _resolve_path(output_text, ctx) if output_text else ctx.temp_path("county_aggregate.csv")

    if not raster_path.exists():
        raise FileNotFoundError(f"raster_path not found: {raster_path}")
    if not county_path.exists():
        raise FileNotFoundError(f"county_path not found: {county_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    agg_list = _parse_aggs(str(args.get("aggregations") or "sum,mean,count"))
    day = _resolve_day(args, raster_path)

    counties = gpd.read_file(county_path)
    if counties.empty:
        raise ValueError("county_path contains no features")
    if county_id_field not in counties.columns:
        raise ValueError(f"county_id_field '{county_id_field}' not found in county_path columns")
    if county_name_field and county_name_field not in counties.columns:
        raise ValueError(f"county_name_field '{county_name_field}' not found in county_path columns")

    counties = counties.copy()
    counties = counties[counties.geometry.notna() & ~counties.geometry.is_empty].copy()

    with rasterio.open(raster_path) as ds:
        if ds.crs is None:
            raise ValueError("raster_path CRS is required")
        if band < 1 or band > int(ds.count):
            raise ValueError(f"band must be in 1..{ds.count}; got {band}")

        counties = counties.to_crs(ds.crs)
        bounds_geom = box(float(ds.bounds.left), float(ds.bounds.bottom), float(ds.bounds.right), float(ds.bounds.top))
        intersect_mask = counties.geometry.intersects(bounds_geom)
        intersecting = counties.loc[intersect_mask].copy()
        non_intersecting = counties.loc[~intersect_mask].copy()

        rows: list[dict[str, Any]] = []
        agg_columns = [f"{value_prefix}_{agg}" for agg in agg_list]

        for _, row in non_intersecting.iterrows():
            item: dict[str, Any] = {
                "county_id": row[county_id_field],
                "raster_path": raster_path.resolve().as_posix(),
            }
            if county_name_field:
                item["county_name"] = row[county_name_field]
            if day:
                item["day"] = day
            for agg in agg_list:
                value = 0 if agg == "count" else None
                item[f"{value_prefix}_{agg}"] = value
            rows.append(item)

        for _, row in intersecting.iterrows():
            geom = row.geometry
            item = {
                "county_id": row[county_id_field],
                "raster_path": raster_path.resolve().as_posix(),
            }
            if county_name_field:
                item["county_name"] = row[county_name_field]
            if day:
                item["day"] = day

            try:
                masked, _ = mask(
                    ds,
                    [geom],
                    crop=True,
                    indexes=band,
                    filled=False,
                    all_touched=all_touched,
                )
                band_values = masked.compressed()
                agg_values = _compute_aggs(band_values, agg_list)
            except ValueError:
                agg_values = _empty_agg_values(agg_list)

            for agg in agg_list:
                item[f"{value_prefix}_{agg}"] = agg_values.get(agg)
            rows.append(item)

            if verbose:
                ctx.log(
                    f"[geo_county_raster_aggregate] county={item['county_id']} count={item.get(f'{value_prefix}_count', '')}"
                )

    rows.sort(key=lambda r: str(r.get("county_id") or ""))

    base_cols = ["county_id"]
    if county_name_field:
        base_cols.append("county_name")
    if day:
        base_cols.append("day")
    fieldnames = base_cols + agg_columns + ["raster_path"]

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    ctx.log(
        f"[geo_county_raster_aggregate] raster={raster_path.as_posix()} counties={len(counties)} "
        f"intersecting={len(intersecting)} output={output_path.as_posix()}"
    )

    return {
        "input_raster_path": raster_path.resolve().as_posix(),
        "county_path": county_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "row_count": int(len(rows)),
        "county_count": int(len(counties)),
        "intersecting_county_count": int(len(intersecting)),
        "non_intersecting_county_count": int(len(non_intersecting)),
        "aggregations": list(agg_list),
        "day": day,
        "_artifacts": [
            {
                "uri": output_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": True,
            }
        ],
    }


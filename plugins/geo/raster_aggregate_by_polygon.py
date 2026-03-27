# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

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

import yaml


meta = {
    "name": "raster_aggregate_by_polygon",
    "version": "0.2.0",
    "description": "Aggregate raster values by polygon into a tabular output.",
    "inputs": [],
    "outputs": [
        "input_raster_path",
        "polygon_path",
        "output_path",
        "row_count",
        "polygon_count",
        "intersecting_polygon_count",
        "non_intersecting_polygon_count",
        "aggregations",
        "day",
    ],
    "params": {
        "raster_path": {"type": "str", "default": ""},
        "polygon_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "polygon_id_field": {"type": "str", "default": "GEOID"},
        "polygon_name_field": {"type": "str", "default": ""},
        "aggregations": {"type": "str", "default": "sum,mean,count"},
        "value_prefix": {"type": "str", "default": "value"},
        "columns": {"type": "str", "default": ""},
        "band": {"type": "int", "default": 1},
        "all_touched": {"type": "bool", "default": False},
        "include_empty_polygons": {"type": "bool", "default": True},
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

_ALLOWED_TYPES = {"", "str", "string", "int", "integer", "float", "double", "bool", "boolean"}


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
        values[agg] = 0 if agg == "count" else None
    return values


def _sanitize_values(values: Any) -> Any:
    arr = np.asarray(values)
    if arr.size == 0:
        return arr
    if np.issubdtype(arr.dtype, np.number):
        return arr[np.isfinite(arr)]
    cleaned: list[Any] = []
    for value in arr.tolist():
        try:
            number = float(value)
        except Exception:  # noqa: BLE001
            cleaned.append(value)
            continue
        if np.isfinite(number):
            cleaned.append(value)
    return np.asarray(cleaned)


def _compute_aggs(values: Any, agg_list: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    arr = _sanitize_values(values)
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


def _normalize_bool(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Cannot coerce value to bool: {value!r}")


def _coerce_output_value(value: Any, type_name: str) -> Any:
    kind = str(type_name or "").strip().lower()
    if value is None or kind == "":
        return value
    if kind in {"str", "string"}:
        return str(value)
    if kind in {"int", "integer"}:
        if str(value).strip() == "":
            return None
        return int(float(value))
    if kind in {"float", "double"}:
        if str(value).strip() == "":
            return None
        return float(value)
    if kind in {"bool", "boolean"}:
        return _normalize_bool(value)
    raise ValueError(f"Unsupported output column type '{type_name}'")


def _default_columns(args: dict[str, Any], agg_list: list[str], day: str) -> list[dict[str, str]]:
    value_prefix = str(args.get("value_prefix") or "value").strip() or "value"
    columns: list[dict[str, str]] = [{"source": "polygon.id", "name": "polygon_id"}]
    polygon_name_field = str(args.get("polygon_name_field") or "").strip()
    if polygon_name_field:
        columns.append({"source": "polygon.name", "name": "polygon_name"})
    if day:
        columns.append({"source": "context.day", "name": "day"})
    for agg in agg_list:
        columns.append({"source": f"raster.{agg}", "name": f"{value_prefix}_{agg}"})
    columns.append({"source": "context.raster_path", "name": "raster_path"})
    return columns


def _parse_columns_spec(raw: Any, args: dict[str, Any], agg_list: list[str], day: str) -> list[dict[str, str]]:
    if raw in {None, ""}:
        parsed = _default_columns(args, agg_list, day)
    elif isinstance(raw, list):
        parsed = raw
    else:
        try:
            parsed = yaml.safe_load(str(raw))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"columns could not be parsed as YAML/JSON: {exc}") from exc
    if not isinstance(parsed, list) or not parsed:
        raise ValueError("columns must resolve to a non-empty list")

    columns: list[dict[str, str]] = []
    seen_names: set[str] = set()
    for idx, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError(f"columns[{idx}] must be a mapping")
        source = str(item.get("source") or "").strip()
        name = str(item.get("name") or "").strip()
        type_name = str(item.get("type") or "").strip().lower()
        raw_value = item.get("value")
        if not source:
            raise ValueError(f"columns[{idx}].source is required")
        if not name:
            raise ValueError(f"columns[{idx}].name is required")
        if type_name not in _ALLOWED_TYPES:
            allowed = ", ".join(sorted(t for t in _ALLOWED_TYPES if t))
            raise ValueError(f"columns[{idx}].type '{type_name}' is unsupported. Allowed: {allowed}")
        if name in seen_names:
            raise ValueError(f"Duplicate output column name '{name}'")
        seen_names.add(name)
        column = {"source": source, "name": name, "type": type_name}
        if source.lower() == "literal":
            if raw_value is None:
                raise ValueError(f"columns[{idx}].value is required when source=literal")
            column["value"] = raw_value
        columns.append(column)
    return columns


def _resolve_agg_list(args: dict[str, Any], columns: list[dict[str, str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for column in columns:
        source = str(column.get("source") or "").strip().lower()
        if not source.startswith("raster."):
            continue
        agg = source.split(".", 1)[1]
        if agg not in _ALLOWED_AGGS:
            allowed = ", ".join(sorted(_ALLOWED_AGGS))
            raise ValueError(f"Unsupported raster column source '{source}'. Allowed raster.* sources: {allowed}")
        if agg not in seen:
            seen.add(agg)
            out.append(agg)
    if out:
        return out
    return _parse_aggs(str(args.get("aggregations") or "sum,mean,count"))


def _resolve_column_value(
    *,
    column: dict[str, str],
    polygon_row: Any,
    agg_values: dict[str, Any],
    polygon_id_field: str,
    polygon_name_field: str,
    day: str,
    raster_path: Path,
) -> Any:
    source = str(column.get("source") or "").strip()
    source_lower = source.lower()
    if source_lower == "polygon.id":
        return polygon_row[polygon_id_field]
    if source_lower == "polygon.name":
        if not polygon_name_field:
            return None
        return polygon_row[polygon_name_field]
    if source_lower.startswith("polygon."):
        field_name = source.split(".", 1)[1]
        return polygon_row[field_name]
    if source_lower.startswith("raster."):
        agg = source_lower.split(".", 1)[1]
        return agg_values.get(agg)
    if source_lower == "literal":
        return column.get("value")
    if source_lower == "context.day":
        return day
    if source_lower == "context.raster_path":
        return raster_path.resolve().as_posix()
    raise ValueError(f"Unsupported column source '{source}'")


def _build_output_row(
    *,
    columns: list[dict[str, str]],
    polygon_row: Any,
    agg_values: dict[str, Any],
    polygon_id_field: str,
    polygon_name_field: str,
    day: str,
    raster_path: Path,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for column in columns:
        value = _resolve_column_value(
            column=column,
            polygon_row=polygon_row,
            agg_values=agg_values,
            polygon_id_field=polygon_id_field,
            polygon_name_field=polygon_name_field,
            day=day,
            raster_path=raster_path,
        )
        row[str(column["name"])] = _coerce_output_value(value, str(column.get("type") or ""))
    return row


def run(args, ctx):
    if gpd is None or box is None:
        raise RuntimeError(
            "raster_aggregate_by_polygon requires geopandas and shapely. Install requirements.txt in the active environment."
        )
    if rasterio is None or mask is None or np is None:
        raise RuntimeError(
            "raster_aggregate_by_polygon requires rasterio and numpy. Install requirements.txt in the active environment."
        )

    raster_text = str(args.get("raster_path") or "").strip()
    polygon_text = str(args.get("polygon_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    polygon_id_field = str(args.get("polygon_id_field") or "GEOID").strip()
    polygon_name_field = str(args.get("polygon_name_field") or "").strip()
    band = int(args.get("band", 1) or 1)
    all_touched = bool(args.get("all_touched", False))
    include_empty_polygons = bool(args.get("include_empty_polygons", True))
    verbose = bool(args.get("verbose", False))

    if not raster_text:
        raise ValueError("raster_path is required")
    if not polygon_text:
        raise ValueError("polygon_path is required")

    raster_path = _resolve_path(raster_text, ctx)
    polygon_path = _resolve_path(polygon_text, ctx)
    output_path = _resolve_path(output_text, ctx) if output_text else ctx.temp_path("polygon_aggregate.csv")

    if not raster_path.exists():
        raise FileNotFoundError(f"raster_path not found: {raster_path}")
    if not polygon_path.exists():
        raise FileNotFoundError(f"polygon_path not found: {polygon_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    day = _resolve_day(args, raster_path)
    columns = _parse_columns_spec(args.get("columns"), args, _parse_aggs(str(args.get("aggregations") or "sum,mean,count")), day)
    agg_list = _resolve_agg_list(args, columns)

    polygons = gpd.read_file(polygon_path)
    if polygons.empty:
        raise ValueError("polygon_path contains no features")
    if polygon_id_field not in polygons.columns:
        raise ValueError(f"polygon_id_field '{polygon_id_field}' not found in polygon_path columns")
    if polygon_name_field and polygon_name_field not in polygons.columns:
        raise ValueError(f"polygon_name_field '{polygon_name_field}' not found in polygon_path columns")
    for column in columns:
        source = str(column.get("source") or "").strip()
        if source.lower().startswith("polygon.") and source.lower() not in {"polygon.id", "polygon.name"}:
            field_name = source.split(".", 1)[1]
            if field_name not in polygons.columns:
                raise ValueError(f"polygon field '{field_name}' from columns was not found in polygon_path columns")

    polygons = polygons.copy()
    polygons = polygons[polygons.geometry.notna() & ~polygons.geometry.is_empty].copy()

    with rasterio.open(raster_path) as ds:
        if ds.crs is None:
            raise ValueError("raster_path CRS is required")
        if band < 1 or band > int(ds.count):
            raise ValueError(f"band must be in 1..{ds.count}; got {band}")

        polygons = polygons.to_crs(ds.crs)
        bounds_geom = box(float(ds.bounds.left), float(ds.bounds.bottom), float(ds.bounds.right), float(ds.bounds.top))
        intersect_mask = polygons.geometry.intersects(bounds_geom)
        intersecting = polygons.loc[intersect_mask].copy()
        non_intersecting = polygons.loc[~intersect_mask].copy()

        rows: list[dict[str, Any]] = []

        if include_empty_polygons:
            for _, polygon_row in non_intersecting.iterrows():
                agg_values = _empty_agg_values(agg_list)
                rows.append(
                    _build_output_row(
                        columns=columns,
                        polygon_row=polygon_row,
                        agg_values=agg_values,
                        polygon_id_field=polygon_id_field,
                        polygon_name_field=polygon_name_field,
                        day=day,
                        raster_path=raster_path,
                    )
                )

        for _, polygon_row in intersecting.iterrows():
            try:
                masked, _ = mask(
                    ds,
                    [polygon_row.geometry],
                    crop=True,
                    indexes=band,
                    filled=False,
                    all_touched=all_touched,
                )
                agg_values = _compute_aggs(masked.compressed(), agg_list)
            except ValueError:
                agg_values = _empty_agg_values(agg_list)

            if not include_empty_polygons and int(agg_values.get("count", 0) or 0) == 0:
                continue

            rows.append(
                _build_output_row(
                    columns=columns,
                    polygon_row=polygon_row,
                    agg_values=agg_values,
                    polygon_id_field=polygon_id_field,
                    polygon_name_field=polygon_name_field,
                    day=day,
                    raster_path=raster_path,
                )
            )

            if verbose:
                ctx.log(
                    f"[raster_aggregate_by_polygon] polygon={polygon_row[polygon_id_field]} "
                    f"count={agg_values.get('count', '')}"
                )

    fieldnames = [str(column["name"]) for column in columns]
    sort_key = next((name for name in ["polygon_id", "county_id", "tile_field_id"] if name in fieldnames), fieldnames[0])
    rows.sort(key=lambda row: str(row.get(sort_key) or ""))

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    ctx.log(
        f"[raster_aggregate_by_polygon] raster={raster_path.as_posix()} polygons={len(polygons)} "
        f"intersecting={len(intersecting)} output={output_path.as_posix()}"
    )

    return {
        "input_raster_path": raster_path.resolve().as_posix(),
        "polygon_path": polygon_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "row_count": int(len(rows)),
        "polygon_count": int(len(polygons)),
        "intersecting_polygon_count": int(len(intersecting)),
        "non_intersecting_polygon_count": int(len(non_intersecting)),
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

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import rasterio  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None


meta = {
    "name": "raster_facts",
    "version": "0.1.0",
    "description": "Build a file manifest and compute raster facts/stats for rasters under a directory tree.",
    "inputs": [],
    "outputs": [
        "input_dir",
        "output_dir",
        "manifest_csv",
        "manifest_json",
        "raster_facts_csv",
        "raster_facts_json",
        "file_count",
        "raster_file_count",
        "band_fact_count",
    ],
    "params": {
        "input_dir": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ".runs/raster_facts"},
        "raster_extensions": {"type": "str", "default": ".tif,.tiff,.img,.vrt,.asc,.bil,.jp2,"},
        "probe_unknown": {"type": "bool", "default": True},
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


def _to_utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_extensions(raw: str) -> set[str]:
    out: set[str] = set()
    for part in str(raw or "").replace(";", ",").split(","):
        token = str(part or "").strip().lower()
        if not token:
            continue
        if not token.startswith("."):
            token = "." + token
        out.add(token)
    return out


def _to_list(value: Any) -> Any:
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def _iter_data_mask(data: Any, mask: Any) -> Iterable[Tuple[Any, bool]]:
    data_val = _to_list(data)
    mask_val = _to_list(mask)
    if isinstance(data_val, list):
        if isinstance(mask_val, list):
            for dv, mv in zip(data_val, mask_val):
                yield from _iter_data_mask(dv, mv)
            return
        for dv in data_val:
            yield from _iter_data_mask(dv, mask_val)
        return
    yield data_val, bool(mask_val)


def _crs_text(crs: Any) -> str:
    if crs is None:
        return ""
    try:
        if hasattr(crs, "to_string"):
            return str(crs.to_string() or "")
    except Exception:  # noqa: BLE001
        pass
    return str(crs or "")


def _band_stats(ds, band_index: int) -> Dict[str, Any]:
    nodata_values = list(getattr(ds, "nodatavals", []) or [])
    nodata_value = nodata_values[band_index - 1] if band_index - 1 < len(nodata_values) else None
    dtype = ""
    dtypes = list(getattr(ds, "dtypes", []) or [])
    if band_index - 1 < len(dtypes):
        dtype = str(dtypes[band_index - 1] or "")

    total_count = 0
    valid_count = 0
    nodata_count = 0
    zero_count = 0
    nan_count = 0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    value_sum = 0.0
    value_sum_sq = 0.0

    windows = []
    try:
        windows = [w for _, w in ds.block_windows(band_index)]
    except Exception:  # noqa: BLE001
        windows = [None]
    if not windows:
        windows = [None]

    for win in windows:
        chunk = ds.read(band_index, window=win, masked=True)
        data = getattr(chunk, "data", chunk)
        mask = getattr(chunk, "mask", False)
        for value, is_masked in _iter_data_mask(data, mask):
            total_count += 1
            if is_masked:
                nodata_count += 1
                continue
            if value is None:
                nodata_count += 1
                continue
            try:
                num = float(value)
            except Exception:  # noqa: BLE001
                continue
            if math.isnan(num):
                nan_count += 1
                continue
            valid_count += 1
            if num == 0.0:
                zero_count += 1
            value_sum += num
            value_sum_sq += num * num
            if min_value is None or num < min_value:
                min_value = num
            if max_value is None or num > max_value:
                max_value = num

    mean_value: Optional[float] = None
    stddev_value: Optional[float] = None
    value_range: Optional[float] = None
    if valid_count > 0:
        mean_value = value_sum / float(valid_count)
        variance = max(0.0, (value_sum_sq / float(valid_count)) - (mean_value * mean_value))
        stddev_value = math.sqrt(variance)
    if min_value is not None and max_value is not None:
        value_range = max_value - min_value

    return {
        "band_index": band_index,
        "pixel_type": dtype,
        "nodata_value": "" if nodata_value is None else str(nodata_value),
        "total_count": total_count,
        "valid_count": valid_count,
        "nodata_count": nodata_count,
        "zero_count": zero_count,
        "nan_count": nan_count,
        "min_value": min_value,
        "max_value": max_value,
        "value_range": value_range,
        "mean_value": mean_value,
        "stddev_value": stddev_value,
    }


def run(args, ctx):
    input_dir_text = str(args.get("input_dir") or "").strip()
    if not input_dir_text:
        raise ValueError("input_dir is required")
    input_dir = _resolve_path(input_dir_text, ctx)
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input_dir not found: {input_dir}")

    output_dir = _resolve_path(str(args.get("output_dir") or ".runs/raster_facts"), ctx)
    output_dir.mkdir(parents=True, exist_ok=True)

    ext_set = _parse_extensions(str(args.get("raster_extensions") or ""))
    probe_unknown = bool(args.get("probe_unknown", True))
    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[raster_facts] start input={input_dir.resolve().as_posix()} output={output_dir.resolve().as_posix()} "
        f"probe_unknown={probe_unknown}"
    )

    file_rows: List[Dict[str, Any]] = []
    raster_rows: List[Dict[str, Any]] = []
    raster_files: set[str] = set()

    files = sorted(p for p in input_dir.rglob("*") if p.is_file())
    for path in files:
        rel = path.relative_to(input_dir).as_posix()
        st = path.stat()
        suffix = path.suffix.lower()
        candidate = suffix in ext_set or (probe_unknown and suffix == "")
        file_rows.append(
            {
                "relative_path": rel,
                "file_name": path.name,
                "extension": suffix,
                "size_bytes": int(st.st_size),
                "created_utc": _to_utc_iso(st.st_ctime),
                "modified_utc": _to_utc_iso(st.st_mtime),
                "raster_candidate": int(candidate),
            }
        )

        if not candidate:
            continue
        if rasterio is None:
            continue

        try:
            with rasterio.open(path) as ds:
                crs = _crs_text(getattr(ds, "crs", None))
                bounds_obj = getattr(ds, "bounds", None)
                bounds = ""
                if bounds_obj is not None:
                    try:
                        bounds = ",".join(
                            [
                                str(float(bounds_obj.left)),
                                str(float(bounds_obj.bottom)),
                                str(float(bounds_obj.right)),
                                str(float(bounds_obj.top)),
                            ]
                        )
                    except Exception:  # noqa: BLE001
                        bounds = str(bounds_obj)
                for band_index in range(1, int(getattr(ds, "count", 0)) + 1):
                    stats = _band_stats(ds, band_index)
                    raster_rows.append(
                        {
                            "relative_path": rel,
                            "band_index": int(stats["band_index"]),
                            "pixel_type": str(stats["pixel_type"] or ""),
                            "width": int(getattr(ds, "width", 0)),
                            "height": int(getattr(ds, "height", 0)),
                            "band_count": int(getattr(ds, "count", 0)),
                            "crs": crs,
                            "bounds": bounds,
                            "nodata_value": str(stats["nodata_value"] or ""),
                            "total_count": int(stats["total_count"]),
                            "valid_count": int(stats["valid_count"]),
                            "nodata_count": int(stats["nodata_count"]),
                            "zero_count": int(stats["zero_count"]),
                            "nan_count": int(stats["nan_count"]),
                            "min_value": stats["min_value"],
                            "max_value": stats["max_value"],
                            "value_range": stats["value_range"],
                            "mean_value": stats["mean_value"],
                            "stddev_value": stats["stddev_value"],
                        }
                    )
                raster_files.add(rel)
        except Exception as exc:  # noqa: BLE001
            raster_rows.append(
                {
                    "relative_path": rel,
                    "band_index": 0,
                    "pixel_type": "",
                    "width": "",
                    "height": "",
                    "band_count": "",
                    "crs": "",
                    "bounds": "",
                    "nodata_value": "",
                    "total_count": "",
                    "valid_count": "",
                    "nodata_count": "",
                    "zero_count": "",
                    "nan_count": "",
                    "min_value": "",
                    "max_value": "",
                    "value_range": "",
                    "mean_value": "",
                    "stddev_value": "",
                    "error": str(exc),
                }
            )

    manifest_csv = output_dir / "file_manifest.csv"
    with manifest_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "relative_path",
                "file_name",
                "extension",
                "size_bytes",
                "created_utc",
                "modified_utc",
                "raster_candidate",
            ],
        )
        writer.writeheader()
        writer.writerows(file_rows)

    manifest_json = output_dir / "file_manifest.json"
    manifest_json.write_text(json.dumps(file_rows, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    raster_csv = output_dir / "raster_facts.csv"
    with raster_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "relative_path",
                "band_index",
                "pixel_type",
                "width",
                "height",
                "band_count",
                "crs",
                "bounds",
                "nodata_value",
                "total_count",
                "valid_count",
                "nodata_count",
                "zero_count",
                "nan_count",
                "min_value",
                "max_value",
                "value_range",
                "mean_value",
                "stddev_value",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(raster_rows)

    raster_json = output_dir / "raster_facts.json"
    raster_json.write_text(json.dumps(raster_rows, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    if rasterio is None:
        ctx.log("[raster_facts] rasterio not installed; manifest was created but raster stats were skipped", "WARN")
    ctx.log(
        f"[raster_facts] files={len(file_rows)} raster_files={len(raster_files)} "
        f"band_rows={len(raster_rows)} output={output_dir.as_posix()}"
    )
    if verbose and raster_rows:
        ctx.log(f"[raster_facts] band_preview={raster_rows[:3]}")
    return {
        "input_dir": input_dir.resolve().as_posix(),
        "output_dir": output_dir.resolve().as_posix(),
        "manifest_csv": manifest_csv.resolve().as_posix(),
        "manifest_json": manifest_json.resolve().as_posix(),
        "raster_facts_csv": raster_csv.resolve().as_posix(),
        "raster_facts_json": raster_json.resolve().as_posix(),
        "file_count": len(file_rows),
        "raster_file_count": len(raster_files),
        "band_fact_count": len(raster_rows),
    }

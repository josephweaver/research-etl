from __future__ import annotations

import ast
import csv
import re
from collections import deque
from datetime import datetime, timedelta
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
    "name": "geo_raster_running_window",
    "version": "0.1.0",
    "description": "Compute trailing running-window raster metrics from a dated raster series and write GeoTIFF outputs.",
    "inputs": [],
    "outputs": [
        "input_count",
        "output_dir",
        "manifest_path",
        "generated_count",
        "skipped_count",
        "metric",
        "windows",
    ],
    "params": {
        "input_glob": {"type": "str", "default": ""},
        "input_paths": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ""},
        "metric": {"type": "str", "default": "sum"},
        "windows": {"type": "str", "default": "3,7,14,30"},
        "band": {"type": "int", "default": 1},
        "day_from_filename_regex": {"type": "str", "default": "(\\d{8})"},
        "day_from_filename_group": {"type": "int", "default": 1},
        "target_year": {"type": "int", "default": 0},
        "input_start_day": {"type": "str", "default": ""},
        "input_end_day": {"type": "str", "default": ""},
        "output_start_day": {"type": "str", "default": ""},
        "output_end_day": {"type": "str", "default": ""},
        "overwrite": {"type": "bool", "default": False},
        "compress": {"type": "str", "default": "LZW"},
    },
    "idempotent": True,
}

_ALLOWED_METRICS = {"sum"}


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


def _parse_windows(raw: Any) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for token in _coerce_list(raw):
        try:
            value = int(token)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"Invalid window size '{token}'") from exc
        if value <= 0:
            raise ValueError(f"Window size must be positive; got {value}")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    if not out:
        raise ValueError("windows must include at least one positive integer")
    return sorted(out)


def _parse_metric(raw: Any) -> str:
    metric = str(raw or "sum").strip().lower()
    if not metric:
        metric = "sum"
    if metric not in _ALLOWED_METRICS:
        allowed = ", ".join(sorted(_ALLOWED_METRICS))
        raise ValueError(f"Unsupported metric '{metric}'. Allowed: {allowed}")
    return metric


def _collect_input_paths(args, ctx) -> list[Path]:
    glob_text = str(args.get("input_glob") or "").strip()
    listed = [_resolve_path(item, ctx) for item in _coerce_list(args.get("input_paths"))]
    if glob_text:
        glob_path = _resolve_path(glob_text, ctx)
        if any(ch in str(glob_path) for ch in "*?["):
            matches = [Path(p) for p in sorted(glob_path.parent.glob(glob_path.name))]
        else:
            matches = [Path(p) for p in sorted(Path(".").glob(str(glob_path)))]
        listed.extend(matches)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in listed:
        resolved = path.resolve()
        key = resolved.as_posix()
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    if not unique:
        raise ValueError("one of input_glob or input_paths must resolve to at least one raster")
    return unique


def _extract_day(path: Path, pattern: str, group_idx: int) -> str:
    if not pattern:
        return path.name
    match = re.search(pattern, path.name)
    if not match:
        raise ValueError(f"Could not extract day from filename: {path.name}")
    try:
        value = str(match.group(group_idx) or "").strip()
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid day_from_filename_group={group_idx} for filename: {path.name}") from exc
    if not value:
        raise ValueError(f"Extracted empty day from filename: {path.name}")
    return value


def _day_in_output_range(day: str, *, start_day: str, end_day: str) -> bool:
    if start_day and day < start_day:
        return False
    if end_day and day > end_day:
        return False
    return True


def _derive_day_windows(
    *,
    target_year: int,
    windows: list[int],
    input_start_day: str,
    input_end_day: str,
    output_start_day: str,
    output_end_day: str,
) -> tuple[str, str, str, str]:
    if target_year <= 0:
        return input_start_day, input_end_day, output_start_day, output_end_day
    year_start = datetime(int(target_year), 1, 1)
    year_end = datetime(int(target_year), 12, 31)
    if not output_start_day:
        output_start_day = year_start.strftime("%Y%m%d")
    if not output_end_day:
        output_end_day = year_end.strftime("%Y%m%d")
    if not input_start_day:
        lookback_days = max(0, max(windows or [1]) - 1)
        input_start_day = (year_start - timedelta(days=lookback_days)).strftime("%Y%m%d")
    if not input_end_day:
        input_end_day = output_end_day
    return input_start_day, input_end_day, output_start_day, output_end_day


def _read_masked_raster(path: Path, *, band: int):
    with rasterio.open(path) as ds:
        if band < 1 or band > int(ds.count):
            raise ValueError(f"band must be in 1..{ds.count}; got {band}")
        arr = ds.read(band, masked=True).astype("float32")
        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=1, dtype="float32")
        nodata = ds.nodata if ds.nodata is not None else -9999.0
        signature = {
            "width": int(ds.width),
            "height": int(ds.height),
            "crs": str(ds.crs or ""),
            "transform": tuple(ds.transform),
        }
    return arr, profile, float(nodata), signature


def _ensure_same_grid(items: list[dict[str, Any]]) -> None:
    if not items:
        return
    expected = items[0]["signature"]
    expected_path = items[0]["path"]
    for item in items[1:]:
        if item["signature"] != expected:
            raise ValueError(
                f"Input rasters do not share the same grid: {item['path'].name} differs from {expected_path.name}"
            )


def _metric_sum(arrays) -> Any:
    return np.ma.sum(np.ma.stack(list(arrays)), axis=0)


def _compute_metric(metric: str, arrays) -> Any:
    # Add new metrics here as needed. Keep the dispatch isolated so the
    # rolling-window orchestration stays unchanged when new math is added.
    if metric == "sum":
        return _metric_sum(arrays)
    raise ValueError(f"Unsupported metric '{metric}'")


def _write_output(path: Path, *, data, profile: dict[str, Any], nodata: float, compress: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out_profile = profile.copy()
    out_profile.update(nodata=float(nodata), compress=compress)
    masked = np.ma.asarray(data)
    filled = masked.filled(float(nodata)).astype("float32")
    with rasterio.open(path, "w", **out_profile) as dst:
        dst.write(filled, 1)


def run(args, ctx):
    if rasterio is None or np is None:
        raise RuntimeError(
            "geo_raster_running_window requires rasterio and numpy. Install requirements.txt in the active environment."
        )

    output_text = str(args.get("output_dir") or "").strip()
    if not output_text:
        raise ValueError("output_dir is required")

    metric = _parse_metric(args.get("metric"))
    windows = _parse_windows(args.get("windows"))
    band = int(args.get("band", 1) or 1)
    overwrite = bool(args.get("overwrite", False))
    compress = str(args.get("compress") or "LZW").strip() or "LZW"
    day_pattern = str(args.get("day_from_filename_regex") or "(\\d{8})").strip()
    day_group = int(args.get("day_from_filename_group", 1) or 1)
    target_year = int(args.get("target_year") or 0)
    input_start_day = str(args.get("input_start_day") or "").strip()
    input_end_day = str(args.get("input_end_day") or "").strip()
    output_start_day = str(args.get("output_start_day") or "").strip()
    output_end_day = str(args.get("output_end_day") or "").strip()

    input_start_day, input_end_day, output_start_day, output_end_day = _derive_day_windows(
        target_year=target_year,
        windows=windows,
        input_start_day=input_start_day,
        input_end_day=input_end_day,
        output_start_day=output_start_day,
        output_end_day=output_end_day,
    )

    if input_start_day and input_end_day and input_start_day > input_end_day:
        raise ValueError("input_start_day must be <= input_end_day")
    if output_start_day and output_end_day and output_start_day > output_end_day:
        raise ValueError("output_start_day must be <= output_end_day")

    input_paths = _collect_input_paths(args, ctx)
    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"input raster not found: {path}")

    output_dir = _resolve_path(output_text, ctx)
    output_dir.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, Any]] = []
    seen_days: set[str] = set()
    for path in input_paths:
        day = _extract_day(path, day_pattern, day_group)
        if not _day_in_output_range(day, start_day=input_start_day, end_day=input_end_day):
            continue
        if day in seen_days:
            raise ValueError(f"Duplicate day '{day}' detected in raster series")
        seen_days.add(day)
        arr, profile, nodata, signature = _read_masked_raster(path, band=band)
        items.append(
            {
                "day": day,
                "path": path,
                "array": arr,
                "profile": profile,
                "nodata": nodata,
                "signature": signature,
            }
        )
    if not items:
        raise ValueError("no rasters remained after applying input day filters")
    items.sort(key=lambda item: item["day"])
    _ensure_same_grid(items)

    buffer = deque(maxlen=max(windows))
    generated_count = 0
    skipped_count = 0
    manifest_path = output_dir / "manifest.csv"
    manifest_rows: list[dict[str, Any]] = []

    for item in items:
        buffer.append(item)
        for window in windows:
            if len(buffer) < window:
                continue
            trailing = list(buffer)[-window:]
            source_item = item
            if not _day_in_output_range(
                source_item["day"],
                start_day=output_start_day,
                end_day=output_end_day,
            ):
                continue
            output_path = output_dir / f"{metric}_{window:02d}d" / source_item["path"].name
            if output_path.exists() and not overwrite:
                skipped_count += 1
            else:
                result = _compute_metric(metric, [entry["array"] for entry in trailing])
                _write_output(
                    output_path,
                    data=result,
                    profile=source_item["profile"],
                    nodata=source_item["nodata"],
                    compress=compress,
                )
                generated_count += 1
            manifest_rows.append(
                {
                    "day": source_item["day"],
                    "window_days": window,
                    "metric": metric,
                    "input_count": window,
                    "output_path": output_path.resolve().as_posix(),
                }
            )

    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["day", "window_days", "metric", "input_count", "output_path"])
        writer.writeheader()
        writer.writerows(manifest_rows)

    ctx.log(
        f"[geo_raster_running_window] inputs={len(items)} metric={metric} windows={windows} "
        f"generated={generated_count} skipped={skipped_count} output_dir={output_dir.as_posix()}"
    )

    return {
        "input_count": int(len(items)),
        "output_dir": output_dir.resolve().as_posix(),
        "manifest_path": manifest_path.resolve().as_posix(),
        "generated_count": int(generated_count),
        "skipped_count": int(skipped_count),
        "metric": metric,
        "windows": list(windows),
        "_artifacts": [
            {
                "uri": manifest_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": True,
            }
        ],
    }

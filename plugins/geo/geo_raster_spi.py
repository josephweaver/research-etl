from __future__ import annotations

import ast
import csv
import glob
import re
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
    "name": "geo_raster_spi",
    "version": "0.1.0",
    "description": "Compute raster SPI-like standardized anomalies from an accumulated raster series.",
    "inputs": [],
    "outputs": [
        "input_count",
        "output_dir",
        "manifest_path",
        "generated_count",
        "skipped_count",
        "method",
        "mean_raster_path",
        "std_raster_path",
    ],
    "params": {
        "input_glob": {"type": "str", "default": ""},
        "input_paths": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ""},
        "method": {"type": "str", "default": "zscore"},
        "band": {"type": "int", "default": 1},
        "day_from_filename_regex": {"type": "str", "default": "(\\d{8})"},
        "day_from_filename_group": {"type": "int", "default": 1},
        "min_std": {"type": "float", "default": 1e-6},
        "overwrite": {"type": "bool", "default": False},
        "compress": {"type": "str", "default": "LZW"},
    },
    "idempotent": True,
}

_ALLOWED_METHODS = {"zscore"}


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


def _collect_input_paths(args, ctx) -> list[Path]:
    glob_text = str(args.get("input_glob") or "").strip()
    listed = [_resolve_path(item, ctx) for item in _coerce_list(args.get("input_paths"))]
    if glob_text:
        glob_path = _resolve_path(glob_text, ctx)
        if any(ch in str(glob_path) for ch in "*?["):
            matches = [Path(p) for p in sorted(glob.glob(str(glob_path), recursive=True))]
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


def _parse_method(raw: Any) -> str:
    method = str(raw or "zscore").strip().lower()
    if method not in _ALLOWED_METHODS:
        allowed = ", ".join(sorted(_ALLOWED_METHODS))
        raise ValueError(f"Unsupported method '{method}'. Allowed: {allowed}")
    return method


def _read_masked_raster(path: Path, *, band: int):
    with rasterio.open(path) as ds:
        if band < 1 or band > int(ds.count):
            raise ValueError(f"band must be in 1..{ds.count}; got {band}")
        arr = ds.read(band, masked=True).astype("float64")
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


def _read_raster_metadata(path: Path, *, band: int) -> dict[str, Any]:
    with rasterio.open(path) as ds:
        if band < 1 or band > int(ds.count):
            raise ValueError(f"band must be in 1..{ds.count}; got {band}")
        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=1, dtype="float32")
        nodata = ds.nodata if ds.nodata is not None else -9999.0
        return {
            "profile": profile,
            "nodata": float(nodata),
            "signature": {
                "width": int(ds.width),
                "height": int(ds.height),
                "crs": str(ds.crs or ""),
                "transform": tuple(ds.transform),
            },
        }


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


def _write_raster(path: Path, *, data: Any, profile: dict[str, Any], nodata: float, compress: str) -> None:
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
            "geo_raster_spi requires rasterio and numpy. Install requirements.txt in the active environment."
        )

    output_text = str(args.get("output_dir") or "").strip()
    if not output_text:
        raise ValueError("output_dir is required")

    method = _parse_method(args.get("method"))
    band = int(args.get("band", 1) or 1)
    min_std = float(args.get("min_std", 1e-6) or 1e-6)
    overwrite = bool(args.get("overwrite", False))
    compress = str(args.get("compress") or "LZW").strip() or "LZW"
    day_pattern = str(args.get("day_from_filename_regex") or "(\\d{8})").strip()
    day_group = int(args.get("day_from_filename_group", 1) or 1)

    input_paths = _collect_input_paths(args, ctx)
    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"input raster not found: {path}")

    items: list[dict[str, Any]] = []
    seen_days: set[str] = set()
    for path in input_paths:
        day = _extract_day(path, day_pattern, day_group)
        if day in seen_days:
            raise ValueError(f"Duplicate day '{day}' detected in raster series")
        seen_days.add(day)
        meta = _read_raster_metadata(path, band=band)
        items.append(
            {
                "day": day,
                "path": path,
                "profile": meta["profile"],
                "nodata": meta["nodata"],
                "signature": meta["signature"],
            }
        )
    items.sort(key=lambda item: item["day"])
    _ensure_same_grid(items)

    template = items[0]
    shape = (int(template["signature"]["height"]), int(template["signature"]["width"]))
    count = np.zeros(shape, dtype=np.int32)
    mean = np.zeros(shape, dtype=np.float64)
    m2 = np.zeros(shape, dtype=np.float64)

    for item in items:
        arr, _, _, _ = _read_masked_raster(item["path"], band=band)
        arr = np.ma.asarray(arr, dtype=np.float64)
        valid = ~np.ma.getmaskarray(arr)
        if not np.any(valid):
            continue
        values = arr.filled(0.0)
        prev_mean = mean[valid]
        prev_count = count[valid].astype(np.float64)
        next_count = prev_count + 1.0
        delta = values[valid] - prev_mean
        next_mean = prev_mean + (delta / next_count)
        delta2 = values[valid] - next_mean
        count[valid] += 1
        mean[valid] = next_mean
        m2[valid] += delta * delta2

    valid_stats = count > 1
    std = np.zeros(shape, dtype=np.float64)
    std[valid_stats] = np.sqrt(m2[valid_stats] / (count[valid_stats] - 1.0))
    usable_stats = valid_stats & (std > float(min_std))

    output_dir = _resolve_path(output_text, ctx)
    output_dir.mkdir(parents=True, exist_ok=True)
    stats_dir = output_dir / "_stats"
    mean_path = stats_dir / "mean.tif"
    std_path = stats_dir / "std.tif"
    nodata = float(template["nodata"])
    stats_mask = ~usable_stats
    mean_masked = np.ma.array(mean.astype("float32"), mask=stats_mask)
    std_masked = np.ma.array(std.astype("float32"), mask=stats_mask)
    if overwrite or not mean_path.exists():
        _write_raster(mean_path, data=mean_masked, profile=template["profile"], nodata=nodata, compress=compress)
    if overwrite or not std_path.exists():
        _write_raster(std_path, data=std_masked, profile=template["profile"], nodata=nodata, compress=compress)

    manifest_path = output_dir / "manifest.csv"
    manifest_rows: list[dict[str, Any]] = []
    generated_count = 0
    skipped_count = 0
    artifacts: list[dict[str, Any]] = [
        {
            "uri": mean_path.resolve().as_posix(),
            "class": "published",
            "location_type": "run_artifact",
            "canonical": False,
        },
        {
            "uri": std_path.resolve().as_posix(),
            "class": "published",
            "location_type": "run_artifact",
            "canonical": False,
        },
    ]

    for item in items:
        out_path = output_dir / item["day"][:4] / item["path"].name
        if out_path.exists() and not overwrite:
            skipped_count += 1
        else:
            arr, _, _, _ = _read_masked_raster(item["path"], band=band)
            arr = np.ma.asarray(arr, dtype=np.float64)
            arr_mask = np.ma.getmaskarray(arr)
            out_mask = arr_mask | ~usable_stats
            out = np.ma.masked_all(shape, dtype=np.float32)
            valid = ~out_mask
            if np.any(valid):
                if method == "zscore":
                    out.data[valid] = ((arr.data[valid] - mean[valid]) / std[valid]).astype(np.float32)
                else:
                    raise ValueError(f"Unsupported method '{method}'")
            out.mask = out_mask
            _write_raster(out_path, data=out, profile=item["profile"], nodata=nodata, compress=compress)
            generated_count += 1
        manifest_rows.append(
            {
                "day": item["day"],
                "method": method,
                "output_path": out_path.resolve().as_posix(),
            }
        )
        artifacts.append(
            {
                "uri": out_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": False,
            }
        )

    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["day", "method", "output_path"])
        writer.writeheader()
        writer.writerows(manifest_rows)

    ctx.log(
        f"[geo_raster_spi] inputs={len(items)} method={method} generated={generated_count} "
        f"skipped={skipped_count} output_dir={output_dir.as_posix()}"
    )

    return {
        "input_count": int(len(items)),
        "output_dir": output_dir.resolve().as_posix(),
        "manifest_path": manifest_path.resolve().as_posix(),
        "generated_count": int(generated_count),
        "skipped_count": int(skipped_count),
        "method": method,
        "mean_raster_path": mean_path.resolve().as_posix(),
        "std_raster_path": std_path.resolve().as_posix(),
        "_artifacts": artifacts
        + [
            {
                "uri": manifest_path.resolve().as_posix(),
                "class": "published",
                "location_type": "run_artifact",
                "canonical": True,
            }
        ],
    }

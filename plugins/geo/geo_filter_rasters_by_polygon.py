# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None

try:
    import rasterio  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None

try:
    from shapely.geometry import box  # type: ignore
except Exception:  # noqa: BLE001
    box = None


meta = {
    "name": "geo_filter_rasters_by_polygon",
    "version": "0.1.0",
    "description": "Select rasters whose header bounds intersect selector polygons. Uses raster header metadata only.",
    "inputs": [],
    "outputs": [
        "input_raster_dir",
        "selector_path",
        "copy_output_dir",
        "selected_rasters_csv",
        "selected_footprints_path",
        "copied_raster_count",
        "candidate_raster_count",
        "inspected_raster_count",
        "selected_raster_count",
        "skipped_no_crs_count",
        "error_raster_count",
        "error_samples",
    ],
    "params": {
        "raster_dir": {"type": "str", "default": ""},
        "selector_path": {"type": "str", "default": ""},
        "copy_output_dir": {"type": "str", "default": ""},
        # Deprecated alias retained for compatibility.
        "output_dir": {"type": "str", "default": ""},
        "copy_selected": {"type": "bool", "default": False},
        "selected_rasters_csv": {"type": "str", "default": ""},
        "selected_footprints_path": {"type": "str", "default": ""},
        "raster_extensions": {"type": "str", "default": ".tif,.tiff,.img,.vrt,.asc,.bil,.jp2"},
        "fail_on_missing_crs": {"type": "bool", "default": True},
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


def _crs_text(crs: Any) -> str:
    if crs is None:
        return ""
    try:
        if hasattr(crs, "to_string"):
            return str(crs.to_string() or "")
    except Exception:  # noqa: BLE001
        pass
    return str(crs or "")


def _copy_with_sidecars(src: Path, dst: Path) -> int:
    copied = 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists() and src.is_file():
        shutil.copy2(src, dst)
        copied += 1
    # Copy common sidecars (ENVI/GDAL patterns) when present.
    sidecars = [
        src.with_suffix(".hdr"),
        src.with_suffix(src.suffix + ".aux.xml") if src.suffix else src.with_name(src.name + ".aux.xml"),
        src.with_suffix(".prj"),
        src.with_suffix(".stx"),
        src.with_suffix(".xml"),
    ]
    seen: set[str] = {src.resolve().as_posix().lower()} if src.exists() else set()
    for side in sidecars:
        try:
            key = side.resolve().as_posix().lower()
        except Exception:  # noqa: BLE001
            key = side.as_posix().lower()
        if key in seen:
            continue
        seen.add(key)
        if not side.exists() or not side.is_file():
            continue
        rel = side.name
        # Keep sidecars colocated with copied raster.
        side_dst = dst.with_name(rel)
        shutil.copy2(side, side_dst)
        copied += 1
    return copied


def run(args, ctx):
    if gpd is None or box is None:
        raise RuntimeError(
            "geo_filter_rasters_by_polygon requires geopandas and shapely. Install requirements.txt in the active environment."
        )
    if rasterio is None:
        raise RuntimeError(
            "geo_filter_rasters_by_polygon requires rasterio. Install requirements.txt in the active environment."
        )

    raster_dir_text = str(args.get("raster_dir") or "").strip()
    selector_text = str(args.get("selector_path") or "").strip()
    if not raster_dir_text:
        raise ValueError("raster_dir is required")
    if not selector_text:
        raise ValueError("selector_path is required")

    raster_dir = _resolve_path(raster_dir_text, ctx)
    selector_path = _resolve_path(selector_text, ctx)
    if not raster_dir.exists() or not raster_dir.is_dir():
        raise FileNotFoundError(f"raster_dir not found: {raster_dir}")
    if not selector_path.exists():
        raise FileNotFoundError(f"selector_path not found: {selector_path}")

    copy_output_raw = str(args.get("copy_output_dir") or "").strip()
    if not copy_output_raw:
        copy_output_raw = str(args.get("output_dir") or "").strip()
    copy_selected = bool(args.get("copy_selected", False))
    selected_csv_raw = str(args.get("selected_rasters_csv") or "").strip()
    selected_footprints_raw = str(args.get("selected_footprints_path") or "").strip()
    copy_output_dir = _resolve_path(copy_output_raw, ctx) if copy_output_raw else None
    selected_csv = _resolve_path(selected_csv_raw, ctx) if selected_csv_raw else None
    selected_footprints_path = _resolve_path(selected_footprints_raw, ctx) if selected_footprints_raw else None
    if copy_selected and copy_output_dir is None:
        raise ValueError("copy_selected=true requires copy_output_dir (or output_dir) to be set")
    if copy_output_dir is not None:
        copy_output_dir.mkdir(parents=True, exist_ok=True)
    if selected_csv is not None:
        selected_csv.parent.mkdir(parents=True, exist_ok=True)
    if selected_footprints_path is not None:
        selected_footprints_path.parent.mkdir(parents=True, exist_ok=True)

    ext_set = _parse_extensions(str(args.get("raster_extensions") or ".tif,.tiff,.img,.vrt,.asc,.bil,.jp2"))
    fail_on_missing_crs = bool(args.get("fail_on_missing_crs", True))
    verbose = bool(args.get("verbose", False))

    selector = gpd.read_file(selector_path)
    if selector.empty:
        raise ValueError("selector_path contains no features")
    if selector.crs is None:
        raise ValueError("selector_path CRS is required")

    selected_rows: list[dict[str, Any]] = []
    selected_geoms: list[Any] = []
    selector_cache: dict[str, Any] = {}
    skipped_no_crs_count = 0
    error_raster_count = 0
    error_samples: list[str] = []
    inspected_raster_count = 0

    raw_candidates = sorted(p for p in raster_dir.rglob("*") if p.is_file() and p.suffix.lower() in ext_set)
    candidates: list[tuple[Path, Path]] = []
    seen_inspect_paths: set[str] = set()
    for candidate_path in raw_candidates:
        inspect_path = candidate_path
        # ENVI-style rasters often use a no-extension data file with a .hdr sidecar.
        # When candidate is .hdr, inspect the paired data file if present.
        if candidate_path.suffix.lower() == ".hdr":
            paired = candidate_path.with_suffix("")
            if paired.exists() and paired.is_file():
                inspect_path = paired
        inspect_key = inspect_path.resolve().as_posix().lower()
        if inspect_key in seen_inspect_paths:
            continue
        seen_inspect_paths.add(inspect_key)
        candidates.append((candidate_path, inspect_path))

    for candidate_path, inspect_path in candidates:
        rel = candidate_path.relative_to(raster_dir).as_posix()
        try:
            inspected_raster_count += 1
            with rasterio.open(inspect_path) as ds:
                bounds = getattr(ds, "bounds", None)
                if bounds is None:
                    raise ValueError("missing bounds")
                crs_text = _crs_text(getattr(ds, "crs", None))
                if not crs_text:
                    if fail_on_missing_crs:
                        raise ValueError(f"missing crs for raster: {rel}")
                    skipped_no_crs_count += 1
                    continue
                geom = box(float(bounds.left), float(bounds.bottom), float(bounds.right), float(bounds.top))
                selector_for_crs = selector_cache.get(crs_text)
                if selector_for_crs is None:
                    selector_for_crs = selector.to_crs(crs_text)
                    selector_cache[crs_text] = selector_for_crs
                if bool(selector_for_crs.geometry.intersects(geom).any()):
                    geom_selector_crs = gpd.GeoSeries([geom], crs=crs_text).to_crs(selector.crs).iloc[0]
                    selected_rows.append(
                        {
                            "relative_path": inspect_path.relative_to(raster_dir).as_posix(),
                            "raster_path": inspect_path.resolve().as_posix(),
                            "crs": crs_text,
                            "minx": float(bounds.left),
                            "miny": float(bounds.bottom),
                            "maxx": float(bounds.right),
                            "maxy": float(bounds.top),
                        }
                    )
                    selected_geoms.append(geom_selector_crs)
        except Exception as exc:  # noqa: BLE001
            error_raster_count += 1
            if len(error_samples) < 10:
                error_samples.append(f"{rel}: {exc}")
            if verbose:
                ctx.log(f"[geo_filter_rasters_by_polygon] skip {rel}: {exc}")

    copied_raster_count = 0
    if copy_selected and copy_output_dir is not None:
        for row in selected_rows:
            rel_path = str(row.get("relative_path") or "").strip()
            if not rel_path:
                continue
            src = (raster_dir / rel_path).resolve()
            dst = (copy_output_dir / rel_path).resolve()
            copied_raster_count += _copy_with_sidecars(src, dst)

    if selected_csv is not None:
        with selected_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["relative_path", "raster_path", "crs", "minx", "miny", "maxx", "maxy"],
            )
            writer.writeheader()
            writer.writerows(selected_rows)

    if selected_footprints_path is not None:
        selected_gdf = gpd.GeoDataFrame(selected_rows, geometry=selected_geoms, crs=selector.crs)
        if selected_rows:
            selected_gdf.to_file(selected_footprints_path, driver="GPKG")
        else:
            # Keep output contract stable, even for empty selection.
            empty = gpd.GeoDataFrame(
                columns=["relative_path", "raster_path", "crs", "minx", "miny", "maxx", "maxy", "geometry"],
                geometry="geometry",
                crs=selector.crs,
            )
            empty.to_file(selected_footprints_path, driver="GPKG")

    ctx.log(
        f"[geo_filter_rasters_by_polygon] candidates={len(candidates)} inspected={inspected_raster_count} "
        f"selected={len(selected_rows)} copied={copied_raster_count} "
        f"skipped_no_crs={skipped_no_crs_count} errors={error_raster_count}"
    )
    if error_samples:
        ctx.log(f"[geo_filter_rasters_by_polygon] error_samples={error_samples}", "WARN")

    return {
        "input_raster_dir": raster_dir.resolve().as_posix(),
        "selector_path": selector_path.resolve().as_posix(),
        "copy_output_dir": copy_output_dir.resolve().as_posix() if copy_output_dir else "",
        "selected_rasters_csv": selected_csv.resolve().as_posix() if selected_csv else "",
        "selected_footprints_path": selected_footprints_path.resolve().as_posix() if selected_footprints_path else "",
        "copied_raster_count": int(copied_raster_count),
        "candidate_raster_count": int(len(candidates)),
        "inspected_raster_count": int(inspected_raster_count),
        "selected_raster_count": int(len(selected_rows)),
        "skipped_no_crs_count": int(skipped_no_crs_count),
        "error_raster_count": int(error_raster_count),
        "error_samples": error_samples,
    }


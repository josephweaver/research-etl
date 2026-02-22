# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None


meta = {
    "name": "geo_vector_reproject",
    "version": "0.1.0",
    "description": "Reproject a vector dataset to a target CRS.",
    "inputs": [],
    "outputs": [
        "input_path",
        "output_path",
        "source_crs",
        "target_crs",
        "input_feature_count",
        "output_feature_count",
    ],
    "params": {
        "input_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "target_crs": {"type": "str", "default": ""},
        "source_crs": {"type": "str", "default": ""},
        "driver": {"type": "str", "default": ""},
        "keep_fields": {"type": "str", "default": ""},
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


def _parse_keep_fields(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    row = next(csv.reader([text], skipinitialspace=True), [])
    out = [str(v).strip() for v in row if str(v).strip()]
    # Preserve order while removing duplicates.
    seen: set[str] = set()
    keep: list[str] = []
    for field in out:
        key = field.lower()
        if key in seen:
            continue
        seen.add(key)
        keep.append(field)
    return keep


def _driver_for_path(path: Path, override: str) -> str | None:
    if override.strip():
        return override.strip()
    ext = path.suffix.lower()
    if ext == ".gpkg":
        return "GPKG"
    if ext == ".shp":
        return "ESRI Shapefile"
    if ext == ".geojson" or ext == ".json":
        return "GeoJSON"
    return None


def run(args, ctx):
    if gpd is None:
        raise RuntimeError(
            "geo_vector_reproject requires geopandas. Install requirements.txt in the active environment."
        )

    input_text = str(args.get("input_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    target_crs = str(args.get("target_crs") or "").strip()
    source_crs = str(args.get("source_crs") or "").strip()
    keep_fields = _parse_keep_fields(str(args.get("keep_fields") or ""))
    overwrite = bool(args.get("overwrite", True))
    verbose = bool(args.get("verbose", False))

    if not input_text:
        raise ValueError("input_path is required")
    if not output_text:
        raise ValueError("output_path is required")
    if not target_crs:
        raise ValueError("target_crs is required")

    input_path = _resolve_path(input_text, ctx)
    output_path = _resolve_path(output_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_path not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output_path exists and overwrite=false: {output_path}")

    gdf = gpd.read_file(input_path)
    in_count = int(len(gdf))
    if gdf.crs is None:
        if not source_crs:
            raise ValueError("input vector has no CRS; provide source_crs")
        gdf = gdf.set_crs(source_crs, allow_override=True)

    src_crs_text = str(gdf.crs.to_string() if hasattr(gdf.crs, "to_string") else gdf.crs)
    reproj = gdf.to_crs(target_crs)

    if keep_fields:
        missing = [f for f in keep_fields if f not in reproj.columns]
        if missing:
            raise ValueError(f"keep_fields not found in input columns: {missing}")
        cols = list(keep_fields)
        if "geometry" not in cols:
            cols.append("geometry")
        reproj = reproj[cols]

    if overwrite:
        _remove_existing_output(output_path)
    driver = _driver_for_path(output_path, str(args.get("driver") or ""))
    if driver:
        reproj.to_file(output_path, driver=driver)
    else:
        reproj.to_file(output_path)

    dst_crs_text = str(reproj.crs.to_string() if hasattr(reproj.crs, "to_string") else reproj.crs)
    ctx.log(
        f"[geo_vector_reproject] input={input_path.as_posix()} output={output_path.as_posix()} "
        f"source_crs={src_crs_text} target_crs={target_crs} in_count={in_count} out_count={len(reproj)}"
    )
    if verbose:
        ctx.log(f"[geo_vector_reproject] columns={list(reproj.columns)}")

    return {
        "input_path": input_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "source_crs": src_crs_text,
        "target_crs": dst_crs_text,
        "input_feature_count": in_count,
        "output_feature_count": int(len(reproj)),
    }

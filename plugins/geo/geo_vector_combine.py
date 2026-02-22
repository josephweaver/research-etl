# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None

try:
    import pandas as pd  # type: ignore
except Exception:  # noqa: BLE001
    pd = None


meta = {
    "name": "geo_vector_combine",
    "version": "0.1.0",
    "description": "Combine multiple vector files into one vector output, reprojecting to a common CRS when needed.",
    "inputs": [],
    "outputs": [
        "output_path",
        "input_count",
        "row_count",
        "source_crs",
        "target_crs",
        "input_files",
    ],
    "params": {
        "input_glob": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "target_crs": {"type": "str", "default": ""},
        "driver": {"type": "str", "default": ""},
        "source_name_field": {"type": "str", "default": "source_name"},
        "include_source_name": {"type": "bool", "default": True},
        "overwrite": {"type": "bool", "default": True},
        "allow_empty": {"type": "bool", "default": False},
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


def run(args, ctx):
    if gpd is None or pd is None:
        raise RuntimeError("geo_vector_combine requires geopandas and pandas. Install requirements.txt.")

    pattern = str(args.get("input_glob") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    if not pattern:
        raise ValueError("input_glob is required")
    if not output_text:
        raise ValueError("output_path is required")

    output_path = _resolve_path(output_text, ctx)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    overwrite = bool(args.get("overwrite", True))
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output_path exists and overwrite=false: {output_path}")

    raw_matches = sorted(glob.glob(pattern, recursive=True))
    if not raw_matches:
        raw_matches = sorted(glob.glob(str((ctx.workdir / pattern).as_posix()), recursive=True))
    paths = [Path(p) for p in raw_matches if Path(p).is_file()]
    allow_empty = bool(args.get("allow_empty", False))
    if not paths and not allow_empty:
        raise FileNotFoundError(f"no files matched input_glob: {pattern}")

    include_source_name = bool(args.get("include_source_name", True))
    source_name_field = str(args.get("source_name_field") or "source_name").strip() or "source_name"
    target_crs = str(args.get("target_crs") or "").strip()
    verbose = bool(args.get("verbose", False))

    frames: list[Any] = []
    source_crs = ""
    for p in paths:
        gdf = gpd.read_file(p)
        if gdf.empty:
            continue
        if gdf.crs is None:
            raise ValueError(f"input vector has no CRS: {p}")
        if not source_crs:
            source_crs = str(gdf.crs.to_string() if hasattr(gdf.crs, "to_string") else gdf.crs)
        if target_crs:
            gdf = gdf.to_crs(target_crs)
        if include_source_name:
            gdf[source_name_field] = p.name
        frames.append(gdf)

    if frames:
        combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=(target_crs or source_crs))
    else:
        crs_val = target_crs or source_crs or None
        combined = gpd.GeoDataFrame(geometry=[], crs=crs_val)
        if include_source_name and source_name_field not in combined.columns:
            combined[source_name_field] = []

    if overwrite:
        _remove_existing_output(output_path)
    driver = _driver_for_path(output_path, str(args.get("driver") or ""))
    if driver:
        combined.to_file(output_path, driver=driver)
    else:
        combined.to_file(output_path)

    out_crs = str(combined.crs.to_string() if hasattr(combined.crs, "to_string") else combined.crs)
    ctx.log(
        f"[geo_vector_combine] inputs={len(paths)} rows={len(combined)} output={output_path.as_posix()} "
        f"target_crs={target_crs or source_crs}"
    )
    if verbose:
        ctx.log(f"[geo_vector_combine] files={[p.as_posix() for p in paths[:20]]}")

    return {
        "output_path": output_path.resolve().as_posix(),
        "input_count": int(len(paths)),
        "row_count": int(len(combined)),
        "source_crs": source_crs,
        "target_crs": out_crs,
        "input_files": [p.resolve().as_posix() for p in paths],
    }

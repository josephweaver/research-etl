# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

try:
    import rasterio  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None


def parse_world_file_text(text: str) -> Optional[Dict[str, float]]:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if len(lines) < 6:
        return None
    try:
        values = [float(lines[i]) for i in range(6)]
    except Exception:  # noqa: BLE001
        return None
    return {
        "pixel_size_x": values[0],
        "rotation_x": values[1],
        "rotation_y": values[2],
        "pixel_size_y": values[3],
        "origin_x": values[4],
        "origin_y": values[5],
    }


def _read_sidecar_text(path: Path, suffix: str, max_chars: int = 1200) -> Optional[str]:
    sidecar = path.with_suffix(suffix)
    if not sidecar.exists():
        return None
    try:
        return sidecar.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except Exception:  # noqa: BLE001
        return None


def extract_raster_metadata(path: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": path.resolve().as_posix(),
        "suffix": path.suffix.lower(),
        "source": "filesystem",
        "driver": None,
        "width": None,
        "height": None,
        "count": None,
        "dtype": None,
        "crs": None,
        "bounds": None,
        "transform": None,
        "has_prj_sidecar": path.with_suffix(".prj").exists(),
        "has_world_file_sidecar": path.with_suffix(".tfw").exists(),
    }

    if rasterio is not None:
        try:
            with rasterio.open(path) as ds:
                out["driver"] = str(ds.driver or "")
                out["width"] = int(ds.width)
                out["height"] = int(ds.height)
                out["count"] = int(ds.count)
                if ds.dtypes:
                    out["dtype"] = str(ds.dtypes[0])
                if ds.crs:
                    out["crs"] = ds.crs.to_string()
                if ds.bounds:
                    out["bounds"] = {
                        "left": float(ds.bounds.left),
                        "bottom": float(ds.bounds.bottom),
                        "right": float(ds.bounds.right),
                        "top": float(ds.bounds.top),
                    }
                out["transform"] = [float(v) for v in tuple(ds.transform)[:6]]
                return out
        except Exception as exc:  # noqa: BLE001
            out["rasterio_error"] = str(exc)
    else:
        out["rasterio_error"] = "rasterio not installed"

    prj_text = _read_sidecar_text(path, ".prj")
    if prj_text:
        out["crs_wkt_excerpt"] = prj_text
    tfw_text = _read_sidecar_text(path, ".tfw")
    if tfw_text:
        parsed = parse_world_file_text(tfw_text)
        if parsed:
            out["world_file"] = parsed
    return out

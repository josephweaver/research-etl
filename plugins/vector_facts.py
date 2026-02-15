from __future__ import annotations

import csv
import json
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


meta = {
    "name": "vector_facts",
    "version": "0.1.0",
    "description": "Build a file manifest and collect basic facts for shapefiles under a directory tree.",
    "inputs": [],
    "outputs": [
        "input_dir",
        "output_dir",
        "manifest_csv",
        "manifest_json",
        "vector_facts_csv",
        "vector_facts_json",
        "file_count",
        "vector_file_count",
    ],
    "params": {
        "input_dir": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ".runs/vector_facts"},
        "shape_glob": {"type": "str", "default": "**/*.shp"},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


_SHAPE_TYPE_MAP = {
    0: "Null Shape",
    1: "Point",
    3: "Polyline",
    5: "Polygon",
    8: "MultiPoint",
    11: "PointZ",
    13: "PolylineZ",
    15: "PolygonZ",
    18: "MultiPointZ",
    21: "PointM",
    23: "PolylineM",
    25: "PolygonM",
    28: "MultiPointM",
    31: "MultiPatch",
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


def _read_shp_header(shp_path: Path) -> Dict[str, Any]:
    with shp_path.open("rb") as f:
        header = f.read(100)
    if len(header) < 100:
        raise ValueError(f"invalid shapefile header (expected 100 bytes): {shp_path}")

    file_code = struct.unpack(">i", header[0:4])[0]
    file_len_words = struct.unpack(">i", header[24:28])[0]
    version = struct.unpack("<i", header[28:32])[0]
    shape_type_code = struct.unpack("<i", header[32:36])[0]
    xmin, ymin, xmax, ymax = struct.unpack("<4d", header[36:68])
    return {
        "file_code": file_code,
        "file_length_bytes": int(file_len_words * 2),
        "version": version,
        "shape_type_code": shape_type_code,
        "geometry_type": _SHAPE_TYPE_MAP.get(shape_type_code, f"Unknown({shape_type_code})"),
        "bbox_xmin": xmin,
        "bbox_ymin": ymin,
        "bbox_xmax": xmax,
        "bbox_ymax": ymax,
    }


def _read_dbf_record_count(dbf_path: Path) -> Optional[int]:
    if not dbf_path.exists():
        return None
    with dbf_path.open("rb") as f:
        header = f.read(32)
    if len(header) < 8:
        return None
    # dBASE header bytes 4-7 (little-endian uint32) = number of records.
    return int(struct.unpack("<I", header[4:8])[0])


def _read_prj_excerpt(prj_path: Path, max_chars: int = 1200) -> str:
    if not prj_path.exists():
        return ""
    try:
        return prj_path.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except Exception:  # noqa: BLE001
        return ""


def run(args, ctx):
    input_dir_text = str(args.get("input_dir") or "").strip()
    if not input_dir_text:
        raise ValueError("input_dir is required")
    input_dir = _resolve_path(input_dir_text, ctx)
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input_dir not found: {input_dir}")

    output_dir = _resolve_path(str(args.get("output_dir") or ".runs/vector_facts"), ctx)
    output_dir.mkdir(parents=True, exist_ok=True)
    shape_glob = str(args.get("shape_glob") or "**/*.shp").strip() or "**/*.shp"
    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[vector_facts] start input={input_dir.resolve().as_posix()} output={output_dir.resolve().as_posix()} "
        f"shape_glob={shape_glob}"
    )

    file_rows: List[Dict[str, Any]] = []
    for path in sorted(p for p in input_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(input_dir).as_posix()
        st = path.stat()
        file_rows.append(
            {
                "relative_path": rel,
                "file_name": path.name,
                "extension": path.suffix.lower(),
                "size_bytes": int(st.st_size),
                "created_utc": _to_utc_iso(st.st_ctime),
                "modified_utc": _to_utc_iso(st.st_mtime),
            }
        )

    shape_files = sorted(input_dir.glob(shape_glob))
    vector_rows: List[Dict[str, Any]] = []
    for shp in shape_files:
        if not shp.is_file():
            continue
        rel = shp.relative_to(input_dir).as_posix()
        dbf = shp.with_suffix(".dbf")
        shx = shp.with_suffix(".shx")
        prj = shp.with_suffix(".prj")
        try:
            header = _read_shp_header(shp)
            vector_rows.append(
                {
                    "relative_path": rel,
                    "geometry_type": header["geometry_type"],
                    "shape_type_code": int(header["shape_type_code"]),
                    "feature_count": _read_dbf_record_count(dbf),
                    "bbox_xmin": header["bbox_xmin"],
                    "bbox_ymin": header["bbox_ymin"],
                    "bbox_xmax": header["bbox_xmax"],
                    "bbox_ymax": header["bbox_ymax"],
                    "has_dbf": int(dbf.exists()),
                    "has_shx": int(shx.exists()),
                    "has_prj": int(prj.exists()),
                    "prj_wkt_excerpt": _read_prj_excerpt(prj),
                }
            )
        except Exception as exc:  # noqa: BLE001
            vector_rows.append(
                {
                    "relative_path": rel,
                    "geometry_type": "",
                    "shape_type_code": "",
                    "feature_count": "",
                    "bbox_xmin": "",
                    "bbox_ymin": "",
                    "bbox_xmax": "",
                    "bbox_ymax": "",
                    "has_dbf": int(dbf.exists()),
                    "has_shx": int(shx.exists()),
                    "has_prj": int(prj.exists()),
                    "prj_wkt_excerpt": _read_prj_excerpt(prj),
                    "error": str(exc),
                }
            )

    manifest_csv = output_dir / "file_manifest.csv"
    with manifest_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["relative_path", "file_name", "extension", "size_bytes", "created_utc", "modified_utc"],
        )
        writer.writeheader()
        writer.writerows(file_rows)

    manifest_json = output_dir / "file_manifest.json"
    manifest_json.write_text(json.dumps(file_rows, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    facts_csv = output_dir / "vector_facts.csv"
    with facts_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "relative_path",
                "geometry_type",
                "shape_type_code",
                "feature_count",
                "bbox_xmin",
                "bbox_ymin",
                "bbox_xmax",
                "bbox_ymax",
                "has_dbf",
                "has_shx",
                "has_prj",
                "prj_wkt_excerpt",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(vector_rows)

    facts_json = output_dir / "vector_facts.json"
    facts_json.write_text(json.dumps(vector_rows, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    ctx.log(
        f"[vector_facts] files={len(file_rows)} vectors={len(vector_rows)} "
        f"output={output_dir.as_posix()}"
    )
    if verbose and vector_rows:
        ctx.log(f"[vector_facts] vector_preview={vector_rows[:3]}")
    return {
        "input_dir": input_dir.resolve().as_posix(),
        "output_dir": output_dir.resolve().as_posix(),
        "manifest_csv": manifest_csv.resolve().as_posix(),
        "manifest_json": manifest_json.resolve().as_posix(),
        "vector_facts_csv": facts_csv.resolve().as_posix(),
        "vector_facts_json": facts_json.resolve().as_posix(),
        "file_count": len(file_rows),
        "vector_file_count": len(vector_rows),
    }

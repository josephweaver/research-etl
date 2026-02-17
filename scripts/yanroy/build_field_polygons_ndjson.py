from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Sequence


_IGNORED_SUFFIXES = {
    ".hdr",
    ".tfw",
    ".prj",
    ".aux",
    ".xml",
    ".ovr",
}


def _read_tiles_of_interest(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"tiles csv not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames or "tile_id" not in rdr.fieldnames:
            raise ValueError("tiles.of.interest.csv must contain tile_id column")
        seen: set[str] = set()
        ordered: List[str] = []
        for row in rdr:
            tile_id = str(row.get("tile_id") or "").strip().lower()
            if not tile_id or tile_id in seen:
                continue
            seen.add(tile_id)
            ordered.append(tile_id)
    if not ordered:
        raise ValueError("no tile_id rows found in tiles.of.interest.csv")
    return ordered


def _candidate_rasters(tile_dir: Path) -> Sequence[Path]:
    if not tile_dir.exists() or not tile_dir.is_dir():
        return []
    out: List[Path] = []
    for p in sorted(tile_dir.rglob("*")):
        if not p.is_file():
            continue
        low = p.name.lower()
        if "field_segments" not in low:
            continue
        if low.endswith("_ancillary_data"):
            continue
        if p.suffix.lower() in _IGNORED_SUFFIXES:
            continue
        out.append(p)
    return out


def _feature_rows_for_raster(tile_id: str, raster_path: Path) -> List[dict]:
    try:
        import numpy as np
        import rasterio
        from rasterio import features
        from rasterio.warp import transform_geom
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_field_polygons_ndjson requires rasterio and numpy") from exc

    features_out: List[dict] = []
    with rasterio.open(raster_path) as ds:
        arr = ds.read(1, masked=False)
        nodata = ds.nodata
        valid = arr > 0
        if nodata is not None:
            valid = valid & (arr != nodata)

        for geom, val in features.shapes(arr, mask=valid, transform=ds.transform):
            field_id = int(val)
            if field_id <= 0:
                continue
            lonlat_geom = transform_geom(ds.crs, "EPSG:4326", geom, antimeridian_cutting=True, precision=6)
            features_out.append(
                {
                    "type": "Feature",
                    "geometry": lonlat_geom,
                    "properties": {
                        "primary_key": f"{tile_id}_{field_id}",
                        "tile_id": tile_id,
                        "field_id": field_id,
                    },
                }
            )
    return features_out


def build_field_polygons_ndjson(
    tiles_csv: Path,
    fields_dir: Path,
    output_ndjson: Path,
    summary_json: Path,
    verbose: bool = False,
) -> Dict[str, int]:
    if not fields_dir.exists() or not fields_dir.is_dir():
        raise FileNotFoundError(f"fields dir not found: {fields_dir}")

    tiles = _read_tiles_of_interest(tiles_csv)
    output_ndjson.parent.mkdir(parents=True, exist_ok=True)

    feature_count = 0
    tile_ok = 0
    tile_missing = 0
    tile_errors = 0
    errors: List[dict] = []

    with output_ndjson.open("w", encoding="utf-8") as out:
        for tile_id in tiles:
            tile_dir = fields_dir / tile_id
            candidates = _candidate_rasters(tile_dir)
            if not candidates:
                tile_missing += 1
                if verbose:
                    print(f"[build_field_polygons_ndjson][WARN] no field raster found for {tile_id}")
                continue
            raster_path = candidates[0]
            try:
                rows = _feature_rows_for_raster(tile_id=tile_id, raster_path=raster_path)
            except Exception as exc:  # noqa: BLE001
                tile_errors += 1
                errors.append({"tile_id": tile_id, "error": str(exc), "raster": raster_path.as_posix()})
                if verbose:
                    print(f"[build_field_polygons_ndjson][WARN] failed tile={tile_id}: {exc}")
                continue
            for row in rows:
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
            feature_count += len(rows)
            tile_ok += 1
            if verbose:
                print(
                    f"[build_field_polygons_ndjson] tile={tile_id} raster={raster_path.name} features={len(rows)}"
                )

    summary = {
        "tiles_requested": len(tiles),
        "tiles_ok": tile_ok,
        "tiles_missing_raster": tile_missing,
        "tiles_failed": tile_errors,
        "feature_count": feature_count,
        "output_ndjson": output_ndjson.resolve().as_posix(),
        "errors": errors,
    }
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if verbose:
        print(
            f"[build_field_polygons_ndjson] requested={len(tiles)} ok={tile_ok} "
            f"missing={tile_missing} failed={tile_errors} features={feature_count}"
        )
    return {
        "tiles_requested": len(tiles),
        "tiles_ok": tile_ok,
        "tiles_missing_raster": tile_missing,
        "tiles_failed": tile_errors,
        "feature_count": feature_count,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Create newline-delimited GeoJSON field polygons for tiles.of.interest from Yanroy field_id rasters."
    )
    ap.add_argument("--tiles-csv", required=True, help="Path to tiles.of.interest.csv")
    ap.add_argument("--fields-dir", required=True, help="Root directory containing tile folders (hXXvYY)")
    ap.add_argument("--output-ndjson", required=True, help="Output NDJSON path")
    ap.add_argument("--summary-json", required=True, help="Output summary JSON path")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[build_field_polygons_ndjson][WARN] ignoring unknown args: {' '.join(unknown)}")

    build_field_polygons_ndjson(
        tiles_csv=Path(args.tiles_csv).expanduser().resolve(),
        fields_dir=Path(args.fields_dir).expanduser().resolve(),
        output_ndjson=Path(args.output_ndjson).expanduser().resolve(),
        summary_json=Path(args.summary_json).expanduser().resolve(),
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

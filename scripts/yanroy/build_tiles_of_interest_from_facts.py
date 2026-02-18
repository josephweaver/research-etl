from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


_TILE_RE = re.compile(r"(?i)(h\d{2}v\d{2})")


def _norm(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "").replace("_", "")


def _parse_state_token(raw: str) -> Tuple[str, str]:
    val = str(raw or "").strip()
    if not val:
        return "", ""
    digits = "".join(ch for ch in val if ch.isdigit())
    if len(digits) in {1, 2}:
        return digits.zfill(2), ""
    alpha = "".join(ch for ch in val if ch.isalpha()).upper()
    if len(alpha) == 2:
        return "", alpha
    return "", ""


def _read_state_codes(path: Path) -> Tuple[set[str], set[str]]:
    fips: set[str] = set()
    abbr: set[str] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        has_header = False
        if sample.strip():
            try:
                has_header = csv.Sniffer().has_header(sample)
            except csv.Error:
                # Single-token/line files can fail Sniffer delimiter detection.
                # Treat as plain row values.
                has_header = False

        if has_header:
            rdr = csv.DictReader(f)
            for row in rdr:
                for k, raw in row.items():
                    key = _norm(k)
                    val = str(raw or "").strip()
                    if not val:
                        continue
                    if key in {"statefp", "statefips", "fips", "statecode", "stfips"}:
                        vv = "".join(ch for ch in val if ch.isdigit())
                        if vv:
                            fips.add(vv.zfill(2))
                    if key in {"stusps", "state", "stateabbr", "stateabbrev", "statepostal"}:
                        ab = "".join(ch for ch in val if ch.isalpha()).upper()
                        if len(ab) == 2:
                            abbr.add(ab)
        else:
            rdr = csv.reader(f)
            for row in rdr:
                for raw in row:
                    ff, aa = _parse_state_token(raw)
                    if ff:
                        fips.add(ff)
                    if aa:
                        abbr.add(aa)

    if not fips and not abbr:
        raise ValueError("states csv did not yield any state codes")
    return fips, abbr


def _find_state_shp(extract_dir: Path) -> Path:
    candidates = sorted(extract_dir.rglob("*.shp"))
    for p in candidates:
        name = p.name.lower()
        if "state" in name and "_us_" in name:
            return p
    for p in candidates:
        if "state" in p.name.lower():
            return p
    raise FileNotFoundError(f"could not find state shapefile under {extract_dir}")


def _parse_bounds(text: str) -> tuple[float, float, float, float] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        return None
    try:
        minx, miny, maxx, maxy = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))
    except Exception:
        return None
    if not (maxx > minx and maxy > miny):
        return None
    return minx, miny, maxx, maxy


def _parse_hv(tile_id: str) -> tuple[int | None, int | None]:
    m = _TILE_RE.search(str(tile_id or ""))
    if not m:
        return None, None
    text = m.group(1).lower()
    try:
        return int(text[1:3]), int(text[4:6])
    except Exception:
        return None, None


def _normalize_tile_id(relative_path: str) -> str:
    rel = str(relative_path or "").strip()
    if not rel:
        return ""
    m = _TILE_RE.search(rel)
    if m:
        return m.group(1).lower()
    # Fallback for non-hXXvYY naming: keep stable file stem-ish id.
    name = Path(rel).name
    stem = Path(name).stem if name else rel
    return str(stem or rel).strip().lower()


def _read_raster_footprints_from_facts(raster_facts_csv: Path) -> List[Dict[str, Any]]:
    # One footprint per logical tile_id; duplicate rows/bands are expected in facts output.
    by_tile: Dict[str, Dict[str, Any]] = {}
    with raster_facts_csv.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("raster_facts csv has no header")
        for row in rdr:
            rel = str(row.get("relative_path") or "").strip()
            if not rel:
                continue
            bounds = _parse_bounds(str(row.get("bounds") or ""))
            if bounds is None:
                continue
            crs_text = str(row.get("crs") or "").strip()
            if not crs_text:
                continue
            tile_id = _normalize_tile_id(rel)
            if not tile_id:
                continue
            h_val, v_val = _parse_hv(tile_id)
            minx, miny, maxx, maxy = bounds
            prev = by_tile.get(tile_id)
            if prev is not None:
                # Keep first footprint for tile_id to avoid duplicate-band spam.
                continue
            by_tile[tile_id] = {
                "tile_id": tile_id,
                "h": h_val,
                "v": v_val,
                "crs": crs_text,
                "minx": minx,
                "miny": miny,
                "maxx": maxx,
                "maxy": maxy,
            }
    out = list(by_tile.values())
    if not out:
        raise ValueError("no usable raster footprints (relative_path+bounds+crs) found in raster_facts csv")
    return out


def build_tiles_of_interest(
    states_csv: Path,
    state_shapefile: Path,
    raster_facts_csv: Path,
    output_csv: Path,
    touch_buffer_m: float = 1.0,
    verbose: bool = False,
) -> int:
    try:
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "build_tiles_of_interest_from_facts requires geopandas+shapely. "
            "Install them in the execution environment."
        ) from exc

    fips_interest, abbr_interest = _read_state_codes(states_csv)
    footprints = _read_raster_footprints_from_facts(raster_facts_csv)

    gdf = gpd.read_file(state_shapefile)
    if gdf.empty:
        raise RuntimeError(f"state shapefile has no features: {state_shapefile}")

    has_statefp = "STATEFP" in gdf.columns
    has_stusps = "STUSPS" in gdf.columns
    if not has_statefp and not has_stusps:
        raise RuntimeError("state shapefile missing STATEFP and STUSPS columns")

    missing_fips: list[str] = []
    missing_abbr: list[str] = []
    if has_statefp and fips_interest:
        known_fips = set(gdf["STATEFP"].astype(str).str.zfill(2).tolist())
        missing_fips = sorted(fips_interest - known_fips)
    if has_stusps and abbr_interest:
        known_abbr = set(gdf["STUSPS"].astype(str).str.upper().tolist())
        missing_abbr = sorted(abbr_interest - known_abbr)
    if missing_fips or missing_abbr:
        parts: list[str] = []
        if missing_fips:
            parts.append("fips=" + ",".join(missing_fips))
        if missing_abbr:
            parts.append("abbr=" + ",".join(missing_abbr))
        raise RuntimeError(
            "requested states not found in state shapefile: " + " ".join(parts)
        )

    mask = None
    if has_statefp and fips_interest:
        m = gdf["STATEFP"].astype(str).str.zfill(2).isin(fips_interest)
        mask = m if mask is None else (mask | m)
    if has_stusps and abbr_interest:
        m = gdf["STUSPS"].astype(str).str.upper().isin(abbr_interest)
        mask = m if mask is None else (mask | m)
    if mask is None:
        raise RuntimeError("no matching state columns found for provided codes")

    gdf = gdf.loc[mask].copy()
    if gdf.empty:
        raise RuntimeError("no states matched states.of.interest.csv")
    if gdf.crs is None:
        raise RuntimeError("state shapefile missing CRS")
    src_crs = str(gdf.crs)
    footprints_by_crs: Dict[str, List[Dict[str, Any]]] = {}
    for fp in footprints:
        crs_key = str(fp["crs"])
        footprints_by_crs.setdefault(crs_key, []).append(fp)

    state_cols = [c for c in ["STATEFP", "STUSPS", "geometry"] if c in gdf.columns]
    joined_parts = []
    for crs_key, fps in footprints_by_crs.items():
        state_proj = gdf.to_crs(crs_key)
        state_proj = state_proj.loc[state_proj.geometry.notna()].copy()
        try:
            state_proj["geometry"] = state_proj.geometry.make_valid()
        except Exception:
            state_proj["geometry"] = state_proj.geometry.buffer(0)
        state_proj = state_proj.loc[~state_proj.geometry.is_empty].copy()
        if state_proj.empty:
            continue
        if float(touch_buffer_m) > 0.0:
            # Expand state boundaries slightly to include edge-touch cases that can
            # be missed due to transform / floating precision effects.
            state_proj["geometry"] = state_proj.geometry.buffer(float(touch_buffer_m))
        tile_rows = []
        for fp in fps:
            tile_rows.append(
                {
                    "tile_id": fp["tile_id"],
                    "h": fp["h"],
                    "v": fp["v"],
                    "geometry": box(fp["minx"], fp["miny"], fp["maxx"], fp["maxy"]),
                }
            )
        tdf = gpd.GeoDataFrame(tile_rows, geometry="geometry", crs=crs_key)
        joined = gpd.sjoin(
            state_proj[state_cols],
            tdf[["tile_id", "h", "v", "geometry"]],
            how="inner",
            predicate="intersects",
        )
        if not joined.empty:
            joined_parts.append(joined)
    if not joined_parts:
        raise RuntimeError("no raster_facts footprints intersect selected states")
    if len(joined_parts) == 1:
        joined = joined_parts[0]
    else:
        joined = gpd.GeoDataFrame(pd.concat(joined_parts, ignore_index=True), geometry="geometry")

    def _to_int_or_blank(value: Any) -> int | str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return ""
        try:
            return int(float(text))
        except Exception:
            return ""

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    seen: set[Tuple[str, str]] = set()
    rows: List[dict] = []
    for _, row in joined.iterrows():
        statefp = str(row.get("STATEFP", "") or "").zfill(2)
        stusps = str(row.get("STUSPS", "") or "").upper()
        tile_id = str(row.get("tile_id", "") or "").lower()
        if not tile_id:
            continue
        key = (statefp or stusps, tile_id)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "statefp": statefp,
                "stusps": stusps,
                "tile_id": tile_id,
                "h": _to_int_or_blank(row.get("h")),
                "v": _to_int_or_blank(row.get("v")),
            }
        )
    rows.sort(key=lambda r: (r["statefp"], r["tile_id"]))

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["statefp", "stusps", "tile_id", "h", "v"])
        w.writeheader()
        w.writerows(rows)

    if verbose:
        unique_tiles = sorted({str(r["tile_id"]).lower() for r in rows})
        print(
            "[build_tiles_of_interest_from_facts] "
            f"state_src_crs={src_crs} raster_crs_count={len(footprints_by_crs)} "
            f"states={len(gdf)} available_tiles={len(footprints)} "
            f"matched_rows={len(rows)} matched_unique_tiles={len(unique_tiles)} "
            f"touch_buffer_m={float(touch_buffer_m)} output={output_csv.as_posix()}"
        )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Build tiles.of.interest.csv by intersecting selected states with raster "
            "footprints read from raster_facts.csv (bounds + crs)."
        )
    )
    ap.add_argument("--states-csv", required=True, help="Path to states.of.interest.csv")
    ap.add_argument("--raster-facts-csv", required=True, help="Path to combined raster_facts.csv")
    ap.add_argument("--state-shp", default="", help="Path to TIGER state shapefile")
    ap.add_argument("--extract-dir", default="", help="Directory containing state shapefile(s)")
    ap.add_argument("--output-csv", required=True, help="Output CSV path")
    ap.add_argument(
        "--touch-buffer-m",
        type=float,
        default=1.0,
        help="Positive buffer in projected units (usually meters) applied to state polygons before intersect.",
    )
    ap.add_argument("--verbose", action="store_true")
    args, unknown_args = ap.parse_known_args(argv)
    if unknown_args:
        print(
            "[build_tiles_of_interest_from_facts][WARN] ignoring unknown arguments: "
            + " ".join(str(x) for x in unknown_args)
        )

    states_csv = Path(args.states_csv).expanduser().resolve()
    if not states_csv.exists():
        raise FileNotFoundError(f"states csv not found: {states_csv}")

    raster_facts_csv = Path(args.raster_facts_csv).expanduser().resolve()
    if not raster_facts_csv.exists():
        raise FileNotFoundError(f"raster_facts csv not found: {raster_facts_csv}")

    state_shp = Path(args.state_shp).expanduser().resolve() if str(args.state_shp or "").strip() else None
    if state_shp is None:
        extract_dir = Path(args.extract_dir).expanduser().resolve()
        if not extract_dir.exists():
            raise FileNotFoundError(f"extract dir not found: {extract_dir}")
        state_shp = _find_state_shp(extract_dir)
    if not state_shp.exists():
        raise FileNotFoundError(f"state shapefile not found: {state_shp}")

    output_csv = Path(args.output_csv).expanduser().resolve()
    build_tiles_of_interest(
        states_csv=states_csv,
        state_shapefile=state_shp,
        raster_facts_csv=raster_facts_csv,
        output_csv=output_csv,
        touch_buffer_m=float(args.touch_buffer_m),
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

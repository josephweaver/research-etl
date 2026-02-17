from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, List, Set, Tuple


MODIS_XMIN = -20015109.354
MODIS_YMAX = 10007554.677
MODIS_TILE_SIZE = 1111950.5196666666
MODIS_H_MAX = 35
MODIS_V_MAX = 17
MODIS_SINU_WKT = (
    "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext +units=m +no_defs"
)
_TILE_RE = re.compile(r"(?i)h\d{2}v\d{2}")


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


def _iter_modis_tiles() -> Iterable[tuple[int, int, float, float, float, float]]:
    for h in range(0, MODIS_H_MAX + 1):
        for v in range(0, MODIS_V_MAX + 1):
            minx = MODIS_XMIN + (h * MODIS_TILE_SIZE)
            maxx = minx + MODIS_TILE_SIZE
            maxy = MODIS_YMAX - (v * MODIS_TILE_SIZE)
            miny = maxy - MODIS_TILE_SIZE
            yield h, v, minx, miny, maxx, maxy


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


def _read_tiles_from_raster_facts(raster_facts_csv: Path) -> Set[str]:
    tiles: Set[str] = set()
    with raster_facts_csv.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("raster_facts csv has no header")
        for row in rdr:
            rel = str(row.get("relative_path") or "").strip()
            if not rel:
                continue
            match = _TILE_RE.search(rel)
            if match:
                tiles.add(match.group(0).lower())
    if not tiles:
        raise ValueError("no tile ids (hXXvYY) found in raster_facts csv relative_path")
    return tiles


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
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "build_tiles_of_interest_from_facts requires geopandas+shapely. "
            "Install them in the execution environment."
        ) from exc

    fips_interest, abbr_interest = _read_state_codes(states_csv)
    tiles_available = _read_tiles_from_raster_facts(raster_facts_csv)

    gdf = gpd.read_file(state_shapefile)
    if gdf.empty:
        raise RuntimeError(f"state shapefile has no features: {state_shapefile}")

    has_statefp = "STATEFP" in gdf.columns
    has_stusps = "STUSPS" in gdf.columns
    if not has_statefp and not has_stusps:
        raise RuntimeError("state shapefile missing STATEFP and STUSPS columns")

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

    gdf = gdf.to_crs(MODIS_SINU_WKT)
    # Repair invalid geometries after reprojection to reduce topology/CRS edge-case misses.
    gdf = gdf.loc[gdf.geometry.notna()].copy()
    try:
        gdf["geometry"] = gdf.geometry.make_valid()
    except Exception:
        gdf["geometry"] = gdf.geometry.buffer(0)
    gdf = gdf.loc[~gdf.geometry.is_empty].copy()
    if gdf.empty:
        raise RuntimeError("all selected state geometries became empty/invalid after CRS transform")
    if float(touch_buffer_m) > 0.0:
        # Expand state boundaries slightly to include edge-touch cases that can
        # be missed due to CRS transform / floating precision effects.
        gdf["geometry"] = gdf.geometry.buffer(float(touch_buffer_m))
    tiles = []
    for h, v, minx, miny, maxx, maxy in _iter_modis_tiles():
        tile_id = f"h{h:02d}v{v:02d}"
        if tile_id not in tiles_available:
            continue
        tiles.append({"tile_id": tile_id, "h": h, "v": v, "geometry": box(minx, miny, maxx, maxy)})
    if not tiles:
        raise RuntimeError("no MODIS tile polygons left after filtering by raster_facts.csv")
    tdf = gpd.GeoDataFrame(tiles, geometry="geometry", crs=MODIS_SINU_WKT)

    state_cols = [c for c in ["STATEFP", "STUSPS", "geometry"] if c in gdf.columns]
    joined = gpd.sjoin(
        gdf[state_cols],
        tdf[["tile_id", "h", "v", "geometry"]],
        how="inner",
        predicate="intersects",
    )
    if joined.empty:
        raise RuntimeError("no available MODIS tiles intersect selected states")

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
                "h": int(row.get("h")),
                "v": int(row.get("v")),
            }
        )
    rows.sort(key=lambda r: (r["statefp"], r["tile_id"]))

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["statefp", "stusps", "tile_id", "h", "v"])
        w.writeheader()
        w.writerows(rows)

    if verbose:
        print(
            "[build_tiles_of_interest_from_facts] "
            f"state_src_crs={src_crs} state_join_crs={MODIS_SINU_WKT} "
            f"states={len(gdf)} available_tiles={len(tiles_available)} matched_rows={len(rows)} "
            f"touch_buffer_m={float(touch_buffer_m)} output={output_csv.as_posix()}"
        )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Build tiles.of.interest.csv by intersecting selected states with MODIS tiles, "
            "then filtering to tile_ids present in raster_facts.csv"
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
        help="Positive buffer (meters, MODIS sinusoidal) applied to state polygons before intersect.",
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

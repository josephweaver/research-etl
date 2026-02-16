from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, List, Tuple


MODIS_XMIN = -20015109.354
MODIS_YMAX = 10007554.677
MODIS_TILE_SIZE = 1111950.5196666666
MODIS_H_MAX = 35
MODIS_V_MAX = 17
MODIS_SINU_WKT = (
    "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext +units=m +no_defs"
)


def _norm(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "").replace("_", "")


def _read_state_codes(path: Path) -> Tuple[set[str], set[str]]:
    fips: set[str] = set()
    abbr: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("states csv has no header")
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
                    if ab:
                        abbr.add(ab)
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


def build_tiles_of_interest(states_csv: Path, state_shapefile: Path, output_csv: Path, verbose: bool = False) -> int:
    try:
        import geopandas as gpd
        from shapely.geometry import box
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "build_tiles_of_interest requires geopandas+shapely. "
            "Install them in the execution environment."
        ) from exc

    fips_interest, abbr_interest = _read_state_codes(states_csv)
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

    gdf = gdf.to_crs(MODIS_SINU_WKT)
    tiles = []
    for h, v, minx, miny, maxx, maxy in _iter_modis_tiles():
        tiles.append({"tile_id": f"h{h:02d}v{v:02d}", "h": h, "v": v, "geometry": box(minx, miny, maxx, maxy)})
    tdf = gpd.GeoDataFrame(tiles, geometry="geometry", crs=MODIS_SINU_WKT)

    joined = gpd.sjoin(
        gdf[["STATEFP", "STUSPS", "geometry"] if has_statefp and has_stusps else [c for c in ["STATEFP", "STUSPS", "geometry"] if c in gdf.columns]],
        tdf[["tile_id", "h", "v", "geometry"]],
        how="inner",
        predicate="intersects",
    )
    if joined.empty:
        raise RuntimeError("no MODIS tiles intersect selected states")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    seen: set[Tuple[str, str]] = set()
    rows: List[dict] = []
    for _, row in joined.iterrows():
        statefp = str(row.get("STATEFP", "") or "").zfill(2)
        stusps = str(row.get("STUSPS", "") or "").upper()
        tile_id = str(row.get("tile_id", "") or "")
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
        print(f"[build_tiles_of_interest] states={len(gdf)} rows={len(rows)} output={output_csv.as_posix()}")
    return len(rows)


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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build MODIS tiles.of.interest.csv by intersecting states with MODIS grid.")
    ap.add_argument("--states-csv", required=True, help="Path to states.of.interest.csv")
    ap.add_argument("--state-shp", default="", help="Path to TIGER state shapefile")
    ap.add_argument("--extract-dir", default="", help="Directory containing state shapefile(s)")
    ap.add_argument("--output-csv", required=True, help="Output CSV path")
    ap.add_argument("--verbose", action="store_true")
    args, unknown_args = ap.parse_known_args(argv)
    if unknown_args:
        print(f"[build_tiles_of_interest][WARN] ignoring unknown arguments: {' '.join(str(x) for x in unknown_args)}")

    states_csv = Path(args.states_csv).expanduser().resolve()
    if not states_csv.exists():
        raise FileNotFoundError(f"states csv not found: {states_csv}")

    state_shp = Path(args.state_shp).expanduser().resolve() if str(args.state_shp or "").strip() else None
    if state_shp is None:
        extract_dir = Path(args.extract_dir).expanduser().resolve()
        if not extract_dir.exists():
            raise FileNotFoundError(f"extract dir not found: {extract_dir}")
        state_shp = _find_state_shp(extract_dir)
    if not state_shp.exists():
        raise FileNotFoundError(f"state shapefile not found: {state_shp}")

    output_csv = Path(args.output_csv).expanduser().resolve()
    build_tiles_of_interest(states_csv=states_csv, state_shapefile=state_shp, output_csv=output_csv, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

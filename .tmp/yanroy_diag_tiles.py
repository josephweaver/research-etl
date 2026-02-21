# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from pathlib import Path
import csv
import re
import geopandas as gpd
from shapely.geometry import box

MODIS_XMIN = -20015109.354
MODIS_YMAX = 10007554.677
MODIS_TILE_SIZE = 1111950.5196666666
MODIS_H_MAX = 35
MODIS_V_MAX = 17
MODIS_SINU_WKT = "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext +units=m +no_defs"
TILE_RE = re.compile(r"(?i)h\d{2}v\d{2}")


def norm(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "").replace("_", "")


def parse_state_token(raw: str):
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


def read_state_codes(path: Path):
    fips, abbr = set(), set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        has_header = False
        if sample.strip():
            try:
                has_header = csv.Sniffer().has_header(sample)
            except csv.Error:
                has_header = False
        if has_header:
            rdr = csv.DictReader(f)
            for row in rdr:
                for k, raw in row.items():
                    key = norm(k)
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
                    ff, aa = parse_state_token(raw)
                    if ff:
                        fips.add(ff)
                    if aa:
                        abbr.add(aa)
    return fips, abbr


def read_tiles_from_raster_facts(path: Path):
    tiles = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            rel = str(row.get("relative_path") or "").strip()
            m = TILE_RE.search(rel)
            if m:
                tiles.add(m.group(0).lower())
    return tiles


def iter_modis_tiles():
    for h in range(0, MODIS_H_MAX + 1):
        for v in range(0, MODIS_V_MAX + 1):
            minx = MODIS_XMIN + (h * MODIS_TILE_SIZE)
            maxx = minx + MODIS_TILE_SIZE
            maxy = MODIS_YMAX - (v * MODIS_TILE_SIZE)
            miny = maxy - MODIS_TILE_SIZE
            yield h, v, minx, miny, maxx, maxy


states_csv = Path('states.of.interest.csv')
state_shp = Path('/mnt/scratch/weave151/data/tiger/state/extract/tl_2025_us_state.shp')
rf = Path('/mnt/scratch/weave151/data/yanroy/meta/raster_facts.csv')

fips, abbr = read_state_codes(states_csv)
gdf = gpd.read_file(state_shp)
mask = None
if 'STATEFP' in gdf.columns and fips:
    m = gdf['STATEFP'].astype(str).str.zfill(2).isin(fips)
    mask = m if mask is None else (mask | m)
if 'STUSPS' in gdf.columns and abbr:
    m = gdf['STUSPS'].astype(str).str.upper().isin(abbr)
    mask = m if mask is None else (mask | m)
gdf = gdf.loc[mask].copy()
gdf = gdf.to_crs(MODIS_SINU_WKT)
gdf = gdf.loc[gdf.geometry.notna()].copy()
try:
    gdf['geometry'] = gdf.geometry.make_valid()
except Exception:
    gdf['geometry'] = gdf.geometry.buffer(0)
gdf = gdf.loc[~gdf.geometry.is_empty].copy()
gdf['geometry'] = gdf.geometry.buffer(1.0)

all_tiles = []
for h, v, minx, miny, maxx, maxy in iter_modis_tiles():
    tid = f'h{h:02d}v{v:02d}'
    all_tiles.append({'tile_id': tid, 'h': h, 'v': v, 'geometry': box(minx, miny, maxx, maxy)})

tdf_all = gpd.GeoDataFrame(all_tiles, geometry='geometry', crs=MODIS_SINU_WKT)
joined_all = gpd.sjoin(
    gdf[['STATEFP', 'STUSPS', 'geometry']],
    tdf_all[['tile_id', 'h', 'v', 'geometry']],
    how='inner',
    predicate='intersects',
)

rf_tiles = read_tiles_from_raster_facts(rf)
tdf_rf = tdf_all[tdf_all['tile_id'].isin(rf_tiles)].copy()
joined_rf = gpd.sjoin(
    gdf[['STATEFP', 'STUSPS', 'geometry']],
    tdf_rf[['tile_id', 'h', 'v', 'geometry']],
    how='inner',
    predicate='intersects',
)

all_unique = sorted(set(str(t).lower() for t in joined_all['tile_id'].tolist()))
rf_unique = sorted(set(str(t).lower() for t in joined_rf['tile_id'].tolist()))
states = sorted(set(str(s).upper() for s in gdf['STUSPS'].astype(str).tolist()))

print('states_selected=', states)
print('state_count=', len(states))
print('all_intersections_rows=', len(joined_all), 'all_unique_tiles=', len(all_unique))
print('rf_intersections_rows=', len(joined_rf), 'rf_unique_tiles=', len(rf_unique))
print('rf_tiles=', ','.join(rf_unique))
missing = sorted(list(set(all_unique) - set(rf_unique)))
print('all_minus_rf_unique=', len(missing))
print('example_missing=', ','.join(missing[:25]))

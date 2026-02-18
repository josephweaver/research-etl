# Geo Plugins TODO

## Goal
Build a reusable geospatial ETL flow for Yanroy:
- download zip
- unzip 360 Yanroy rasters
- filter TIGER states by a variable list (9 states)
- filter 360 rasters by intersection with filtered states
- convert selected Yanroy rasters to polygons
- aggregate PRISM raster values by Yanroy polygons into tabular output

## Next Session Reminder

- [ ] Create/verify a dedicated geo plugin folder structure (for example `plugins/geo/`) and move/new geo plugins there.
- [ ] Update UI pipeline step plugin selector to hierarchical tree (`jsTree`) so plugins can be selected by folder/group instead of flat list.

## Pipeline Draft

- [ ] `pipelines/yanroy/geo_stage.yml`
  - [ ] Step `download_yanroy_zip`
  - [ ] Step `unzip_yanroy_rasters`
  - [ ] Step `scan_raster_facts` (bounds/crs manifest)
  - [ ] Step `select_states_of_interest` (TIGER filtered by `vars.states`)
  - [ ] Step `filter_rasters_by_states`
  - [ ] Step `raster_to_polygon` (79 rasters -> polygons)
  - [ ] Step `zonal_stats_prism_by_yanroy_poly`
  - [ ] Step `combine_outputs_to_tabular`

## Plugin Backlog

- [x] `plugins/geo_vector_filter.py`
  - [x] Input: vector file
  - [x] Params: `key`, `op` (`eq|ne|in|not_in`), `value/values`, optional `where`
  - [x] Output: filtered vector artifact
  - [x] Supports simple where syntax: `COL in (...)`, `COL == ...`, `COL != ...`

- [ ] `plugins/geo_filter_rasters_by_polygon.py`
  - [ ] Input: raster dir or raster facts csv
  - [ ] Input: selector polygon file
  - [ ] Logic: read raster bounds+crs, build footprints, intersect polygons
  - [ ] Output: selected rasters list + selected footprint table

- [ ] `plugins/raster_to_polygon.py`
  - [ ] Input: raster list/dir
  - [ ] Params: polygonization settings, dissolve options, connectivity
  - [ ] Output: polygon layer with `tile_id`, `field_id`

- [ ] `plugins/raster_zonal_stats.py`
  - [ ] Input: zone polygons (`tile_id`, `field_id`)
  - [ ] Input: PRISM raster (or time series)
  - [ ] Params: stats list (`mean`, `max`, `min`, `std`, `count`)
  - [ ] Output: tabular stats (`tile_id`, `field_id`, stat columns)

- [ ] `plugins/tabular_combine.py` (or reuse existing combine plugin)
  - [ ] Merge per-tile outputs to final long/wide table

## Data Contracts

- [ ] States list contract
  - [ ] Accept `STUSPS` list from vars/csv
  - [ ] Validate against TIGER values (fail on unknown code)

- [ ] Raster facts contract
  - [ ] Required columns: `relative_path`, `crs`, `bounds`
  - [ ] Bounds format: `minx,miny,maxx,maxy`

- [ ] Polygon zone contract
  - [ ] Required columns: `tile_id`, `field_id`, `geometry`
  - [ ] CRS required and explicit

- [ ] Zonal stats output contract
  - [ ] Required columns: `tile_id`, `field_id`, `source_raster`
  - [ ] One row per zone per source raster time slice

## Validation/QA

- [ ] Add a preflight validation step in pipeline
  - [ ] Confirm state list valid
  - [ ] Confirm CRS present for all rasters and polygons
  - [ ] Confirm non-empty intersection count before heavy steps

- [ ] Add threshold sanity checks
  - [ ] Selected raster count within expected range
  - [ ] Selected polygon count > 0

- [ ] Add smoke tests
  - [ ] Tiny fixture with 2 rasters + 1 polygon
  - [ ] Expected filtered count and zonal mean

## Implementation Order

- [ ] 1. Finalize `geo_filter_rasters_by_polygon` behavior and output schema
- [x] 2. Add vector feature filter (`geo_vector_filter`)
- [ ] 3. Add `raster_zonal_stats`
- [ ] 4. Wire `pipelines/yanroy/geo_stage.yml`
- [ ] 5. Add tests + docs

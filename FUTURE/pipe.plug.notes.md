# Pipeline + Plugin Work Notes

Last updated: 2026-02-09

## Goals
- Migrate legacy R workflows into ETL pipeline + plugin architecture.
- Prioritize `yanroy` pipeline first, then `prism`.
- Enforce reproducible execution and clear data lifecycle zones (`work`, `cache`, `publish`).

## Data Zones (target model)
- `work`:
  - Per-run scratch artifacts only.
  - Auto-cleanup policy (TTL).
- `cache`:
  - Shared/reusable inputs and intermediates.
  - Re-creatable if deleted.
  - Includes Google Drive sourced datasets.
- `publish`:
  - Durable final outputs.
  - Versioned, immutable release artifacts.
  - Final distribution target can be Google Drive.

## Cross-Cutting Tasks
- [ ] Define canonical directory variables in global config:
  - [ ] `global.work`
  - [ ] `global.cache`
  - [ ] `global.publish`
  - [ ] `global.bin` (optional tooling/scripts)
- [ ] Add plugin parameter conventions for `input_dir`, `output_dir`, and `dataset_version`.
- [ ] Add checksum manifest writing for publish outputs.
- [ ] Add retention/cleanup task for `work` zone.
- [ ] Add validation plugin template (row counts, schema checks, null checks, CRS checks).

## YanRoy (Priority 1)

### Plugin Creation Tasks
- [ ] `plugins/yanroy/stage_yanroy_raw_from_gdrive.py`
  - [ ] Wrap/replace `tools/gdrv/download.R` behavior.
  - [ ] Download raw YanRoy inputs from Google Drive into `cache/yanroy/raw/<version>/`.
  - [ ] Emit manifest of downloaded files.
- [ ] `plugins/yanroy/scan_raw_metadata.py`
  - [ ] Port `R/yanroy/01-extract-raw-meta.R`.
  - [ ] Produce `yanroy_raw_metadata.csv`.
  - [ ] Produce `tiles.of.interest.csv` from states list.
- [ ] `plugins/yanroy/reproject_tile.py`
  - [ ] Port `R/yanroy/02-raw-to-epsg5070.R`.
  - [ ] Input: tile coordinate.
  - [ ] Output: `*_5070.tif` (and optional `*_4269.tif`).
  - [ ] Fix known R script defects during port (`r5070` variable mismatch).
- [ ] `plugins/yanroy/extract_field_meta.py`
  - [ ] Port `R/yanroy/02-extract-field-meta.R`.
  - [ ] Output: tile metadata CSV + boundary raster.
  - [ ] Fix known R defects during port (`na.rm` typo).
- [ ] `plugins/yanroy/validate_tile_outputs.py`
  - [ ] Check expected files for each tile.
  - [ ] Verify non-empty outputs and required columns.

### Pipeline Tasks
- [ ] Create `pipelines/yanroy.yml`:
  - [ ] Step: stage raw dataset from Drive -> cache.
  - [ ] Step: scan metadata + tiles-of-interest.
  - [ ] Step: foreach tile -> reproject.
  - [ ] Step: foreach tile -> extract field metadata.
  - [ ] Step: validate outputs.
- [ ] Add execution config defaults for YanRoy pipeline (local + slurm env examples).
- [ ] Add dry-run test scenario for `yanroy.yml`.

### Testing Tasks
- [ ] Unit tests for each YanRoy plugin.
- [ ] Integration test for full YanRoy mini-run (1-2 tiles fixture).
- [ ] Parity check against current R output on a sample tile.

## PRISM (Priority 2)

### Plugin Creation Tasks
- [ ] `plugins/prism/download_unzip.py` (port `01-download-PRISM.R`, fix existence-check logic).
- [ ] `plugins/prism/reproject_5070.py` (port `02-reproject-5070-R`).
- [ ] `plugins/prism/extract_tile_monthly.py` (port `03-extract-tiles.R`).
- [ ] `plugins/prism/extract_normals.py` (merge/parameterize `0202` + `0203` scripts).
- [ ] `plugins/prism/combine_tile.py` (port `04-combine-tile.R`).
- [ ] `plugins/prism/validate_outputs.py`.

### Pipeline Tasks
- [ ] Create `pipelines/prism.yml` with:
  - [ ] staged source inputs in `cache`
  - [ ] extraction in `work`
  - [ ] combined outputs in `publish`
- [ ] Standardize CRS policy (prefer canonical 5070 for derived products).

### Testing Tasks
- [ ] Unit tests for each PRISM plugin.
- [ ] Integration test for one tile/year.
- [ ] Output parity spot-check vs R scripts.

## Documentation Tasks
- [ ] Add migration notes doc (`docs/migration_r_to_plugins.md`).
- [ ] Add runbook for data zones + retention policy.
- [ ] Add Google Drive staging/publish operational notes.

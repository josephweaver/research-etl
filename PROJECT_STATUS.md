# Project Status (2026-04-07)

## What this file is for

Use this file for active execution status:

- what we are working on now
- what is currently working
- what is currently blocked
- what the next execution steps are

`README.md` defines scope and operating rules.
`FUTURE.md` holds deferred work.

## Current delivery goal

Produce crop-insurance datasets and landcore datasets reliably enough to trust and repeat the runs.

Current engineering focus is limited to the `needed-now core` areas:

- pipeline definition
- runtime context
- resource resolution
- step execution contract
- artifact tracking
- validation
- logging/provenance

## Current focus

- LandCore risk-model execution path on HPCC
- county-neighborhood model-input generation in `../landcore-etl-pipelines`
- chunked county fit orchestration through the lightweight `controler/`
- active stabilization work is runtime path correctness, ETL child execution, SLURM submission flow, and resumable county fit handoff

## What is currently working

- local execution modes are working for shared pipeline asset resolution
- immutable source layout is working locally with sibling checkouts under `.out/src`
- runtime context and execution-mode cleanup has improved consistency between local modes
- project pipeline assets can resolve through configured project sources instead of only repo-local paths
- lightweight external controller scaffold exists under `controler/`
- controller supports:
  - `status`
  - `doctor`
  - `preview`
  - `run-item`
  - `run-once`
- controller worker mode `etl_pipeline` now launches the ETL child pipeline instead of a raw shell worker command
- ETL child pipeline exists at `pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml`
- child pipeline now resolves the R script from the LandCore pipeline assets repo at `{sys.projectdir}/scripts/model/Neighborhood_fit_chkptstanr.R`
- child pipeline now matches the checkpoint R script interface:
  - `data_csv`
  - `checkpoint_dir`
  - `iter_warmup`
  - `iter_sampling`
  - `iter_per_chkpt`
  - `chains`
  - `seed`
  - `stop_after`
  - `reset`
- in `../landcore-etl-pipelines`, the tillage model-input pipeline now writes county-neighborhood files plus a manifest instead of one giant CSV

## Current blockers / risks

- HPCC runtime pathing and pipeline asset checkout freshness still need direct verification on real runs
- remote execution is not yet proven end-to-end for the actual landcore risk-model county fit flow
- controller assumptions still rely on checkpoint files and logs being directly visible to the controller process
- controller has not yet been wired to auto-discover county input CSVs from the county-neighborhood manifest
- the in-flight model-input rebuild still needs validation once processing completes
- there is still risk of drifting into architecture cleanup unless the change clearly serves one of the seven core areas

## Next execution steps

- let the current LandCore tillage neighborhood-input rebuild finish on HPCC
- verify outputs from `../landcore-etl-pipelines/pipelines/risk_model/tillage_covariates_model_input.yml`:
  - `data/risk_model/model_input/county_model_input/<FIPS>/county_data.csv`
  - `data/risk_model/model_input/county_model_input/<FIPS>/county_data.summary.json`
  - `data/risk_model/model_input/county_model_input_manifest.csv`
- update the ETL child pipeline to accept direct `county_input_csv` overrides if controller-side manifest-driven submission is needed
- test one county manually through:
  - `python -m controler.main preview --config controler/config.yml --fips <FIPS>`
  - then the rendered `etl run ... pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml ...`
- once one county works, test one controller wave with `python -m controler.main run-once --config controler/config.yml`
- confirm checkpoint JSON path, log path field names, and completion/resume log markers on HPCC
- treat failures as concrete runtime-context, path-resolution, validation, or provenance defects before doing broader cleanup

## Executor strategy

- Use `hpcc_direct` first to debug remote runtime context, path resolution, pipeline asset resolution, and provenance quickly.
- Once `hpcc_direct` is working for the target pipelines, move to `slurm` and fix any scheduler-specific issues.
- Do not treat `hpcc_direct` and `slurm` as separate product tracks. The intent is for `hpcc_direct` to de-risk the same remote execution model that `slurm` will use.

## Recent progress

### 2026-04-07

- added a lightweight external controller under `controler/` for chunked county-fit resubmission
- controller uses ETL execution environment config and `SlurmProvisioner`
- controller supports diagnostics:
  - `doctor`
  - `preview`
- controller worker mode now targets ETL child runs rather than raw shell workers
- added ETL child pipeline:
  - `pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml`
- child pipeline now resolves the checkpointing R script from the LandCore assets repo copy:
  - `../landcore-etl-pipelines/scripts/model/Neighborhood_fit_chkptstanr.R`
- updated live controller config so it no longer overrides `fit_script_path` back to the old HPCC file location
- in `../landcore-etl-pipelines`:
  - `pipelines/yanroy/field_fips.yml` now uses canonical `tile_field_id`
  - `pipelines/risk_model/tillage_covariates_model_input.yml` now filters to years `2005` to `2016`
  - `assets/county_adjacency2010.csv` is now used to write per-county neighborhood model inputs instead of one giant file
- current in-flight HPCC run is rebuilding the tillage neighborhood inputs

### 2026-03-26

- Lobell corn field-year stabilization advanced:
  - replaced `geo_county_raster_aggregate` with `geo/raster_aggregate_by_polygon`
  - plugin now supports:
    - `include_empty_polygons`
    - finite-value filtering for raster `NaN` pixels before aggregation
    - structured `columns` specs with `source`, `name`, `type`, optional `format`
    - literal columns via `source: literal` + `value`
  - `combine_files` CSV path was changed to stream rows one file at a time instead of buffering all rows in memory
  - Lobell normalizer was updated to accept polygon-oriented aggregate outputs
- current Lobell verification checkpoint:
  - step 1 (`combine_corn_rasters_by_year`) verified
  - step 2 (`aggregate_corn_fields_by_tile_year`) verified
  - step 3 (`combine_corn_field_year_csvs`) verified
  - step 4 (`normalize_corn_field_year`) still needs direct verification
  - current narrowed debug scope remains `h18v05` and `2016`

### 2026-03-14

- clarified project document roles:
  - `README.md` = scope and operating rules
  - `PROJECT_STATUS.md` = active work and blockers
  - `FUTURE.md` = deferred work
- moved deferred planning and architecture notes into `FUTURE/`
- tightened the root docs so active delivery work is easier to distinguish from future backlog

### 2026-03-13

- crop-insurance query workspace and dataset registration integration advanced:
  - `dataset_store` now captures inferred schema/profile and updates `schema_hash` on dataset versions
  - query workspace API/UI can discover and merge tables across configured local `pipeline_asset_sources`
  - workspace partial manifest model is active under `db/duckdb/workspaces/*.yml`
- crop-insurance USDA-RMA pipeline updates:
  - download steps use conditional HTTP checks
  - registration steps pass `project_id: crop_insurance`
  - workspace auto-register and git commit/push controls were added
- new blocker was observed:
  - workspace git commit/push resolved a repo path without the expected hashed checkout suffix
  - pipeline asset checkout appeared stale relative to the expected pipeline revision

### 2026-02-23

- landcore SSURGO and YanRoy integration advanced in `landcore-etl-pipelines`
- SSURGO download pipelines were reintroduced
- HTTP download plugin gained explicit `out_file` support for unstable source filenames

## Background system status

- core pipeline engine supports `when`, `foreach`, `parallel_with`, retries, and resume
- execution backends include `local`, `slurm`, and `hpcc_direct`
- provenance, artifact registration, and validation/tracking are implemented at a usable baseline
- web UI/API exists, but wider UI expansion is not current priority
- this codebase is still pre-1.0 and should be treated as stabilizing

## Working rule

If a task does not directly improve crop-insurance or landcore delivery through one of the seven `needed-now core` areas, move it to `FUTURE.md` instead of doing it now.

# Project Status (2026-03-14)

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

- `sample.yml` is running in both `local` and `local-dev`
- next execution target is HPCC and `hpcc_direct`
- active stabilization work is immutable source checkout layout, pipeline asset resolution, runtime path correctness, validation, and logging/provenance

## What is currently working

- local execution modes are working for shared pipeline asset resolution
- immutable source layout is working locally with sibling checkouts under `.out/src`
- runtime context and execution-mode cleanup has improved consistency between local modes
- project pipeline assets can resolve through configured project sources instead of only repo-local paths

## Current blockers / risks

- HPCC runtime pathing and pipeline asset checkout freshness still need direct verification on real runs
- remote workspace auto-commit/push behavior may still resolve the wrong repo path or a stale checkout in crop-insurance flows
- remote execution is not yet proven end-to-end for the actual crop-insurance and landcore dataset pipelines
- there is still risk of drifting into architecture cleanup unless the change clearly serves one of the seven core areas

## Next execution steps

- run `sample.yml` on `hpcc_msu` and `hpcc_msu_direct` and verify source checkout, working paths, and logs
- run the next real crop-insurance pipeline on HPCC and confirm artifacts land in the intended output locations
- run the next real landcore pipeline on HPCC and confirm the same
- treat failures as concrete runtime-context, path-resolution, validation, or provenance defects before doing broader cleanup

## Executor strategy

- Use `hpcc_direct` first to debug remote runtime context, path resolution, pipeline asset resolution, and provenance quickly.
- Once `hpcc_direct` is working for the target pipelines, move to `slurm` and fix any scheduler-specific issues.
- Do not treat `hpcc_direct` and `slurm` as separate product tracks. The intent is for `hpcc_direct` to de-risk the same remote execution model that `slurm` will use.

## Recent progress

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

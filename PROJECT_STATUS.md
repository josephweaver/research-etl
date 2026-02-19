# Project Status (2026-02-18)

## Current state
- CLI/package baseline is in place (`pyproject.toml`, `etl` entrypoint, editable install flow, GitHub Actions tests).
- Core pipeline engine supports `when`, `foreach`, `parallel_with`, retries, and resume.
- Execution backends:
  - `local` executor (`etl/executors/local.py`)
  - `slurm` executor (`etl/executors/slurm.py`) with setup + dependent batch/array planning and SSH submission support.
  - `hpcc_direct` executor (`etl/executors/hpcc_direct.py`) for direct SSH dev-node execution of `etl.run_batch`.
- Strict git-pinned execution is implemented for both local and slurm execution paths.
- Pipeline dependencies are supported via `requires_pipelines` with auto-run of missing successful prerequisites and cycle detection.
- Hierarchical iterative variable resolution is active in pipeline parsing:
  - `global.*` + flat globals
  - `env.*` + flat env overrides (from execution config env)
  - `project.*` + flat project vars (from `config/projects.yml`, selected by resolved `project_id`)
  - `pipe.*` + flat pipeline overrides
- Resolver depth guard is configurable and unified:
  - `resolve_max_passes` supported from global/env config (default `20`, clamped `1..100`)
  - used by parser, builder preview/test-step, and runtime runner resolution
  - builder namespace now exposes `resolution.max_passes`, `resolution.passes_used`, `resolution.stable`
- Directory resolution hardening:
  - fixed self-recursive `dirs.workdir` growth (`{workdir}/...`)
  - fixed sibling `dirs.*` precedence so derived dirs (for example `cachedir`) bind to resolved `dirs.workdir`
- Workdir/logging path correctness:
  - builder step-test no longer creates unresolved template directories like `{env.workdir}/...`
  - unresolved run payload workdir values are ignored in favor of resolved precedence
  - step logs now write under configured `dirs.logdir` (when present)
- Builder variable/validation improvements:
  - builder validation now supports unresolved references to prior step outputs via `output_var` placeholders
  - builder namespace includes output-var placeholders for resolve/preview behavior
- Archive extraction hardening:
  - `archive_extract` now supports `include_glob` for selective extraction
  - improved archive discovery for exact-path and glob inputs
  - improved failure diagnostics in step logs (missing `7z`, command stderr/stdout)
- New filesystem transform plugins:
  - `plugins/file_move_regex.py`
  - `plugins/file_delete_regex.py`
- New orchestration/util plugins:
  - `plugins/combine_files.py` (csv/json/yaml/xml/text merge)
  - `plugins/exec_script.py`
  - `plugins/gdrive_upload.py`
- Yanroy pipeline updates:
  - Added `pipelines/yanroy/tiles_of_interest.yml` as a separate stage pipeline.
  - Moved tile builder script to `scripts/yanroy/build_tiles_of_interest.py`.
  - `pipelines/yanroy/extract_fields.yml` now runs `raster_facts.py` via `foreach: tiles` and supports parallel combine steps.
  - Debugged HPCC step-2 failures in `tiles_of_interest`:
    - fixed `states.of.interest.csv` parsing for simple one-value-per-line files in `scripts/yanroy/build_tiles_of_interest_from_facts.py`,
    - confirmed HPCC run success through steps 01-03 after parser/dependency fixes.
- HPCC direct execution hardening:
  - fresh remote checkout each run (no reuse of stale checkout directories),
  - UTF-8 subprocess decode with replacement for Windows launcher stability,
  - richer failure diagnostics by surfacing both remote stdout and stderr,
  - streamed SSH stage output now emits line-by-line logs during execution (Popen-based streaming path),
  - remote `run_batch` now runs unbuffered (`python -u`, `PYTHONUNBUFFERED=1`) for faster live log flush,
  - runtime checks `import etl.run_batch` and installs editable package remotely when missing (`pip install --no-deps -e .`),
  - execution env DB mode/verbosity are exported as `ETL_DB_MODE`/`ETL_DB_VERBOSE` before invoking `run_batch` (prevents unintended DB connection hangs when secrets file defines `ETL_DATABASE_URL`).
- Dependency stabilization for HPCC geospatial steps:
  - `requirements.txt` includes `geopandas`, `python-dateutil`, and `requests`,
  - `hpcc_direct` requirement install now uses `--ignore-installed` to avoid cluster site-package leakage.
- Runner telemetry update:
  - per-attempt fallback resource metrics now capture CPU/memory usage even when plugins do not emit them.
- SLURM/web execution updates:
  - setup job default walltime is `00:10:00` (new `setup_time` override),
  - verbose SLURM scripts now emit safe stage logs without exposing secrets,
  - `run_batch.py` gained `--verbose` and is wired from SLURM verbose mode,
  - SLURM log path precedence now honors pipeline `dirs.logdir` over env `logdir`.
- Web UI updates:
  - Added Plugins page/navigation and plugin stats route integration.
- SLURM batch execution uses `etl/run_batch.py` and emits event-driven status transitions (`batch_started`, `batch_completed`, `batch_failed`, `run_completed`).
- DB migration bootstrap is active (`etl/db.py`, `db/ddl/*.sql`) with checksum/version enforcement.
- Artifact policy/registry baseline is active:
  - metadata-only artifact tables (`etl_artifacts`, `etl_artifact_locations`)
  - policy config support via `config/artifacts.yml` (starter: `config/artifacts.example.yml`)
  - CLI enforcement command: `etl artifacts enforce [--dry-run]`
  - auto-registration from run artifacts and step outputs (local + slurm run_batch), including explicit publish descriptors via `_artifacts`.
  - root-bound registration checks (`locations.*.root_uri` / `root_path`) and class `allowed_location_types`, with violations recorded as `policy_violation`.
- Tracking is persisted to JSONL and DB (when `ETL_DATABASE_URL` is set):
  - `etl_runs`
  - `etl_run_steps`
  - `etl_run_step_attempts`
  - `etl_run_events`
- Project partitioning + access scaffolding is active:
  - `project_id` stamped on runs/validations/artifacts.
  - New DB objects: `etl_projects`, `etl_user_projects`, `etl_users`.
  - Seeded memberships:
    - `land-core` -> `land_core`
    - `gee-lee` -> `gee_lee`
    - `admin` -> `land_core`, `gee_lee`
- Provenance is persisted on all run paths (local/slurm/run_batch/resume):
  - git fields, including `git_origin_url` and `git_repo_name`
  - CLI command
  - pipeline/config/plugin checksums.
- Diagnostics:
  - failure reports written to `.runs/error_reports/*.json`
  - `etl diagnostics latest [--show]` available.
- New plugin: `plugins/gdrive_download.py` wraps `tools/gdrv/download.R` for pipeline-based Google Drive staging.
- YanRoy pipeline split is scaffolded:
  - `pipelines/yanroy_base.yml` (dependency stage: gdrive raw staging)
  - `pipelines/yanroy.yml` (main pipeline requiring base)
- New geospatial filter plugin:
  - `plugins/geo_vector_filter.py` filters vector features by attribute and writes a new vector output.
  - Supports either:
    - explicit args: `key` + `op` (`eq|ne|in|not_in`) + `value/values`,
    - lightweight `where` expressions: `COL in (...)`, `COL == ...`, `COL != ...`.
  - Designed for TIGER state filtering use cases (for example `STUSPS in (...)`) as a reusable pre-step.

## Web UI/API status
- Web server: `etl web --host 127.0.0.1 --port 8000 --reload`
- Compact navigation bar with primary routes:
  - `Operations` (`/`)
  - `Pipelines` (`/pipelines`)
  - `New Pipeline` (`/pipelines/new`)
  - quick Live Run jump (`/runs/{run_id}/live`)
- Operations dashboard (`/`): failed/running triage + quick resume/view.
- Pipeline catalog (`/pipelines`) and pipeline detail (`/pipelines/{pipeline_id}`) are live.
- Live run view (`/runs/{run_id}/live`) is live with event timeline/active attempt summary.
- Artifact tree + file viewer are live (`/api/runs/{run_id}/files`, `/api/runs/{run_id}/file`).
- Builder routes are live:
  - `/pipelines/new`
  - `/pipelines/{pipeline_id}/edit`
- Builder APIs are live:
  - `GET /api/builder/source`
  - `POST /api/builder/validate`
  - `POST /api/builder/test-step`
  - `POST /api/builder/generate` (OpenAI-backed with one-pass auto-repair)
- Pipeline draft persistence APIs are live:
  - `POST /api/pipelines`
  - `PUT /api/pipelines/{pipeline_id}`
- API access scoping is active (service-mode scaffold):
  - user scope via `X-ETL-User` header or `as_user` query param
  - project checks enforced on run/pipeline list/detail/action routes
  - optional `project_id` query filtering for runs/pipelines/validations
- Top-nav user selector is live in web UI:
  - `admin`
  - `land-core`
  - `gee-lee`
  - selection persists in browser local storage and auto-applies to API calls.

## Test status
- Current focused regression runs (2026-02-13):
  - `tests/test_web_api.py -k "builder_namespace or builder_test_step"` -> `10 passed`
  - `tests/test_pipeline_resolution.py tests/test_runner_sys_vars.py` -> `18 passed`
  - `tests/test_plugin_archive_extract.py` -> passed
  - `tests/test_plugin_file_move_regex.py` -> passed
  - `tests/test_plugin_file_delete_regex.py` -> passed
  - additional targeted resolver/workdir/logging suites passed during this update cycle.
  - New plugin tests added: `tests/test_plugin_geo_vector_filter.py` (attribute filter behavior and validation).
    - Note: currently skips in local `.venv` if `geopandas`/`shapely` are unavailable.
- Coverage includes:
  - DB migration bootstrap
  - tracking write paths
  - SLURM event transitions (success + failed)
  - resume from partial success
  - retry attempts with `attempt_no > 1`
  - run_batch event payload checks
  - web route/API coverage for operations, pipeline detail, live run, builder, and draft save/update flows.

## Known gaps / risks
- Web resume currently supports `local` executor only (SLURM resume still via CLI).
- SLURM artifact browsing supports only local-visible paths; remote-only cluster paths still need SSH-based retrieval.
- No central reconciliation loop for missing/late SLURM events.
- Current access model is a lightweight scaffold (`X-ETL-User`/`as_user`); no real authentication provider/session/JWT yet.
- Builder persistence is file-backed; DB-backed draft/version history is not implemented.
- AI draft generation currently uses a single repair pass and no schema-constrained decoding.
- Config/pipeline/plugin catalog tables are not yet populated by runtime snapshots.
- Artifact class/location inference is heuristic when plugins do not emit explicit `_artifacts` descriptors.
- `--allow-dirty-git` behavior for remote executors is not complete yet:
  - current remote runs are commit-pinned; dirty local workspace changes are not propagated to remote checkout.

## Suggested next steps
1) Add SSH-backed remote artifact retrieval for SLURM paths in web API.
2) Add DB-backed pipeline draft/version model.
3) Add auth guard for web UI if multi-user exposure is planned.
4) Persist config/catalog snapshots into DB catalog tables at run start.
5) Add offline event buffering strategy for runs executed without DB connectivity.
6) Improve AI generation with stronger constrained output schema and optional additional repair retries.
7) Add remote dirty-overlay support for `--allow-dirty-git`: checkout pinned commit on remote, then apply local dirty files over that checkout before execution.
8) Wire `geo_vector_filter.py` into `pipelines/yanroy/tiles_of_interest.yml` before tile intersection logic (state prefilter stage).

## Possible future features

- Dynamic chained fan-out from prior fan-out outputs:
  - Allow a step to expand from a prior step's `foreach` outputs (for example `foreach_from: <prior_output_list>`), including optional nested fan-out patterns.
  - Primary use case: geospatial multi-stage chaining where each selected raster/file from stage N drives stage N+1 work.
  - Constraints for safe implementation:
    - deterministic expansion manifests persisted before execution,
    - bounded expansion (`max_items`, `max_depth`, `max_concurrency`),
    - stable child step instance IDs for resume/retry,
    - clear parent-child dependency mapping to avoid ambiguous cross-links.
  - Risk profile:
    - unbounded job explosion with nested `foreach` + `parallel_with`,
    - harder retry/resume semantics when dynamic expansion is not persisted,
    - scheduler pressure (SLURM array/job limits) and reduced debuggability without strict caps.

- Resolved dynamic execution plans (materialized before run):
  - Current model is mostly static (predefined vars/list-based `foreach`), with `foreach_glob` planned but not fully functional.
  - Future model should support dynamic discovery while preserving reproducibility:
    - discovery steps emit explicit manifests (item lists + metadata),
    - engine resolves and freezes the expanded DAG from those manifests before executing dependent stages,
    - frozen plan is persisted with stable step-instance IDs for audit, resume, and retry.
  - UX/API implications:
    - add a "plan resolution" phase/status before execution starts,
    - expose planned fan-out counts and cap violations early in builder/validation.

- Adaptive SLURM execution packing from historical runtime telemetry:
  - After enough run history is recorded, estimate per-step/per-item durations and pack work into configurable target walltime blocks (for example `target_block_minutes`).
  - Convert many short tasks into fewer scheduler-friendly jobs:
    - sequential micro-steps can be merged into one block job,
    - large parallel fan-outs can be repacked into fewer longer-running chunks (for example `100 x ~1 minute` into `4 x ~25 minute` jobs).
  - Goals:
    - reduce scheduler overhead and queue pressure,
    - improve cluster throughput and job-start efficiency,
    - keep walltime requests closer to real usage.
  - Guardrails:
    - preserve deterministic step ordering/dependencies,
    - enforce max block duration/item count limits,
    - support fallback to original un-packed plan when estimates are low-confidence.

## Quick commands
- Install/editable dev env: `python -m pip install -e ".[dev]"`
- Validate pipeline: `etl validate pipelines/sample.yml`
- Run local success: `etl run pipelines/sample.yml --executor local`
- Run local failure: `etl run pipelines/sample_fail.yml --executor local`
- Run SLURM dry-run: `etl run pipelines/sample_parallel.yml --executor slurm --dry-run`
- List runs: `etl runs list`
- Show run: `etl runs show <run_id>`
- Latest diagnostics: `etl diagnostics latest --show`
- Enforce artifact policy (dry-run): `etl artifacts enforce --dry-run --config config/artifacts.yml`
- Start web UI: `etl web --host 127.0.0.1 --port 8000 --reload`
- Open builder: `http://127.0.0.1:8000/pipelines/new`
- Test suite: `python -m pytest -q`


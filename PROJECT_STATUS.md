# Project Status (2026-02-13)

## Current state
- CLI/package baseline is in place (`pyproject.toml`, `etl` entrypoint, editable install flow, GitHub Actions tests).
- Core pipeline engine supports `when`, `foreach`, `parallel_with`, retries, and resume.
- Execution backends:
  - `local` executor (`etl/executors/local.py`)
  - `slurm` executor (`etl/executors/slurm.py`) with setup + dependent batch/array planning and SSH submission support.
- Strict git-pinned execution is implemented for both local and slurm execution paths.
- Pipeline dependencies are supported via `requires_pipelines` with auto-run of missing successful prerequisites and cycle detection.
- Hierarchical iterative variable resolution is active in pipeline parsing:
  - `global.*` + flat globals
  - `env.*` + flat env overrides (from execution config env)
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
  - additional targeted resolver/workdir/logging suites passed during this update cycle.
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

## Suggested next steps
1) Add SSH-backed remote artifact retrieval for SLURM paths in web API.
2) Add DB-backed pipeline draft/version model.
3) Add auth guard for web UI if multi-user exposure is planned.
4) Persist config/catalog snapshots into DB catalog tables at run start.
5) Add offline event buffering strategy for runs executed without DB connectivity.
6) Improve AI generation with stronger constrained output schema and optional additional repair retries.

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


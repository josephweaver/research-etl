# Project Status (2026-02-10)

## Current state
- CLI/package baseline is in place (`pyproject.toml`, `etl` entrypoint, editable install flow, GitHub Actions tests).
- Core pipeline engine supports `when`, `foreach`, `parallel_with`, retries, and resume.
- Execution backends:
  - `local` executor (`etl/executors/local.py`)
  - `slurm` executor (`etl/executors/slurm.py`) with setup + dependent batch/array planning and SSH submission support.
- Strict git-pinned execution is implemented for both local and slurm execution paths.
- SLURM batch execution uses `etl/run_batch.py` and emits event-driven status transitions (`batch_started`, `batch_completed`, `batch_failed`, `run_completed`).
- DB migration bootstrap is active (`etl/db.py`, `db/ddl/*.sql`) with checksum/version enforcement.
- Tracking is persisted to JSONL and DB (when `ETL_DATABASE_URL` is set):
  - `etl_runs`
  - `etl_run_steps`
  - `etl_run_step_attempts`
  - `etl_run_events`
- Provenance is persisted on all run paths (local/slurm/run_batch/resume):
  - git fields, including `git_origin_url` and `git_repo_name`
  - CLI command
  - pipeline/config/plugin checksums.
- Diagnostics:
  - failure reports written to `.runs/error_reports/*.json`
  - `etl diagnostics latest [--show]` available.

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

## Test status
- Current local run: `69 passed` (`python -m pytest -q`).
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
- No auth/authorization on web UI (dev/local usage model currently).
- Builder persistence is file-backed; DB-backed draft/version history is not implemented.
- AI draft generation currently uses a single repair pass and no schema-constrained decoding.
- Config/pipeline/plugin catalog tables are not yet populated by runtime snapshots.

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
- Start web UI: `etl web --host 127.0.0.1 --port 8000 --reload`
- Open builder: `http://127.0.0.1:8000/pipelines/new`
- Test suite: `python -m pytest -q`

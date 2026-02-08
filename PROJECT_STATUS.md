# Project Status (2026-02-08)

## Current state
- CLI/package baseline is in place (`pyproject.toml`, `etl` entrypoint, editable install flow, GitHub Actions tests).
- Core pipeline engine supports `when`, `foreach`, `parallel_with`, retries, and resume.
- Execution backends:
  - `local` executor (`etl/executors/local.py`)
  - `slurm` executor (`etl/executors/slurm.py`) with setup + dependent batch/array planning and SSH submission support.
- SLURM batch execution uses `etl/run_batch.py` and emits event-driven status transitions (`batch_started`, `batch_completed`, `batch_failed`, `run_completed`).
- DB migration bootstrap is active (`etl/db.py`, `db/ddl/*.sql`) with checksum/version enforcement.
- Tracking is persisted to JSONL and DB (when `ETL_DATABASE_URL` is set):
  - `etl_runs`
  - `etl_run_steps`
  - `etl_run_step_attempts`
  - `etl_run_events`
- Provenance is persisted on all run paths (local/slurm/run_batch/resume):
  - git fields, CLI command, pipeline/config checksums, plugin checksums.
- Diagnostics:
  - failure reports written to `.runs/error_reports/*.json`
  - `etl diagnostics latest [--show]` available.
- Web UI/API is scaffolded and functional:
  - `etl web --host 127.0.0.1 --port 8000 --reload`
  - list/detail views, filters, resume action (local-only), artifact tree + text file viewer.

## Recent additions
- Standardized plugin logging + per-step artifact logs:
  - plugins can call `ctx.log(...)`, `ctx.info(...)`, `ctx.warn(...)`, `ctx.error(...)`
  - runner writes `<step_workdir>/step.log`.
- Executor-owned artifact retrieval hooks:
  - `artifact_tree(...)`
  - `artifact_file(...)`
  - web API now routes artifact browsing through executor behavior.
- Test fixtures for controlled failure:
  - `plugins/fail_always.py`
  - `pipelines/sample_fail.yml`

## Test status
- Current local run: `38 passed, 6 skipped` (`python -m pytest -q`).
- Integration coverage includes:
  - DB migration bootstrap
  - tracking write paths
  - SLURM event transitions (success + failed)
  - resume from partial success
  - retry attempts with `attempt_no > 1`
  - run_batch event payload checks

## Known gaps / risks
- Web resume currently supports `local` executor only (SLURM resume still via CLI).
- SLURM artifact browsing supports only paths visible to the local UI host; remote-only cluster paths need SSH-based retrieval.
- No central reconciliation loop for missing/late SLURM events.
- No auth/authorization on web UI (dev/local usage model currently).
- Config/pipeline/plugin catalog tables are not yet populated by runtime snapshots.
- Some pycache artifacts may appear during local runs/tests.

## Suggested next steps
1) Add SSH-backed remote artifact retrieval for SLURM paths in web API.
2) Add web-side “run action” (validate/run/resume) with explicit executor config form.
3) Add auth guard for web UI if multi-user exposure is planned.
4) Persist config/catalog snapshots into DB catalog tables at run start.
5) Add offline event buffering strategy for runs executed without DB connectivity.

## Quick commands
- Install/editable dev env: `python -m pip install -e ".[dev]"`
- Validate pipeline: `etl validate pipelines/sample.yml`
- Run local success: `etl run pipelines/sample.yml --executor local`
- Run local failure: `etl run pipelines/sample_fail.yml --executor local`
- Run SLURM dry-run (queue-style records): `etl run pipelines/sample_parallel.yml --executor slurm --dry-run`
- List runs: `etl runs list`
- Show run: `etl runs show <run_id>`
- Latest diagnostics: `etl diagnostics latest --show`
- Start web UI: `etl web --host 127.0.0.1 --port 8000 --reload`
- Test suite: `python -m pytest -q`

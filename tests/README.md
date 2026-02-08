# Test Catalog

This folder contains the automated test suite for the ETL project.

## Test files

- `tests/test_db_bootstrap.py`
  - Verifies database migration bootstrap behavior in `etl/db.py`.
  - Confirms new `.sql` migrations are applied in order.
  - Confirms checksum drift on an already-applied migration raises an error.

- `tests/test_tracking_db_writes.py`
  - Verifies tracking writes expected SQL statements for run-level and step-level persistence.
  - Covers JSONL + DB write flow for `record_run(...)`.
  - Verifies `upsert_step_attempt(...)` writes both summary (`etl_run_steps`) and attempt (`etl_run_step_attempts`) records.

- `tests/test_db_integration_postgres.py`
  - Real Postgres integration tests (marked `integration`).
  - Verifies migration bootstrap against a real DB.
  - Verifies run/step/attempt/event writes in actual database tables.
  - Verifies SLURM event transitions persisted through DB (`run_queued -> batch_started -> batch_completed -> run_completed`, and failed path with `batch_failed`).
  - Verifies local resume from partial success persists skipped/success states correctly (`s1` skipped, `s2` executed).
  - Verifies retry persistence with multiple attempts (`attempt_no` 1 then 2) in `etl_run_step_attempts`.
  - Skips automatically when `ETL_DATABASE_URL` is not set.

- `tests/test_runner_paths.py`
  - Verifies run artifact paths use timestamp-first layout:
    - `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/...`
  - Confirms step workdir creation under the run artifact directory.

- `tests/test_runner_retries.py`
  - Verifies retry behavior in `etl/runner.py`.
  - Covers success after retries and failure after max retries is exhausted.
  - Verifies per-step attempt history and final attempt count.

- `tests/test_runner_resume.py`
  - Verifies resume behavior in `etl/runner.py`.
  - Confirms previously successful steps can be skipped.
  - Confirms prior outputs are injected so downstream `when` conditions still evaluate correctly.

- `tests/test_runner_edge_cases.py`
  - Verifies retry + `foreach` + `parallel_with` interaction in `etl/runner.py`.
  - Confirms expanded foreach steps retry independently and succeed.
  - Confirms resume skips expanded foreach step names (`fan_0`, `fan_1`) while downstream steps continue using prior outputs.

- `tests/test_plugin_logging.py`
  - Verifies standardized plugin logging calls (`ctx.log`, `ctx.info`, `ctx.warn`, `ctx.error`) work.
  - Verifies per-step `step.log` artifact files are written.

- `tests/test_provenance.py`
  - Verifies provenance collection payload shape from `etl/provenance.py`.
  - Confirms pipeline and plugin checksums are captured.

- `tests/test_provenance_paths.py`
  - Verifies `cli.py run` passes provenance into both Local and SLURM executor submission contexts.
  - Verifies resume path (`--resume-run-id`) still carries provenance metadata.

- `tests/test_slurm_executor.py`
  - Verifies SLURM submission planning for parallel batches (array job generation).
  - Confirms chained dependencies between setup and subsequent jobs.
  - Confirms `--resume-run-id`, `--max-retries`, and `--retry-delay-seconds` are propagated to `run_batch.py` commands.

- `tests/test_run_batch_events.py`
  - Executes `etl/run_batch.py` via `main(...)` and validates tracking event emissions.
  - Confirms success path emits `batch_completed` and final `run_completed` with correct `step_attempts` payload.
  - Confirms failure path emits `batch_failed` with final error details in `step_attempts`.
  - Confirms batch/run status updates include provenance metadata.

- `tests/test_error_messages.py`
  - Verifies CLI error classification returns actionable messages for common failures.
  - Covers resume-without-DB, missing `sbatch`, and remote secret bootstrap SLURM errors.
  - Verifies negative retry argument validation and execution env override integer validation.

- `tests/test_diagnostics.py`
  - Verifies portable diagnostic reports are written with traceback and frame metadata.
  - Verifies `cli.py` prints the saved diagnostic report path when a run submission fails.
  - Verifies diagnostic discovery via `find_latest_error_report(...)` and `etl diagnostics latest`.

- `tests/test_web_queries.py`
  - Verifies web-query helpers return run list/detail payloads from tracking tables.
  - Verifies query-side filtering (`status`, `executor`, `q`) is applied.
  - Verifies missing DB configuration raises a clear `WebQueryError`.

- `tests/test_web_api.py`
  - Verifies FastAPI UI API endpoints for `/api/runs` and `/api/runs/{run_id}`.
  - Verifies resume endpoint behavior (`POST /api/runs/{run_id}/resume`, local-only guard).
  - Skips when FastAPI dependencies are not importable.

## Notes

- Tests are executed with `pytest`.
- Run all tests from repo root:
  - `python -m pytest -q`
- Run only fast/unit tests:
  - `python -m pytest -q -m "not integration"`
- Run only integration tests (requires `ETL_DATABASE_URL`):
  - `python -m pytest -q -m "integration"`

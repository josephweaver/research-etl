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

## Notes

- Tests are executed with `pytest`.
- Run all tests from repo root:
  - `python -m pytest -q`
- Run only fast/unit tests:
  - `python -m pytest -q -m "not integration"`
- Run only integration tests (requires `ETL_DATABASE_URL`):
  - `python -m pytest -q -m "integration"`

# Project Status (2026-02-08)

## What's implemented
- Plugin contract and loader (`etl/plugins/base.py`) with metadata + optional validate/cleanup hooks.
- Pipeline parsing/templating/validation (`etl/pipeline.py`) with `vars`, `dirs`, `when`, `foreach`, and `parallel_with`.
- Local runner (`etl/runner.py`) with script parsing, param coercion, per-step env, per-step workdirs, and timestamped artifact paths:
  - `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/<step_name>/`
- Local executor (`etl/executors/local.py`) with run tracking.
- SLURM executor (`etl/executors/slurm.py`) with setup + chained batch/array submission and SSH remote submission support.
- Remote secret bootstrap for DB URL:
  - ensures `~/.secrets/etl` with secure permissions
  - writes/updates `export ETL_DATABASE_URL=...` when available
  - batch scripts source `~/.secrets/etl`
- Batch runner (`etl/run_batch.py`) for subset execution with run-level status/event updates.
- Tracking:
  - JSONL: `.runs/runs.jsonl`
  - Database (when `ETL_DATABASE_URL` is set): `etl_runs`, `etl_run_steps`, `etl_run_step_attempts`, `etl_run_events`
- DB migration bootstrap (`etl/db.py`) from `db/ddl/*.sql` with checksum enforcement and version tables:
  - `etl_schema_versions`
  - `etl_schema_state`
- Initial DDL scripts:
  - `001_core_tables.sql`
  - `002_config_and_catalog_tables.sql`
  - `003_step_attempts.sql` (retry-ready attempts table)
- CLI (`cli.py`) commands:
  - `plugins list`
  - `validate`
  - `run`
  - `runs list/show`
- Test suite (pytest) with CI:
  - migration bootstrap tests
  - tracking DB write tests
  - timestamped run path tests
  - GitHub Actions workflow at `.github/workflows/tests.yml`

## Known gaps / TODO
- SLURM lifecycle is event-driven from submitted jobs; no central reconciliation of missing/late events yet.
- Retry execution policy is not implemented yet (schema supports attempts, runtime still writes attempt `1` by default).
- No resume-from-step orchestration yet.
- No strict schema validation for pipeline files/plugins beyond current checks.
- No structured logging backend yet (logs primarily stdout + Slurm files).
- DB writes are integrated for runs/steps/events; config/pipeline/plugin catalog tables are not yet populated by runtime.

## Next recommended steps
1) Implement retry policy (max attempts/backoff) and write attempt numbers >1 into `etl_run_step_attempts`.
2) Add resume support using DB state (restart from failed/incomplete steps).
3) Populate `etl_global_configs`, `etl_execution_configs`, `etl_pipelines`, and `etl_plugin_catalog` snapshots at run start.
4) Add more tests:
   - `run_batch` failure transitions
   - SLURM path generation and secret bootstrap edge cases
   - migration checksum drift + concurrent startup
5) Add lightweight observability fields (host, git SHA, config hash, plugin versions) into run records.

## Quick commands
- Validate sample: `python cli.py validate pipelines/sample.yml`
- Run local dry-run: `python cli.py run pipelines/sample.yml --dry-run`
- Run remote SLURM sample: `python cli.py run pipelines/sample.yml --executor slurm --execution-config config/execution.yml --env hpcc_msu --verbose`
- List runs: `python cli.py runs list`
- Show run: `python cli.py runs show <run_id>`
- Run tests: `python -m pytest -q`

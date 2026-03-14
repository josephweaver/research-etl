# Proposed Cleanup Plan

Date: 2026-03-14

Goal:
- Reduce obvious dead code now
- Centralize duplicated behavior next
- Avoid touching execution semantics unless the change is fully covered

## Completed In The Safe-Only Patch

- Removed confirmed-unused imports and locals in low-risk areas
- Removed two no-op `f` prefixes
- Preserved `etl.web.helpers.parse_bool` as a compatibility wrapper because it is imported from `web_api.py`

## Phase 1: Dead-Code Follow-Up

Priority: high

- Review the large unused-local block in `etl/executors/slurm/executor.py`
- Review `vulture` findings in `etl/runtime_context.py`
- Review low-confidence helper candidates in:
  - `etl/subprocess_logging.py`
  - `etl/transports/ssh.py`
  - `etl/executors/hpcc_direct.py`
  - `etl/executors/slurm/sbatch_*`

Success criteria:
- `ruff` no longer reports those `F841`/`F401` items
- SLURM/HPCC focused tests still pass

## Phase 2: Duplicate-Code Extraction

Priority: high

1. Extract shared checkout-preparation logic from:
   - `etl/executors/local.py`
   - `etl/executors/local_batch.py`

2. Extract shared SLURM helper logic from:
   - `etl/executors/slurm/executor.py`
   - `etl/executors/slurm/run_spec_builder.py`

3. Extract shared DB tunnel rewrite shell generation from:
   - `etl/executors/hpcc_direct.py`
   - `etl/executors/slurm/executor.py`

Success criteria:
- Duplicate-code clusters drop materially
- No behavior changes in local, local-batch, SLURM, or HPCC test paths

## Phase 3: Coverage-Driven Review

Priority: medium

Target the lowest-covered operational modules first:

- `etl/ai_pipeline.py`
- `etl/datasets/transports/local_fs.py`
- `etl/datasets/transports/rclone.py`
- `etl/query/runners/duckdb_runner.py`
- `etl/cli_cmd/sync_db.py`
- `plugins/duckdb_load.py`
- `plugins/zip_create.py`
- `plugins/geo/*`

Approach:
- Add characterization tests first
- Then remove or simplify stale code only when tests pin current behavior

## Suggested Guardrails

- Keep `ruff`, `vulture`, and duplicate-code checks in the cleanup workflow
- Treat framework/decorator-registered functions as opt-out from automatic dead-code removal
- Treat public helper re-exports like `etl.web.helpers.parse_bool` as API surface unless explicitly retired
- Prefer extraction over deletion for duplicate execution logic

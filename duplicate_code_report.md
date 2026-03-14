# Duplicate Code Report

Date: 2026-03-14

Tools run:
- `python -m pylint --disable=all --enable=duplicate-code --min-similarity-lines=10 etl plugins tests cli.py`

Summary:
- `pylint` reported `44` duplicate-code clusters
- Most are not safe deletions; they are refactor candidates

## Safe To Remove

No duplicate-code findings were classified as safe to remove without a real refactor.

Reason:
- The duplicate-code tool reported repeated logic blocks, not dead duplicate files or unreachable copies
- Removing any of them safely requires extraction or redesign, not deletion

## Likely Removable, Needs Review

These are the most actionable duplication clusters.

- `etl/executors/local.py` and `etl/executors/local_batch.py`
  - Duplicate strict-checkout / overlay preparation flow
  - Strong candidate for extraction into one shared helper
- `etl/executors/slurm/executor.py` and `etl/executors/slurm/run_spec_builder.py`
  - Duplicate step-index parsing and pipeline-asset overlay helpers
  - Good candidate for a shared SLURM utility module
- `etl/executors/hpcc_direct.py` and `etl/executors/slurm/executor.py`
  - Duplicate DB tunnel URL rewrite shell block
  - Good candidate for one shared shell-snippet builder
- `etl/git_checkout.py` and `etl/provenance.py`
  - Duplicate Git command helpers
  - Likely extractable to a common Git utility
- `plugins/file_copy_regex.py` and `plugins/file_delete_regex.py`
  - Duplicate path-resolution/meta patterns
  - Likely extractable to shared plugin utility logic
- `plugins/geo/geo_raster_polygonize.py` and `plugins/geo/geo_vector_combine.py`
  - Duplicate filesystem/path/output-driver helpers
  - Likely extractable to a geo plugin helper module

## Probably Intentional

These are duplicates, but the duplication may be acceptable given local coupling, test readability, or module-boundary clarity.

- `tests/test_job_planning.py` and `tests/test_local_batch_adapter.py`
  - Shared fixture/build pattern in tests
  - Probably acceptable unless test churn is high
- Small parsing/URL sanitation overlaps where the owning modules serve different layers
  - Example: `etl/db.py` and `etl/executors/slurm/executor.py`
- Some executor-local shell script fragments
  - Duplication exists, but not all of it should be centralized if the execution modes are intentionally decoupled

## Recommended Next Duplicate-Code Refactors

1. Extract the local/local-batch checkout preparation path.
2. Extract the SLURM duplicate helper logic into a small `etl/executors/slurm/common.py`.
3. Extract the DB tunnel rewrite snippet used by both SLURM and `hpcc_direct`.
4. Only after that, evaluate plugin-level helper extraction.

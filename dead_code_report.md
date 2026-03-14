# Dead Code Report

Date: 2026-03-14

Tools run:
- `python -m ruff check .`
- `python -m vulture etl tests cli.py`
- `python -m coverage run -m pytest`
- `python -m coverage report -m`

Coverage summary:
- Overall coverage from the full test run: `77%`
- Coverage was used here mainly to avoid false positives. Low coverage alone was not treated as dead code.

## Safe To Remove

These were either confirmed by `ruff` as unused or were no-op string/intermediate values. They were removed in the safe-only cleanup patch.

- `etl/executors/local.py`
  - Unused local `preparse_workdir`
- `etl/expr.py`
  - Unused import `Optional`
- `etl/pipeline_assets.py`
  - Unused import `subprocess`
- `etl/plugins/base.py`
  - Unused import `inspect`
- `etl/run_batch.py`
  - Unused import `get_app_logger`
- `etl/runner.py`
  - Unused import `PluginDefinition`
- `etl/transports/base.py`
  - Unused import `Any`
- `etl/web_api.py`
  - Unused imports `shutil`, `subprocess`, `Step`
- `plugins/ai_dataset_evidence_bundle.py`
  - Unused local `records` in `_extract_archive_raster_details`
- `plugins/combine_files.py`
  - Unused import `deepcopy`
- `plugins/exec_script.py`
  - Two `f` prefixes on literal-only strings
- `plugins/ftp_download_tree.py`
  - Unused imports `Iterable`, `Optional`
- `plugins/gdrive_download.py`
  - Unused import `Optional`
- `plugins/geo/geo_raster_polygonize.py`
  - Unused imports `Iterable`, `List`
- `plugins/geo/geo_vector_reproject.py`
  - Unused import `Any`

Note:
- `etl/web/helpers.py` looked removable at first glance, but `web_api.py` imports `parse_bool` from it as a public helper surface. That item was a false positive for "safe removal". I kept the API and made it an explicit wrapper.

## Likely Removable, Needs Review

These findings are credible, but removing them safely requires understanding external call sites, import side effects, or intended extension points.

- `etl/executors/slurm/executor.py`
  - Large block of unused locals reported by `ruff` around run-spec staging
  - These are strong cleanup candidates, but I did not bulk-remove them in the first pass because they sit in the remote execution path
- `etl/executors/local_batch.py`
  - `vulture` reports an unsatisfiable ternary at line 248
  - This should be reviewed directly in context before editing
- `etl/runtime_context.py`
  - `vulture` reports `update_base`, `update_vars`, `promote_runtime`, `default_runtime_log_file`, and some catalog fields as unused
  - Given the current refactor direction, these may be intentional near-term APIs
- `etl/subprocess_logging.py`
  - `spawn_logged_subprocess`
- `etl/transports/ssh.py`
  - `run_cmd_with_retries`
- `etl/executors/hpcc_direct.py`
  - `_ssh_common_args`
- `etl/executors/slurm/sbatch_controller.py`
  - `write_controller_sbatch`
- `etl/executors/slurm/sbatch_setup.py`
  - `write_setup_sbatch`
- `etl/executors/slurm/sbatch_step.py`
  - `write_step_sbatch`

## Probably Intentional

These are very likely framework hooks, public API surface, test doubles, or decorator-registered functions that static tools cannot see well.

- `etl/web/routes/api_actions.py`
  - FastAPI route handlers reported unused by `vulture`
- `etl/web/routes/api_read.py`
  - FastAPI route handlers reported unused by `vulture`
- `etl/web/routes/ui.py`
  - UI route handlers reported unused by `vulture`
- `etl/web_api.py`
  - `_init_web_runtime_context`, action helpers, and API functions reported unused by `vulture`
  - These are consistent with framework registration / internal dispatch use
- `etl/executors/base.py`, `etl/logging.py`, `etl/plugins/base.py`, `etl/provisioners/base.py`
  - Methods/fields like `warn`, `is_terminal`, enum-like dataclass fields
  - Likely part of intentionally broader interfaces
- Test-only findings such as:
  - `exc_type`, `tb`, `capture_output`, `_last_params`, `commits`
  - These are common in fakes, context manager stubs, and behavioral assertions

## Coverage Hotspots To Review Next

These are not dead-code findings by themselves, but they are the best places to review for stale logic after the current cleanup:

- `etl/ai_pipeline.py` `16%`
- `etl/datasets/transports/local_fs.py` `14%`
- `etl/datasets/transports/rclone.py` `19%`
- `etl/query/runners/duckdb_runner.py` `16%`
- `etl/cli_cmd/sync_db.py` `25%`
- `plugins/duckdb_load.py` `9%`
- `plugins/zip_create.py` `12%`
- Several geo plugins under `plugins/geo/*` are also very lightly covered

# ETL

[![Tests](https://github.com/<OWNER>/<REPO>/actions/workflows/tests.yml/badge.svg)](https://github.com/<OWNER>/<REPO>/actions/workflows/tests.yml)

A lightweight ETL tool to construct new pipelines from modular Python "plugin" scripts and to track runs and validation. In the future this may also call ChatGPT to write data dictionary entries.

## Current direction

This project has moved from a local prototype toward an operational research runner:

- Execution: local + remote SLURM (setup + dependent batch/array jobs).
- Completion tracking: event-driven from `etl/run_batch.py` (`batch_started`, `batch_completed`, `batch_failed`, `run_completed`), without scheduler polling.
- Persistence: JSONL (`.runs/runs.jsonl`) plus Postgres tables when `ETL_DATABASE_URL` is configured:
  - `etl_runs`
  - `etl_run_steps`
  - `etl_run_step_attempts` (retry-ready)
  - `etl_run_events`
- Provenance: run records capture Git and snapshot context (commit/branch/tag/dirty state, CLI command, and checksums for pipeline/config/plugins).
- Git is the source of truth for pipeline/config content; DB stores checksums and run provenance only (no full pipeline/config text blobs).
- Schema management: migration bootstrap from `db/ddl/*.sql` with checksum/version tracking (`etl_schema_versions`, `etl_schema_state`).
- Artifacts: timestamp-first run paths for easier debugging:
  - `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/<step_name>/`
- Quality: pytest suite + GitHub Actions test workflow.

## Plugin Script

A set of import, transform, and export scripts, along with validation, designed for reusability between jobs.

## Plugin Definition

Ideally each script will have the following properties:
- Define an input and output.
- Be idempotent - allow the same script to run repeatedly without changing the output.
- Contain metadata so the ETL engine can understand the inputs, outputs, and what the script does.
  
## Plugin interface

Each plugin is a Python module under `plugins/` that exposes:

- `meta`: dict with `name`, `version`, `description`, `inputs`, `outputs`, `params` (schema + defaults), `deps`, `idempotent` (bool), and optional `resources` (cpu, mem, gpu).
- `run(args, ctx) -> outputs`: `args` are resolved params/inputs; `ctx` provides `log`, `workdir`, `run_id`, `temp_path()`.
- Optional `validate(args, outputs, ctx) -> ValidationResult`: return/raise failures that block downstream steps.
- Optional `cleanup(ctx)`: best-effort teardown for temp files or remote handles.

Example skeleton:
```python
# plugins/project_raster.py
meta = {
    "name": "project_raster",
    "version": "0.1.0",
    "description": "Reproject raster to target EPSG",
    "inputs": ["raster_path"],
    "outputs": ["projected_path"],
    "params": {"epsg": {"type": "int", "default": 5070}},
    "idempotent": True,
    "resources": {"cpu": 2, "mem": "4Gi"}
}

def run(args, ctx):
    src = args["raster_path"]
    dst = ctx.temp_path("raster_proj.tif")
    # do reprojection...
    return {"projected_path": dst}
```

## Pipeline file format

- `vars`: free-form variables; supports string templating with `{var}` using combined `global` and `vars` scope.
- `dirs`: derived paths or other computed values; also templated.
- `steps`: list of steps; each step supports:
  - `name` (optional; defaults to `step_<index>`)
  - `script` (required string, templated)
  - `output_var` (optional string identifier)
  - `env` (optional mapping, templated)
  - `when` (optional string condition; evaluated later)
  - `foreach` (optional list var name; fans out one step per item)
  - `parallel_with` (optional string; consecutive steps with the same value run in parallel)

Example:
```yaml
vars:
  jobname: prism
dirs:
  datadir: {global.data}/{jobname}
steps:
  - name: fetch_urls
    script: import/read_txt_lines.py {bindir}/urls.txt
    output_var: urls
  - name: download
    script: import/download_http.py {urls}
```

### Download scripts
- http_download.py - list of HTTPS files
- ftp_download.py - all files in an FTP folder

### File identification scripts
- is_zip.py - determine if a file is a zip file
- is_raster.py - determine if a file is a raster image

### Transformation scripts
- unzip.py
- project_raster.py - projects a file to a given EPSG
- slice_raster.py - cuts the raster image into polygons

### Aggregation
- describe_raster.py - calculates descriptive statistics of a raster image

### Validation

## Master configuration

A YAML-powered interface defining global variables that all pipelines/scripts share, such as the bin, data, and log directories - like a system environment key/value list.

## Pipelines definition

An interface where we create a folder structure and add/edit pipelines to define sequences of scripts to run in sequence or parallel.  

## Example pipeline
```yaml
vars:
    jobname: prism
    datadir: {global.data}/{jobname}
    bindir: {global.bin}/{jobname}
    logdir: {global.log}/{jobname}
dirs: 
    rawdir: {datadir}/raw
    unzipdir: {datadir}/unzip
    epsg5070_ras: {datadir}/5070
    county_ras: {datadir}/county_ras
    county_agg: {datadir}/county_agg
steps: 
  - name: fetch_urls
    script: import/read_txt_lines.py {bindir}/urls.txt
    output_var: urls
  - name: download
    script: import/download_http.py {urls}
```
    
## Pipeline creation interface

An interface that lets you call the pipeline scripts one by one, recording each step so you can save a repeatable ETL pipeline.

## Quickstart (local)

Detailed platform install steps: `docs/install.md`.

1) Create and activate a virtual environment (Python 3.10+).  
2) Install package + dev tools: `pip install -e ".[dev]"`.  
3) Confirm CLI install: `etl --version`.  
4) Add a plugin under `plugins/` (see `plugins/echo.py`).  
5) Create a pipeline YAML (see `pipelines/sample.yml`).  
6) (Optional) Create global config `config/global.yml` using `config/global.example.yml` and reference with `{global.data}` etc.  
7) (Optional) Define execution environments in `config/execution.yml` (see `config/execution.example.yml`) and pick one with `--execution-config ... --env hpcc_alpha`.  
8) Run: `etl validate pipelines/sample.yml --global-config config/global.yml` then `etl run pipelines/sample.yml --global-config config/global.yml --execution-config config/execution.yml --env hpcc_alpha`.  
9) Inspect runs: `etl runs list` and `etl runs show <run_id>`.

Defaults: plugins in `plugins/`, run artifacts in `.runs/`, run records in `.runs/runs.jsonl`.

Windows note: if `etl` is not found, use `python -m cli ...` or add your user scripts path (for example `%APPDATA%\Python\Python310\Scripts`) to `PATH`.

## Database DDL bootstrap

- Put database DDL migrations in `db/ddl/` using `.sql` files (for example `001_core_tables.sql`, `002_add_columns.sql`).
- On CLI startup, if `ETL_DATABASE_URL` is set, the app validates schema and applies any new scripts in lexical order.
- Migration metadata is tracked in:
  - `etl_schema_versions` (one row per applied script + checksum)
  - `etl_schema_state` (latest applied script/checksum for quick comparison)
- Existing applied scripts are immutable: if a file's checksum changes, startup fails and asks for a new incremental script.
- Use `CREATE TABLE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` style statements in scripts.
- Do not use drop/recreate patterns for normal schema evolution.

## CLI

- `etl plugins list [-d plugins/]` â€“ list discovered plugins.  
- `etl validate <pipeline.yml> [--global-config config/global.yml]` â€“ parse and validate pipeline syntax/templating.  
- `etl run <pipeline.yml> [--executor local|slurm --global-config ... --execution-config ... --env name --plugins-dir plugins --workdir .runs --dry-run --max-retries N --retry-delay-seconds S --resume-run-id <run_id>]` - run locally or submit SLURM jobs.  
  - SLURM executor submits setup + dependent batch/array jobs with `parallel_with`/`foreach` respected; job/array limits can be set in execution config or env vars.  
- `etl runs list [--store .runs/runs.jsonl]` â€“ show recent recorded runs.  
- `etl runs show <run_id> [--store ...]` â€“ show details for a specific run.
- `etl diagnostics latest [--workdir .runs] [--show]` - print latest diagnostic report path; optional `--show` prints JSON contents.

### Error reports

- On command failures, the CLI writes a portable diagnostic JSON report under `.runs/error_reports/`.
- Share that report file when reporting issues; it includes traceback and local code excerpts around failing frames.

## Local runner behavior

- Step `script` is tokenized with `shlex`; first token resolves to a plugin file (relative to plugins dir, `.py` implied). Remaining tokens are parsed as `key=value` params plus positional `args`.  
- Params are type-coerced using plugin `meta.params` (`type`: int/float/bool/str) and defaults applied.  
- Step `env` entries are applied to process env for the step only.  
- `when` expressions run in a restricted eval against the pipeline vars/dirs and previously stored outputs; falsey â†’ step skipped (recorded as skipped).  
- `foreach` fans out a step over a list variable; each item gets its own step instance. `{item}` placeholders in script/env are filled per item; `output_var` is suffixed with the item index.  
- `parallel_with`: consecutive steps sharing the same value are executed in parallel within a batch.  
- Step retries are supported: `--max-retries` and `--retry-delay-seconds` apply per-step retry behavior (also configurable in execution env via `step_max_retries` and `step_retry_delay_seconds`).
- Each step gets its own workdir under `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/<step_name>`.  
- Validation hook runs after `run` if provided; failures stop the pipeline.  
- Run results are appended to `.runs/runs.jsonl` (JSONL records).
- When `ETL_DATABASE_URL` is configured, run tracking is also persisted to DB tables: `etl_runs`, `etl_run_steps`, `etl_run_step_attempts`, and `etl_run_events`.
- For SLURM execution, completion is event-driven from `run_batch.py` (no polling): batches emit `batch_started`/`batch_completed`/`batch_failed`, and the last batch emits `run_completed`.

## Execution environments

Define multiple clusters/targets in `config/execution.yml`:
```yaml
environments:
  hpcc_alpha:
    host: alpha.login.univ.edu
    partition: compute
    account: proj123
    time: "04:00:00"
    cpus_per_task: 8
    mem: "32G"
    logdir: /scratch/logs
    workdir: /scratch/work
    modules: ["gdal", "python/3.11"]
```
Select with `--execution-config config/execution.yml --env hpcc_alpha`.

### Remote execution (SLURM)

Run the sample pipeline on the remote SLURM target:
```powershell
etl run pipelines/sample.yml --executor slurm --execution-config config/execution.yml --env hpcc_msu --verbose
```

Preview submission without executing jobs:
```powershell
etl run pipelines/sample.yml --executor slurm --execution-config config/execution.yml --env hpcc_msu --verbose --dry-run
```

On remote submissions, the SLURM executor ensures `~/.secrets/etl` exists on the login host with `chmod 600`, writes/updates `export ETL_DATABASE_URL=...` in that file when local `ETL_DATABASE_URL` is available, and generated batch scripts source `~/.secrets/etl` so jobs inherit the DB URL. If the current shell does not contain the variable, Windows `setx` values are also checked from User/Machine environment entries. If neither local nor remote secret value exists, submission fails with a clear error.

### SLURM setup (quick notes)
- Ensure `sbatch`/`sacct` available on the submission host (login node).
- Repository and data paths must be visible on the cluster filesystem (no code shipping yet).
- Auth: use your existing SSH keys (or Kerberos if required) to reach the login node; do not embed credentials in configs.
- Set `logdir` and `workdir` to cluster-visible paths; logs default to `<logdir>/etl-<run_id>-%j.out`.
- The current SLURM executor submits a setup job plus one or more dependent batch/array jobs that execute `etl/run_batch.py`.
- Limits/concurrency overrides: config supports `job_limit`, `array_task_limit`, `max_parallel`; environment variables `ETL_SLURM_JOB_LIMIT`, `ETL_SLURM_ARRAY_LIMIT`, `ETL_MAX_PARALLEL` override at runtime.
- Remote submission: set `ssh_host` (and optional `ssh_user`, `ssh_jump` for bastion/ProxyJump, `remote_repo`) in the execution config to submit `sbatch` via SSH to the cluster login node.
- Optional repo sync: set `sync: true` and `remote_repo` in execution config to scp the current repo to the cluster before submission (requires `scp` and may be slow for large repos).
- SSH/SCP timeouts configurable per env (`ssh_timeout`, `scp_timeout`; defaults: 120s/300s).

## Architecture

- Core engine: loads plugin metadata, resolves vars/deps, orchestrates steps with retry/resume/dry-run; serial or bounded-parallel execution.
- Config: global YAML for shared dirs/creds; pipeline YAML with vars + templating; env vars override; config hash recorded per run.
- Plugin contract: each plugin exposes `run(args, ctx)` and `meta` (inputs, outputs, version, idempotent flag, deps, resource hints); optional validation hook; lives in `plugins/`.
- Storage layout: run artifacts under `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/...`; run metadata in JSONL (`.runs/runs.jsonl`) and Postgres tables when `ETL_DATABASE_URL` is set.
- Validation: pluggable checks that can block downstream steps; results saved with the run record.
- Run tracking: records config hash, git SHA, plugin versions, inputs/outputs, timings, status, logs; immutable `run_id`; supports resume-from-step.
- Interfaces: CLI `etl init|validate|run <pipeline.yaml> [--dry-run --from step --parallel N]`; planned FastAPI REST + lightweight web UI for status/history and plugin catalog.
- Observability: structured logs, metrics hooks (OpenTelemetry-ready), optional alerts via webhook/email/Slack.
- Scheduling: cron/Airflow/Prefect/GitHub Actions via CLI; container-friendly entrypoint.

## Execution Backends (pluggable)

Goal: submit pipelines through interchangeable "executors" (local, SLURM/HPCC, Kubernetes, Airflow, AWS Batch, etc.).

- Contract: each executor exports `submit(pipeline, ctx) -> run_id` and `status(run_id) -> {state, msg, started, ended, logs_uri}`; optional `cancel(run_id)`.
- Discovery/config: executor selected via global config `execution.backend` with backend-specific settings under `execution.backends.<name>` (e.g., ssh host, queue/partition, image, resources).
- Responsibilities:
  - Translate steps into a job spec for the target system (e.g., `sbatch`, `kubectl`, Airflow DAG trigger).
  - Ship code/artifacts if needed (rsync/OCI image); honor data-local paths when provided.
  - Capture logs to a stable URI and return it in `status`.
  - Propagate exit codes/failures so pipeline run tracking reflects real state.
- Built-ins (planned): `local` (process), `slurm` (HPCC), `k8s` (Job), `airflow` (REST/DAG run), `batch` (AWS Batch).
- Extensibility: new executors live under `executors/<name>.py` and register via entrypoint `etl.executors`.
- Security: keep credentials per-backend (env vars/secret store), avoid embedding secrets in pipeline YAML.
- Scheduling: pipeline YAML stays portable; only the executor block changes when targeting a different platform.

## CI -> SLURM Handoff (future)

Goal: trigger ETL when a pipeline YAML is committed, but run heavy geospatial work on the HPCC cluster (SLURM).

- Trigger: GitHub push path-filtered to `pipelines/**/*.yml`.
- Action: lightweight GitHub Action uses SSH (deploy key in repo secrets) to submit an `sbatch` on the HPCC login node.
- Wrapper script (HPCC): `scripts/run_etl.sh <pipeline.yml>` loads modules/conda, checks out the commit SHA, and runs `python -m etl run <pipeline>`.
- Logs/artifacts: write to `logs/<pipeline>/<run_id>.out` and `runs/` on the HPCC filesystem.
- Status return: the Action captures the SLURM job ID and can comment on the PR or fail the workflow on non-zero exit.
- If SSH is blocked: use a campus-hosted webhook/queue or a cron poller on HPCC to submit `sbatch` for new pipeline commits.
- Security: use read-only deploy key, minimal privileges; avoid moving data through GitHub - compute and storage stay on HPCC.


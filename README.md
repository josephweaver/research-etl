# ETL

[![Tests](https://github.com/<OWNER>/<REPO>/actions/workflows/tests.yml/badge.svg)](https://github.com/<OWNER>/<REPO>/actions/workflows/tests.yml)

A lightweight ETL tool to construct new pipelines from modular Python "plugin" scripts and to track runs and validation. In the future this may also call ChatGPT to write data dictionary entries.

## Latest updates (2026-02-13)

- Variable resolution now has a configurable recursion/pass guard via `resolve_max_passes` (default `20`, clamped `1..100`).
  - Configure in `config/global.yml` (or environment config override).
  - Applied consistently in parser, builder preview, builder step-test, and runtime step resolution.
- Builder namespace now reports resolver depth metadata:
  - `resolution.max_passes`
  - `resolution.passes_used`
  - `resolution.stable`
- Fixed recursive/self-referential path expansion issues (for example `workdir: "{workdir}/..."` growth).
- Fixed directory precedence resolution so sibling `dirs.*` values resolve correctly (for example `cachedir` now binds to resolved `dirs.workdir`).
- Fixed builder/run workdir fallbacks:
  - unresolved template workdirs no longer create literal `{env.workdir}` directories;
  - unresolved workdir payloads are ignored in favor of resolved precedence.
- Step logs now honor `dirs.logdir` (when defined) rather than always writing under step workdir.
- Builder validation/preview now understands prior-step output variables (for example `{staged_raw.output_dir}`) so step inputs resolve more accurately in UI checks.
- `archive_extract.py` was hardened for archive lookup and diagnostics:
  - clearer `7z` error reporting (stdout/stderr in step log),
  - better archive path/glob handling,
  - optional selective extraction via `include_glob`.
- Added transformation plugins:
  - `file_move_regex.py` (move regex-matched files while preserving relative subdirectories),
  - `file_delete_regex.py` (delete regex-matched files with optional empty-directory cleanup).

### Runtime note (Windows)

- Use the project virtual environment (`.venv`) for plugin/runtime execution.
- `py7zr` is installed and working in `.venv`; system/anaconda Python may have package conflicts.

## Current direction

This project has moved from a local prototype toward an operational research runner:

- Execution: local + remote SLURM (setup + dependent batch/array jobs).
- Completion tracking: event-driven from `etl/run_batch.py` (`batch_started`, `batch_completed`, `batch_failed`, `run_completed`), without scheduler polling.
- Persistence: JSONL (`.runs/runs.jsonl`) plus Postgres tables when `ETL_DATABASE_URL` is configured:
  - `etl_runs`
  - `etl_run_steps`
  - `etl_run_step_attempts` (retry-ready)
  - `etl_run_events`
- Project partitioning: run/validation/artifact metadata is tagged with `project_id`; project/user membership tables are available for multi-project service operation.
- Provenance: run records capture Git and snapshot context (commit/branch/tag/dirty state, CLI command, and checksums for pipeline/config/plugins).
- Strict source pinning: run paths execute from explicit source selection (git checkout / bundle / snapshot / workspace override), with git-pinned mode as default.
- Git is the source of truth for pipeline/config content; DB stores checksums and run provenance only (no full pipeline/config text blobs).
- Schema management: migration bootstrap from `db/ddl/*.sql` with checksum/version tracking (`etl_schema_versions`, `etl_schema_state`).
- Artifacts: timestamp-first run paths for easier debugging:
  - `.runs/<YYMMDD>/<HHMMSS-<run_id_short>>/<step_name>/`
- Quality: pytest suite + GitHub Actions test workflow.
- Web UX: compact nav with Operations, Pipelines, New Pipeline, and Live Run jump; builder and live run routes are active, with a top-nav user selector (`admin`, `land-core`, `gee-lee`) for scoped views.

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
- Optional artifact descriptors in outputs: include `_artifacts` (or `artifacts`) as a list of mappings to explicitly register artifact metadata:
  - `uri` (or `path`/`location_uri`)
  - `class` (`published`, `cache`, `tmp`, `run_log`, etc.)
  - `location_type` (`gdrive`, `local_cache`, `local_tmp`, `run_artifact`, etc.)
  - `canonical`/`is_canonical` (bool)
  - optional `metadata` (object)

If descriptors are not provided, ResearchETL infers artifact paths from step outputs heuristically and auto-registers them.

### AI dataset research plugin

`plugins/ai_dataset_research.py` lets a pipeline call ChatGPT to draft catalog-ready dataset explanations.

Requirements:
- `OPENAI_API_KEY` in environment.
- Optional `OPENAI_MODEL` (default `gpt-4.1-mini`).

Typical pipeline pattern for multiple datasets:

```yaml
vars:
  datasets:
    - serve.demo_a_v1
    - serve.demo_b_v1
steps:
  - name: ai_catalog_research
    foreach: datasets
    parallel_with: ai_research
    script: 'ai_dataset_research.py dataset_id="{item}" specs_file="config/catalog_ai_specs.yml" output_dir=".runs/catalog_ai"'
```

The plugin writes one JSON file per dataset and returns an explicit `_artifacts` descriptor so outputs are auto-registered.
`specs_file` can optionally provide per-dataset defaults (`title`, `data_class`, `artifact_uri`, `notes`, supplemental URLs, file hints) keyed by `dataset_id`.
See `config/catalog_ai_specs.example.yml` for a starter format.

### AI evidence bundle plugin

`plugins/ai_dataset_evidence_bundle.py` builds high-signal context files for `ai_dataset_research.py` from mixed dataset inputs (folders, `.zip`, and `.7z` when `py7zr` is available).
`.zip` inputs are treated like virtual folders (member files are inspected directly without a separate extraction step).

It writes a dataset bundle directory containing:
- `manifest.json` (inventory, extension counts, tile segments like `h00v00`, archive members, README excerpts)
- `schema_summary.json` (tabular field inference when CSV/TSV/JSON/JSONL are present)
- `sample_summary.json` (sample rows + representative files)
- raster metadata (`raster_details`) with sidecar (`.prj`, `.tfw`) fallback and richer fields when `rasterio` is installed
- `notes_for_ai.txt` (compact context notes)
- `supplemental_urls.txt`
- `catalog_ai_specs.fragment.yml` (ready-to-merge `specs_file` block for `ai_dataset_research`)

Example chain for a tiled archive dataset:

```yaml
steps:
  - name: build_ai_context
    script: 'ai_dataset_evidence_bundle.py dataset_id="serve.yanroy_v1" input_path="data/yanroy.7z" output_dir=".runs/ai_context" supplemental_urls_file="config/yanroy_urls.txt" notes="30m raster field identifier tiles over the Midwest"'
    output_var: evidence

  - name: ai_catalog_research
    script: 'ai_dataset_research.py dataset_id="serve.yanroy_v1" sample_file="{evidence.sample_file}" schema_file="{evidence.schema_file}" notes_file="{evidence.notes_file}" supplemental_urls_file="{evidence.supplemental_urls_file}" output_dir=".runs/catalog_ai"'
```

### Catalog merge/sync plugins

Use these two plugins after AI research generation:

- `plugins/catalog_json_upsert.py`
  - Merges one or more `*.research.json` files into local canonical `catalog.json`.
  - Upserts by `dataset_id` (from payload field or filename stem like `serve.demo_v1.research.json`).
- `plugins/catalog_yaml_sync.py`
  - Reads `catalog.json` and updates dataset YAML docs in a catalog repo (e.g., `../landcore-data-catalog`).
  - Creates missing YAML files with defaults and fills managed fields from `catalog.json`.
  - Preserves existing manual content when `overwrite_managed_fields=false`.

Example chained pipeline:

```yaml
vars:
  datasets:
    - serve.demo_a_v1
    - serve.demo_b_v1
steps:
  - name: ai_catalog_research
    foreach: datasets
    parallel_with: ai_research
    script: 'ai_dataset_research.py dataset_id="{item}" specs_file="config/catalog_ai_specs.yml" output_dir=".runs/catalog_ai"'

  - name: upsert_catalog_json
    script: 'catalog_json_upsert.py research_glob=".runs/catalog_ai/*.research.json" catalog_json=".runs/catalog/catalog.json"'

  - name: sync_catalog_yml
    script: 'catalog_yaml_sync.py catalog_json=".runs/catalog/catalog.json" catalog_repo="../landcore-data-catalog" overwrite_managed_fields=false'
```

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

- `requires_pipelines` (optional): list of prerequisite pipeline YAML paths. `etl run` auto-runs missing successful dependencies first (cycle-safe).
- `project_id` (optional): canonical project partition for the pipeline (for example `land_core`).
- `shared_with_projects` (optional): list of additional project ids that can consume the pipeline by policy.
- `vars`: pipeline variables (also exposed as `pipe.*` namespace during resolution).
- `dirs`: derived paths or other computed values; also templated.
- `steps`: list of steps; each step supports:
  - `name` (optional; defaults to `step_<index>`)
  - `script` (required string, templated)
  - `output_var` (optional string identifier)
  - `env` (optional mapping, templated)
  - `when` (optional string condition; evaluated later)
  - `foreach` (optional list var name; fans out one step per item)
  - `parallel_with` (optional string; consecutive steps with the same value run in parallel)

### Hierarchical variable resolution

Templating is iterative (resolved repeatedly until values stop changing). Resolution context precedence is:

1. `global.*` from `config/global.yml`, also loaded as flat keys.
2. `env.*` from selected execution environment (`config/environments.yml --env ...`), also loaded as flat keys and overriding global flat collisions.
3. `pipe.*` from pipeline `vars`, also loaded as flat keys and overriding prior flat collisions.

`resolve_max_passes` controls iterative depth globally/environment-wide:

```yaml
resolve_max_passes: 20
```

Builder preview exposes pass usage/stability in namespace metadata.

This supports patterns like:
- `pipe.datadir: "{env.datadir}/{pipe.name}"`
- `env.datadir: "{global.datadir}/dev"`

Example:
```yaml
requires_pipelines:
  - pipelines/yanroy_base.yml
vars:
  pipe:
    name: prism
dirs:
  datadir: {env.datadir}/{pipe.name}
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
- gdrive_download.py - Google Drive download via `rclone` (standalone; supports shared drive name/id)

### File identification scripts
- is_zip.py - determine if a file is a zip file
- is_raster.py - determine if a file is a raster image

### Transformation scripts
- unzip.py
- project_raster.py - projects a file to a given EPSG
- slice_raster.py - cuts the raster image into polygons
- file_move_regex.py - move files by regex from a source tree to a destination while preserving relative subdirectories
- file_delete_regex.py - delete files by regex under a source tree (optionally prune empty directories)

Example:

```yaml
steps:
  - name: move_tifs
    plugin: file_move_regex.py
    args:
      src: ".out/data/yanroy/unzip"
      dst: ".out/data/yanroy/unverified"
      pattern: "\\.tif$"
      match_on: "relative_path"   # relative_path | filename | absolute_path
      flags: "i"                  # regex flags: i, m, s
      overwrite: false
```

Delete example:

```yaml
steps:
  - name: purge_unverified_tifs
    plugin: file_delete_regex.py
    args:
      src: ".out/data/yanroy/unverified"
      pattern: "\\.tif$"
      match_on: "relative_path"   # relative_path | filename | absolute_path
      flags: "i"
      dry_run: true
      remove_empty_dirs: true
```

### Aggregation
- describe_raster.py - calculates descriptive statistics of a raster image

### Validation

## Master configuration

A YAML-powered interface defining global variables that all pipelines/scripts share, such as the bin, data, and log directories - like a system environment key/value list.

## Artifact policy configuration

Use a separate policy config for artifact governance (`config/artifacts.yml`) rather than embedding policy in execution environment settings.

- Copy starter policy from `config/artifacts.example.yml`.
- Policy model:
  - `classes`: retention/canonical requirements plus optional `allowed_location_types`.
  - `locations`: storage location types (for example `gdrive`, `local_cache`, `hpcc_cache`) with `kind` and optional roots:
    - `root_uri` for non-filesystem locations (for example `gdrive://data/etl`)
    - `root_path` for filesystem locations (absolute path required)
- DB stores metadata only (artifact IDs, class, checksum, URIs, provenance fields), not file blobs.
- Registration enforces class/location/root bounds. Out-of-bounds registrations are stored as `policy_violation` rows in `etl_artifact_locations` for audit/remediation.

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
7) (Optional) Define execution environments in `config/environments.yml` (see `config/environments.example.yml`) and pick one with `--environments-config ... --env hpcc_alpha`.  
8) Run: `etl validate pipelines/sample.yml --global-config config/global.yml` then `etl run pipelines/sample.yml --global-config config/global.yml --environments-config config/environments.yml --env hpcc_alpha`.  
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
- `etl run <pipeline.yml> [--executor local|slurm --global-config ... --environments-config ... --env name --project-id <project_id> --plugins-dir plugins --workdir .runs --dry-run --max-retries N --retry-delay-seconds S --resume-run-id <run_id> --execution-source auto|git_remote|git_bundle|snapshot|workspace --source-bundle <path> --source-snapshot <path> --allow-workspace-source --allow-dirty-git]` - run locally or submit SLURM jobs.  
  - Workdir precedence: `--workdir` (CLI) -> pipeline `workdir` -> execution env `workdir` -> global config `workdir` -> `.runs`.
  - If `requires_pipelines` is set in the target pipeline, missing successful dependencies are run first automatically.
  - SLURM executor submits setup + dependent batch/array jobs with `parallel_with`/`foreach` respected; job/array limits can be set in environments config or env vars.  
- `etl runs list [--store .runs/runs.jsonl]` â€“ show recent recorded runs.  
- `etl runs show <run_id> [--store ...]` â€“ show details for a specific run.
- `etl diagnostics latest [--workdir .runs] [--show]` - print latest diagnostic report path; optional `--show` prints JSON contents.
- `etl web [--host 127.0.0.1 --port 8000 --reload]` - run minimal FastAPI UI/API for runs and details.
- `etl artifacts enforce [--config config/artifacts.yml --dry-run --limit 2000]` - enforce artifact policies (canonical-location checks + retention cleanup for filesystem-backed locations).
- `etl ai research --dataset-id <id> [--data-class SERVE --title ... --artifact-uri ... --sample-file path --schema-file path --supplemental-url <url> --supplemental-urls-file <txt> --notes ... --model ... --output path]` - ask ChatGPT to draft dataset explanations/usage/quality notes as structured JSON for catalog docs.

### Error reports

- On command failures, the CLI writes a portable diagnostic JSON report under `.runs/error_reports/`.
- Share that report file when reporting issues; it includes traceback and local code excerpts around failing frames.

### Web UI quickstart

0) run .\.venv\Scripts\Activate.ps1
1) Install web dependencies: `python -m pip install -e ".[web]"`
2) Start server: `etl web --host 127.0.0.1 --port 8000 --reload`
3) Open: `http://127.0.0.1:8000`
4) Use top nav:
   - `Operations` for failed/running triage
   - `Pipelines` for catalog/detail views
   - `New Pipeline` for draft builder
   - quick Live Run jump by run id
   - `User` selector to scope API/UI access (`admin`, `land-core`, `gee-lee`)
5) In Operations, select failed runs and use `Resume` or `View` cards (resume currently local executor only).
6) In builder (`/pipelines/new` or `/pipelines/{pipeline_id}/edit`), use:
   - `Load` (from existing YAML)
   - `Generate` (OpenAI-backed draft generation + one-pass auto-repair)
   - `Validate Draft`
   - `Test Step`
   - `Save Draft`
7) Optional run/resume overrides:
   - `plugins_dir`
   - `workdir`
   - `max_retries`
   - `retry_delay_seconds`
   - executor override (`local`/`slurm`)

API endpoints:
- `GET /api/health`
- `GET /api/runs?limit=50&status=failed&executor=local&q=sample&project_id=land_core&as_user=land-core`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/live`
- `POST /api/runs/{run_id}/resume`
- `GET /api/runs/{run_id}/files`
- `GET /api/runs/{run_id}/file?path=logs/job.out`
- `GET /api/pipelines`
- `GET /api/pipelines/{pipeline_id}`
- `GET /api/pipelines/{pipeline_id}/runs`
- `POST /api/pipelines/{pipeline_id}/validate`
- `POST /api/pipelines/{pipeline_id}/run`
- `POST /api/pipelines`
- `PUT /api/pipelines/{pipeline_id}`
- `GET /api/builder/source`
- `POST /api/builder/validate`
- `POST /api/builder/test-step`
- `POST /api/builder/generate`

Access scoping notes:
- API user scope can be provided with `X-ETL-User` header or `as_user` query parameter.
- Current seeded users/projects:
  - `land-core` -> `land_core`
  - `gee-lee` -> `gee_lee`
  - `admin` -> both `land_core` and `gee_lee`
- Most list/detail endpoints accept optional `project_id` filtering; access is validated against user scope.

Artifact browsing uses executor-specific retrieval methods. `local` reads local filesystem artifacts directly. `slurm` currently supports local-visible artifact paths and returns a clear message when only remote cluster paths are available.

### Builder + AI notes

- AI draft generation endpoint (`POST /api/builder/generate`) requires `OPENAI_API_KEY`.
- Optional model override: `OPENAI_MODEL` (default in code: `gpt-4.1-mini`).
- Generation flow performs one automatic repair pass if the first draft fails parser/validator checks.
- CLI dataset research (`etl ai research`) also uses `OPENAI_API_KEY` and returns JSON fields you can map into data catalog YAML (`description`, `how_to_use`, `quality`, `tags`, lineage hints).

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
- Plugins can use standardized logging via `ctx.log("msg")`, `ctx.log("msg", "WARN")`, `ctx.info(...)`, `ctx.warn(...)`, and `ctx.error(...)`; per-step logs are written to `<step_workdir>/step.log`.
- When `ETL_DATABASE_URL` is configured, run tracking is also persisted to DB tables: `etl_runs`, `etl_run_steps`, `etl_run_step_attempts`, and `etl_run_events`.
- For SLURM execution, completion is event-driven from `run_batch.py` (no polling): batches emit `batch_started`/`batch_completed`/`batch_failed`, and the last batch emits `run_completed`.

## Execution environments

Define multiple clusters/targets in `config/environments.yml`:
```yaml
environments:
  local:
    executor: local
    workdir: .runs
    logdir: .runs/logs
  hpcc_alpha:
    executor: slurm
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
Select with `--environments-config config/environments.yml --env hpcc_alpha`.
If `--executor local` is used and no `--env` is provided, the tool automatically uses `environments.local` when available.
Each environment can declare `executor: local|slurm`; mismatched selections are rejected.

### Remote execution (SLURM)

Run the sample pipeline on the remote SLURM target:
```powershell
etl run pipelines/sample.yml --executor slurm --environments-config config/environments.yml --env hpcc_msu --verbose
```

Preview submission without executing jobs:
```powershell
etl run pipelines/sample.yml --executor slurm --environments-config config/environments.yml --env hpcc_msu --verbose --dry-run
```

On remote submissions, the SLURM executor ensures `~/.secrets/etl` exists on the login host with `chmod 600`, writes/updates `export ETL_DATABASE_URL=...` in that file when local `ETL_DATABASE_URL` is available, and generated batch scripts source `~/.secrets/etl` so jobs inherit the DB URL. If the current shell does not contain the variable, Windows `setx` values are also checked from User/Machine environment entries. If neither local nor remote secret value exists, submission fails with a clear error.

### SLURM setup (quick notes)
- Ensure `sbatch`/`sacct` available on the submission host (login node).
- Repository and data paths must be visible on the cluster filesystem (no code shipping yet).
- Auth: use your existing SSH keys (or Kerberos if required) to reach the login node; do not embed credentials in configs.
- Set `logdir` and `workdir` to cluster-visible paths; logs default to `<logdir>/etl-<run_id>-%j.out`.
- The current SLURM executor submits a setup job plus one or more dependent batch/array jobs that execute `etl/run_batch.py`.
- Limits/concurrency overrides: config supports `job_limit`, `array_task_limit`, `max_parallel`; environment variables `ETL_SLURM_JOB_LIMIT`, `ETL_SLURM_ARRAY_LIMIT`, `ETL_MAX_PARALLEL` override at runtime.
- Remote submission: set `ssh_host` (and optional `ssh_user`, `ssh_jump` for bastion/ProxyJump, `remote_repo`) in the environments config to submit `sbatch` via SSH to the cluster login node.
- Optional repo sync: set `sync: true` and `remote_repo` in environments config to scp the current repo to the cluster before submission (requires `scp` and may be slow for large repos).
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

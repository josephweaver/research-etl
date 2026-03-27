# CODEX.md

Session carry-forward notes for working in this environment and across the sibling LandCore repos.

Use this file as a practical memory aid for:

- environment quirks
- repeated failure modes
- known working patterns
- explicit do/don't guidance discovered during real work

Keep this file short, concrete, and operational.

Maintenance rule:

- when a session uncovers a recurring difficulty, environment quirk, failed assumption, or reliable workaround, update this file before ending the work

## Workspace

Primary repo:

- `research-etl`

Related sibling repos currently in use:

- `../landcore-etl-pipelines`
- `../landcore-data-dictionary`
- `../landcore-duckdb`

## Environment Notes

- The project uses `.venv` in the repo root.
- Install/runtime behavior is driven mainly by:
  - `requirements.txt`
  - `pyproject.toml`
- Remote/HPCC setup also depends on `requirements.txt`, so dependency changes should be added there, not only installed locally.
- In this environment, `python` may resolve to Anaconda instead of the repo `.venv`.
- For dependency-sensitive tests or commands, prefer `.venv\\Scripts\\python.exe` explicitly.
- Default SLURM resource values should be checked in `config/environments.yml`.
- Do not guess baseline `mem`, `cpus_per_task`, or similar scheduler defaults from generic cluster assumptions.

## Known Environment Quirks

### User environment variables may not appear in the current shell process

Observed with:

- `GCS_HMAC_KEY`
- `GCS_HMAC_SECRET`

They were present in Windows User environment scope but missing from the live PowerShell process.

If needed for a one-off command, explicitly hydrate them into the shell session:

```powershell
$env:GCS_HMAC_KEY = [Environment]::GetEnvironmentVariable('GCS_HMAC_KEY','User')
$env:GCS_HMAC_SECRET = [Environment]::GetEnvironmentVariable('GCS_HMAC_SECRET','User')
```

Do not assume `setx` or Windows UI changes are visible to the current process without a fresh shell.

### Parquet writing requires `pyarrow`

Observed while implementing `../landcore-etl-pipelines/scripts/yanroy/build_db_fields.py`.

If pandas/GeoPandas Parquet writes fail, confirm `pyarrow` is installed in `.venv`.

Current repo decision:

- `pyarrow` was added to:
  - `requirements.txt`
  - `pyproject.toml`

## Plugin System Notes

### Plugin `meta` is strict

The current plugin loader does **not** accept arbitrary extra keys in `meta`.

Observed failure:

- adding `capabilities` to plugin `meta` caused plugin load errors

Rule:

- do not add unsupported extra `meta` keys unless the core plugin loader is updated first

If capability reporting is needed later, make it a first-class plugin-system feature, not an ad hoc extra metadata field.

### `variable_transform` is experimental

Repo path:

- `plugins/variable_transform.py`

Current status:

- prototype only
- marked `EXPERIMENTAL DO NOT USE`

Working rule:

- do not use it in production pipelines
- prefer normal scripts or existing plugins instead

## GCS Notes

### Current working storage path

`dataset_store` / `dataset_get` now support GCS via native Python, not `rclone`.

Implementation:

- transport: `gcs`
- client: `boto3`
- auth: GCS interoperability HMAC credentials

Environment variables:

- `GCS_HMAC_KEY`
- `GCS_HMAC_SECRET`

Important implementation notes:

- use GCS interoperability HMAC keys, not raw service-account JSON
- client needed path-style addressing and `put_object` behavior to work reliably
- direct upload test succeeded against bucket:
  - `crop-dl`
- remote immutable/HPCC runs may fail plugin load if `boto3` is present but `botocore` is missing or partially installed
- keep `botocore` explicitly declared in dependency files alongside `boto3`

Config files updated:

- `config/data_locations.yml`
- `config/artifacts.yml`

Current location alias:

- `LC_GCS`

### Remote executor secret propagation

- GCS transport on remote HPCC/SLURM runs does not work unless `GCS_HMAC_KEY` and `GCS_HMAC_SECRET` are explicitly propagated into the remote job environment.
- Do not rely on the default secret allowlist.
- For `hpcc_msu` / `hpcc_msu_direct`, add these keys under:
  - `secret_env_keys`
  - `required_secret_keys`

## LandCore DuckDB Repo Notes

Repo:

- `../landcore-duckdb`

Current structural decision:

- no redundant top-level `duckdb/` folder
- use repo-root layout:
  - `workspace.yml`
  - `tables/<domain>/...`
  - `views/<domain>/...`
  - `macros/...`

Working assumption:

- canonical data lives in GCS as Parquet
- shared query metadata lives in `landcore-duckdb`
- do **not** treat a single mutable `.duckdb` file as the system of record

## LandCore YanRoy Notes

### New normalized field tables

Implemented in:

- `../landcore-etl-pipelines/pipelines/yanroy/db_fields.yml`
- `../landcore-etl-pipelines/scripts/yanroy/build_db_fields.py`

Purpose:

- derive canonical `field` and `field_boundary` outputs from YanRoy polygons

Important rule:

- keep `build_db_fields.py` as a normal script
- do not over-abstract its geometry-heavy/source-specific logic into generic transform plugins

### Tile regex note

YanRoy filenames like:

- `WELD_h12v04_2010_field_segments...`

do not work well with `\b(h\d{2}v\d{2})\b` because underscores break the intended word-boundary behavior.

Working pattern:

- use a simpler tile regex:
  - `(h\d{2}v\d{2})`

## Lobell Corn Notes

### Current debug checkpoint

- Current narrowed debug target in `../landcore-etl-pipelines/pipelines/lobell/corn_field_year.yml`:
  - tile: `h18v05`
  - year: `2016`
- Verified so far:
  - step 1 `combine_corn_rasters_by_year`
  - step 2 `aggregate_corn_fields_by_tile_year`
  - step 3 `combine_corn_field_year_csvs`
- Still needs verification:
  - step 4 `normalize_corn_field_year`
- Current HPCC references:
  - `/mnt/scratch/weave151/etl/work/by_tile_year_csv/corn_yield_h18v05_2016.csv`
  - `/mnt/scratch/weave151/etl/work/lobell_corn_field_year/260326/230041-a9606475`

### Raster aggregate plugin notes

- `geo_county_raster_aggregate` was replaced by `plugins/geo/raster_aggregate_by_polygon.py`.
- New argument names are polygon-oriented:
  - `polygon_path`
  - `polygon_id_field`
  - `polygon_name_field`
- `raster_aggregate_by_polygon` now supports column specs:
  - `source`
  - `name`
  - `type`
  - optional `format`
  - `source: literal` with required `value`
- Supported `source` forms currently include:
  - `polygon.<field>`
  - `polygon.id`
  - `polygon.name`
  - `raster.<agg>`
  - `context.day`
  - `context.raster_path`
  - `literal`
- If a downstream script still expects `county_*` fields, either:
  - rewrite the normalizer to accept polygon-oriented names, or
  - emit compatibility column names through the plugin `columns` spec

## Current Design Decisions

- Defer typed variable envelopes / typed runtime values.
- Defer plugin capability metadata until the core plugin interface supports it intentionally.
- Defer `python_eval` and full `variable_transform` usage; neither is a current delivery blocker.
- Prefer scripts for:
  - geometry-heavy logic
  - source-specific normalization
  - multi-step dataset shaping

## Working Rule

If a proposed abstraction does not directly unblock:

- dataset publication
- DuckDB query catalog setup
- YanRoy/LandCore dataset delivery
- HPCC/runtime reliability

then defer it rather than broadening the architecture mid-delivery.

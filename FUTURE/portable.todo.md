# Portability TODO

## Objective
Make ETL pipelines and plugins run consistently across machines with minimal environment-specific setup, while keeping heavy platform integrations (SLURM/SSH/rclone) optional.

## Current Portability Risks (Observed)
- External binaries required by some paths:
  - `rclone` (dataset/gdrive plugins and transports)
  - `7z` (archive extraction fallback when `py7zr` is unavailable)
  - `ssh`, `scp`, `sbatch`, `sacct` (remote/SLURM executors)
- Native geospatial stack dependencies:
  - `rasterio`, `geopandas` (imply GDAL/PROJ/GEOS runtime alignment)
- Environment-specific module loading patterns in HPC executors (`module load`, `source activate`).

## Short-Run Plan (1-2 sprints)

### 1) Dependency contract + startup diagnostics
- [ ] Add a single command: `etl diagnostics deps` (or extend current diagnostics) to report:
  - Python package availability and versions
  - External binary availability (`rclone`, `7z`, `ssh`, `sbatch`)
  - Per-plugin readiness summary
- [ ] Make missing dependency errors actionable and uniform in plugin logs.

### 2) Plugin capability metadata
- [ ] Extend plugin metadata with dependency hints, e.g.:
  - `runtime.python`: package constraints
  - `runtime.binaries`: required/optional binaries
  - `runtime.platforms`: supported executors (`local`, `slurm`, etc.)
- [ ] Use this metadata in builder validation so users see incompatibilities before run.

### 3) Optional-path hardening
- [ ] Keep non-portable features optional by default:
  - `py7zr` preferred over `7z` binary when available
  - local filesystem transport available when `rclone` is absent
- [ ] Ensure plugin selector and docs clearly flag plugins as:
  - `portable`
  - `portable-with-extra-binaries`
  - `hpc-only`

### 4) Packaging consistency
- [ ] Align `pyproject.toml` optional extras with real plugin groups:
  - `geo`, `archive`, `web`, `hpc`, `gdrive`
- [ ] Keep base install minimal and explicit; avoid silently assuming geo stack everywhere.

## Full Portability Target (Container-First)

### 1) Define official runtime images
- [ ] Build and publish versioned images:
  - `etl-base` (core engine only)
  - `etl-geo` (adds raster/geo stack)
  - `etl-hpc` (adds SSH/SLURM client tools as needed)
- [ ] Pin OS + Python + native libs to avoid GDAL mismatch drift.

### 2) Run model
- [ ] Standardize local execution through container entrypoint:
  - `docker run ... etl run ...`
- [ ] Keep host installs for developers optional, not required for production runs.

### 3) Executor adaptation
- [ ] For SLURM, move toward submitting containerized jobs (`singularity/apptainer` or site-supported OCI path).
- [ ] Reduce reliance on cluster module names in pipeline/runtime logic.

### 4) Supply chain + reproducibility
- [ ] Pin dependency locks per image (Python + system libs).
- [ ] Add CI that runs smoke pipelines inside each official image.
- [ ] Persist runtime fingerprint on each ETL run:
  - image digest (or explicit dependency manifest)
  - plugin checksums
  - executor + environment name

## Suggested Milestones
- M1: Dependency diagnostics + plugin dependency metadata + builder warnings.
- M2: Optional extras split + docs + portable plugin classification.
- M3: `etl-base` and `etl-geo` images with CI smoke tests.
- M4: Containerized SLURM execution pattern and provenance capture.

## Practical Guidance for Your Current Geo Work
- Near-term portable path: target `etl-geo` container for geo plugins (including county precipitation aggregation).
- Treat `rclone` and SLURM tooling as integration extras, not required for core plugin execution.
- Keep foreach/day parallelization in engine as-is; plugin should stay single-raster/single-day unit for maximal portability and reuse.

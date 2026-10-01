# HPCC Environment Notes

Use this file for HPCC / SLURM / module-managed runtime issues.

## Current Notes

### Do not guess scheduler defaults

- Check default SLURM resource values in `config/environments.yml`.
- Do not infer `mem`, `cpus_per_task`, or related defaults from generic cluster assumptions.

### Module-managed binaries need the module runtime environment

- On HPCC, an absolute binary path may not be enough.
- Some tools require shared libraries provided only after `module load`.

Example failure:

```text
error while loading shared libraries: libgdal.so.37: cannot open shared object file
```

Working rule:

- if a step uses a module-managed tool such as GDAL, R, or similar, load the module in the runtime shell before invoking the binary
- if you wrap the command with `bash -lc`, separate setup commands with `&&`
  and construct the final tool command explicitly

Safe pattern:

```text
bash -lc "source /etc/profile && module purge && module load GDAL/3.11.1-foss-2025a && ogr2ogr ..."
```

Avoid:

```text
bash -lc source /etc/profile module purge module load GDAL ogr2ogr ...
```

### GDAL / FileGDB note

- The confirmed GDAL module currently exposes:
  - `OpenFileGDB`
- It does not expose:
  - `FileGDB`

Implication:

- a FileGDB may contain tables that appear in geodatabase metadata but are not readable as normal layers in the current environment

### Remote dependency behavior

- Remote HPCC setup depends on `requirements.txt`.
- Dependency changes needed by remote jobs must be added there, not only installed locally.

### Interrupted immutable source checkouts

- A cached checkout can retain a `.git` directory even when an interrupted clone left
  the repository metadata incomplete.
- Treat the checkout as corrupt unless
  `git -C <checkout> rev-parse --is-inside-work-tree` succeeds.
- Remove and reclone only that immutable cache directory; the requested revision is
  pinned and can be reconstructed from its remote.
- A typical symptom is `fatal: not a git repository` during the setup job's
  `git fetch`, before any pipeline step runs.

### Python virtualenv portability across node types

- ICER documents `Illegal instruction` failures when a virtualenv or pip-installed packages are created on one node type and reused on another.
- Prefer creating HPCC virtual environments with `python -m venv --copies`.
- For compatibility, create or refresh shared virtual environments from `dev-amd20` rather than from a newer or more specialized node type.
- In job scripts, load the matching Python module before activating the virtual environment.
- If a SLURM step job fails on `import etl.run_batch` or `pip install -e` with `Illegal instruction`, suspect node-type incompatibility in the shared venv before suspecting the pipeline logic.
- Prefer family-specific virtual environments on heterogeneous HPCC hardware (for example separate `amr` and `skl` venvs) rather than one cluster-wide shared venv.
- If step jobs lazily create family-specific venvs, guard creation/install with a lock so parallel jobs do not race while bootstrapping the same family cache.
- Use bounded `flock` locks for shared venv creation and cached source/asset checkouts. A lock-file pathname may remain after a job exits, but the kernel releases the lock automatically when the process is canceled, times out, or exits.
- On MSU HPCC, do not place `flock` files beside targets under `/mnt/gs21` (GPFS): a live tillage run showed two AMR nodes entering the same protected venv creation concurrently. Generated jobs instead hash the protected target into `$HOME/.cache/research-etl/locks` on the NFS home filesystem. `ETL_LOCK_ROOT` may override that location only with a filesystem known to provide cross-node advisory locks.
- Do not use an unbounded `mkdir <path>.lockdir` loop for shared HPCC state. An interrupted owner leaves the directory behind and every later job can wait until its wall-clock limit.
- Do not decide that a `flock` lock is stale merely because its lock file exists. File existence is not lock ownership; probe it with `flock -n` or allow the configured bounded wait to report a timeout.
- Generated SLURM scripts default to a 900-second lock wait. Override it per environment with `lock_wait_seconds` or per submission with the `ETL_LOCK_WAIT_SECONDS` environment variable when a known checkout/install legitimately needs longer.
- Treat the family-specific venv approach as a temporary workaround for heterogeneous HPCC nodes; when remote execution moves to containers, remove this workaround rather than carrying both systems indefinitely.

### Secret propagation for remote runs

- Remote jobs do not automatically receive all local environment secrets.
- For GCS HMAC use, propagate:
  - `GCS_HMAC_KEY`
  - `GCS_HMAC_SECRET`
- Do not rely on the default allowlist; configure explicit secret propagation in the executor environment settings.

### HPCC target vs local profile

- Use `hpcc_msu` when the operator wants `etl run` to submit work to HPCC through SLURM.
- `--env` is the remote scheduler target. `--control-env` is the machine where
  the command is being launched; use `auto` unless debugging path resolution.
- Use `hpcc_local` only when `etl run` is already executing on HPCC, for example inside a SLURM/controller worker command.
- `hpcc_msu_local` is an older compatibility alias; prefer `hpcc_local` in new configs and docs.

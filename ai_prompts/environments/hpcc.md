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

### Secret propagation for remote runs

- Remote jobs do not automatically receive all local environment secrets.
- For GCS HMAC use, propagate:
  - `GCS_HMAC_KEY`
  - `GCS_HMAC_SECRET`
- Do not rely on the default allowlist; configure explicit secret propagation in the executor environment settings.

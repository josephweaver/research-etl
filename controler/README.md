Lightweight external controller for chunked SLURM re-submission.

Purpose:
- read per-county checkpoint JSON files
- inspect county fit logs for completion markers
- query SLURM for active waves
- submit one new array wave for counties that still need work

Current design:
- uses ETL execution-environment config for SSH/SLURM defaults
- uses `etl.provisioners.slurm.SlurmProvisioner` for submit/status
- writes a per-wave CSV manifest
- submits one array job whose tasks call `python -m controler.main run-item`
- each manifest row contains a rendered worker command
- recommended worker mode is `etl_pipeline`, which launches `etl run ... --var county_fips=...`

Assumptions in this first cut:
- checkpoint files are on a filesystem visible to the controller process
- the remote HPCC repo checkout already contains this `controler/` directory
- the worker command is provided by config and is responsible for doing one county chunk only

Typical usage:

```powershell
python -m controler.main status --config controler/config.yml
python -m controler.main doctor --config controler/config.yml
python -m controler.main preview --config controler/config.yml --fips 17019
python -m controler.main run-once --config controler/config.yml
```

Recommended worker pattern:
- keep one persistent checkpoint JSON per county
- keep one persistent county log path in the checkpoint
- have the worker command run one chunk and exit
- let the controller decide if/when the next wave should be submitted

Recommended ETL worker pattern:
- point `worker.pipeline_path` at `pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml`
- use `worker.mode: etl_pipeline`
- pass `county_fips` as a `--var` override
- keep the child pipeline's `cwd` stable so the R script's relative checkpoint directory remains reusable

Important limitation:
- `run-item` executes the manifest row command directly and returns its exit code
- the controller does not yet stage arbitrary local files to the remote host other than the manifest CSV via the transport
- if you run the controller from a workstation, use shared or remote-visible paths for manifests, checkpoints, repo root, and logs

Diagnostics:
- `doctor` checks checkpoint/log assumptions and reports missing keys or missing log paths
- `preview` renders the exact worker command for a county without submitting anything

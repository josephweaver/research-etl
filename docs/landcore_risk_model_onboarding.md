# LandCore Risk Model Onboarding

This is the minimum sufficient path for a new user to run their own version of
the LandCore checkpointed neighborhood risk-model fit.

The current workflow is useful, but it is still rough-edged. Please be patient
with the engine, the paths, and the HPCC/R environment assumptions. When
something fails, the most helpful next step is usually to capture the exact
command, run id, and latest error report path, then fix one layer at a time.

## What You Are Running

Reference pipeline:

```text
../landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml
```

Main model script:

```text
../landcore-etl-pipelines/scripts/model/Neighborhood_fit_chkptstanr.R
```

The pipeline runs one county model fit. It expects a county input CSV, writes
checkpoint files during the fit, and can resume from those checkpoint files.

## Quick Folder Lookup

There are two code repos involved:

```text
research-etl/
  cli.py
  config/
  controler/
  docs/
  etl/
  plugins/

landcore-etl-pipelines/
  assets/
  db/
  pipelines/
  scripts/
```

On HPCC for Monica/controller use, the shared checkout is:

```text
/mnt/research/Viens_AgroEco_Lab/projects/
  research-etl/
  landcore-etl-pipelines/
```

Quick lookup:

- ETL engine code: `research-etl/etl/`
- reusable ETL plugins: `research-etl/plugins/`
- normal ETL config: `research-etl/config/`
- controller code/config: `research-etl/controler/`
- controller HPCC dev config: `research-etl/controler/config.hpcc_dev.yml`
- LandCore pipeline YAMLs: `landcore-etl-pipelines/pipelines/`
- risk-model pipeline YAMLs: `landcore-etl-pipelines/pipelines/risk_model/`
- LandCore project scripts: `landcore-etl-pipelines/scripts/`
- risk-model R/Python scripts: `landcore-etl-pipelines/scripts/model/`
- model input data on HPCC: `/mnt/gs21/scratch/weave151/etl/data/risk_model/model_input/county_model_input/`
- model fit outputs/checkpoints on HPCC: `/mnt/scratch/weave151/fits/`

For the checkpointed risk-model fit, the most important files are:

```text
research-etl/controler/config.hpcc_dev.yml
research-etl/config/environments.yml
research-etl/config/projects.yml
landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml
landcore-etl-pipelines/scripts/model/Neighborhood_fit_chkptstanr.R
```

Role summary:

- `config/` in `research-etl` tells the engine where environments, projects,
  source repos, and runtime roots live.
- `controler/` in `research-etl` submits and monitors waves of county jobs.
- `pipelines/` in `landcore-etl-pipelines` defines what steps run.
- `scripts/` in `landcore-etl-pipelines` contains project-specific Python/R
  scripts called by pipeline steps.

Important current assumptions:

- Run commands from the ETL engine repo: `../etl`.
- Keep `../landcore-etl-pipelines` as a sibling repo.
- The target execution environment is HPCC/SLURM.
- Windows-to-HPCC has been exercised more than Mac-to-HPCC. A Mac workstation is
  likely workable, but the least surprising fallback is to check the repos out
  directly on HPCC and run the controller from there.
- The R script currently assumes the LandCore HPCC R libraries are available.
- The sample `county_fips: "00000"` is a placeholder and must be changed.

## Recommended Path For Monica

Use this order:

1. Get SSH access to HPCC working from the Mac.
2. Try the Mac-to-HPCC path for one tiny smoke-test county.
3. If Mac-to-HPCC submission fails on path, SSH, SCP, shell, or credential
   assumptions, switch to the HPCC-native path instead of spending too long on
   workstation setup.
4. Once one county works, use the controller to submit county waves as a SLURM
   job array.

The desirable path is Mac-to-HPCC because it keeps the local development loop
comfortable. The pragmatic path is HPCC-native because the model, data, R
libraries, checkpoint directories, and final execution all live there anyway.

## Mac-To-HPCC Notes

On a Mac, use normal shell paths and activate the virtual environment with
`source`:

```bash
cd /path/to/etl
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m cli --version
```

Mac setup prerequisites:

- Install Apple command line tools if `git` is missing:

```bash
xcode-select --install
```

- Confirm Python 3.10+ is available:

```bash
python3 --version
```

- Confirm Git is available and has a user identity:

```bash
git --version
git config --global user.name
git config --global user.email
```

- Confirm GitHub auth works for the repos Monica needs. SSH is preferred if the
  remote URLs are `git@github.com:...`:

```bash
ssh -T git@github.com
```

- Install `rclone` if it is not already available. It is not installed by
  default on macOS.

```bash
brew install rclone
rclone version
```

If Homebrew is not installed, install `rclone` using the official install
instructions or a managed lab setup. If a pipeline accesses Google Drive, also
confirm the needed `rclone` remote exists:

```bash
rclone listremotes
```

- Confirm HPCC SSH works from the Mac:

```bash
ssh <hpcc-alias-or-user@host>
```

The ETL remote-submit path assumes a valid SSH setup already exists. In practice
that means `~/.ssh/config` should contain the HPCC host alias used by
`config/environments.yml`, and the key should be loaded or otherwise usable
without prompts that break non-interactive commands.

Example SSH config shape:

```sshconfig
Host dev-amd20.passwordless
  HostName <hpcc-login-host>
  User <hpcc-user>
  IdentityFile ~/.ssh/<hpcc-key>
  IdentitiesOnly yes
  ServerAliveInterval 30
```

After editing SSH config, check the exact alias from `config/environments.yml`:

```bash
ssh dev-amd20.passwordless hostname
```

Mac-specific things to check before trying a remote ETL submission:

- `ssh <user>@<hpcc-login-host>` works without an interactive password prompt,
  or at least works in the same way the configured transport expects.
- `git` can clone both repos from the Mac and from HPCC.
- `rclone version` works on the Mac if local tooling needs it, and the HPCC
  environment loads an `rclone` module or has a valid `rclone_bin` when remote
  jobs need it.
- The selected `config/environments.yml` environment has the right `ssh_host`,
  `ssh_user`, `path_style: unix`, remote repo paths, and source policy.
- Any SSH key name in `db_tunnel_command`, `ssh_jump`, or related config exists
  on the machine where that command runs.
- Use `/` paths in copied examples. Do not paste Windows backslash paths into
  Mac or HPCC config.

If this path fails, do not treat that as a model failure. It may only mean the
Mac-to-HPCC transport path needs more work.

Minimal Mac preflight:

```bash
git clone git@github.com:josephweaver/research-etl.git etl
git clone git@github.com:josephweaver/landcore-etl-pipelines.git landcore-etl-pipelines
cd etl
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m cli validate ../landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml \
  --global-config config/global.yml \
  --projects-config config/projects.yml \
  --project-id land_core
```

If the clone uses HTTPS instead of SSH, make sure the Mac has a working GitHub
credential helper or token setup before expecting controller bootstrap or remote
checkout steps to work.

## HPCC-Native Fallback

The HPCC-native fallback is to SSH into HPCC, clone/update both repos there, and
run the ETL/controller commands from the HPCC checkout.

Sketch:

```bash
ssh <user>@<hpcc-login-host>
cd /mnt/gs21/scratch/<user-or-project>/etl/src
git clone git@github.com:josephweaver/research-etl.git
git clone git@github.com:josephweaver/landcore-etl-pipelines.git
cd research-etl
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m cli validate ../landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml \
  --global-config config/global.yml \
  --projects-config config/projects.yml \
  --project-id land_core
```

When running from HPCC, use the `hpcc_local` environment for child
ETL runs: the child process is local to the compute node, even though the work
is scheduled by SLURM.

## One-Time Setup

From the `etl` repo:

```powershell
cd path\to\etl
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
etl --version
```

On a Mac, use the setup command shape from the Mac-to-HPCC section above. For
later examples, replace PowerShell line-continuation backticks with Bash
backslashes, and prefer `python -m cli ...` if the `etl` console script is not
on `PATH`.

Make sure these sibling repos exist:

```text
../etl
../landcore-etl-pipelines
```

Create local config files if they do not already exist:

```powershell
if (-not (Test-Path config\projects.yml)) { Copy-Item config\projects.example.yml config\projects.yml }
if (-not (Test-Path config\environments.yml)) { Copy-Item config\environments.example.yml config\environments.yml }
if (-not (Test-Path config\global.yml)) { Copy-Item config\global.example.yml config\global.yml }
```

Then edit:

- `config/projects.yml`: confirm `land_core.vars.pipeline_assets_local_repo_path`
  points to `../landcore-etl-pipelines`.
- `config/environments.yml`: confirm the HPCC environment you will use has the
  right `host`, `partition`, `account`, `workdir`, `logdir`, and source policy.
- `config/global.yml`: only change shared defaults if your machine or run
  environment needs different roots.

## Create Your Own Pipeline Copy

Do not edit the reference file directly for a first run. Copy it to a new file
with your initials, experiment name, county, or date in the name:

```powershell
Copy-Item ..\landcore-etl-pipelines\pipelines\risk_model\neighborhood_fit_chkptstanr_child.yml `
  ..\landcore-etl-pipelines\pipelines\risk_model\neighborhood_fit_chkptstanr_child_MYRUN.yml
```

Open your copy and review these variables first:

```yaml
vars:
  dataset_id: model.landcore_neighborhood_fit_chkptstanr_child_v1
  name: landcore_neighborhood_fit_chkptstanr_child
  county_fips: "00000"
  fit_root: "/mnt/scratch/weave151/fits"
  model_input_root: "/mnt/gs21/scratch/weave151/etl/data/risk_model/model_input/county_model_input"
  fit_script_path: "/mnt/gs21/scratch/weave151/etl/src/landcore-etl-pipelines/scripts/model/Neighborhood_fit_chkptstanr.R"
  iter_warmup: 250
  iter_sampling: 2500
  iter_per_chkpt: 250
  chains: 3
  seed: 1234
  reset: 0
```

Minimum changes for a new run:

- Set `county_fips` to the county you want to fit.
- Give `dataset_id` and `name` a unique value if this is a distinct model run.
- Confirm `model_input_root/{county_fips}/county_data.csv` exists on HPCC.
- Confirm `fit_root/{county_fips}` is a location you are allowed to write.
- Confirm `fit_script_path` points to the model script on the filesystem where
  the HPCC job will run.

For a smoke test, reduce the sampling values in your copy:

```yaml
iter_warmup: 10
iter_sampling: 20
iter_per_chkpt: 10
chains: 1
```

After the smoke test works, restore real values.

## Validate Before Running

From `../etl`:

```powershell
etl validate ..\landcore-etl-pipelines\pipelines\risk_model\neighborhood_fit_chkptstanr_child_MYRUN.yml `
  --global-config config\global.yml `
  --projects-config config\projects.yml `
  --project-id land_core
```

Validation only checks pipeline parsing and variable resolution. It does not
prove that HPCC paths, R packages, or input data are present.

Mac/Bash equivalent:

```bash
python -m cli validate ../landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child_MYRUN.yml \
  --global-config config/global.yml \
  --projects-config config/projects.yml \
  --project-id land_core
```

## Dry-Run Submission

Use the HPCC environment name from `config/environments.yml`. The example below
uses `hpcc_msu`.

```powershell
etl run ..\landcore-etl-pipelines\pipelines\risk_model\neighborhood_fit_chkptstanr_child_MYRUN.yml `
  --global-config config\global.yml `
  --projects-config config\projects.yml `
  --environments-config config\environments.yml `
  --env hpcc_msu `
  --project-id land_core `
  --dry-run `
  --verbose
```

Read the planned command and paths. If anything points to your Windows machine
when it should point to HPCC, stop and fix the pipeline/config before submitting.

## Run

```powershell
etl run ..\landcore-etl-pipelines\pipelines\risk_model\neighborhood_fit_chkptstanr_child_MYRUN.yml `
  --global-config config\global.yml `
  --projects-config config\projects.yml `
  --environments-config config\environments.yml `
  --env hpcc_msu `
  --project-id land_core `
  --verbose
```

Mac/Bash equivalent:

```bash
python -m cli run ../landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child_MYRUN.yml \
  --global-config config/global.yml \
  --projects-config config/projects.yml \
  --environments-config config/environments.yml \
  --env hpcc_msu \
  --project-id land_core \
  --verbose
```

The important outputs are under:

```text
{fit_root}/{county_fips}/
{fit_root}/{county_fips}/run/chkpt/
{fit_root}/{county_fips}/fit_summary.json
{fit_root}/{county_fips}/output_model
```

The checkpoint directory is intentionally durable. If a job stops near its wall
clock limit, run the same pipeline again with `reset: 0` to continue from
available checkpoints.

Use `reset: 1` only when you intentionally want to discard the checkpointed
state and start that county fit over.

## Inspect Runs And Errors

List recent runs:

```powershell
etl runs list
```

Show one run:

```powershell
etl runs show <run_id>
```

On command failures, the CLI writes a diagnostic report under:

```text
.runs/error_reports/
```

When asking for help, include:

- the pipeline copy you ran
- the exact command
- the county FIPS
- the run id
- the latest `.runs/error_reports/*.json` path
- the HPCC job id if one was submitted
- the last 50 to 100 lines of the step or SLURM log

## Common First Failures

`script not found`

Check `fit_script_path`. For HPCC runs it must be valid on HPCC, not only on
Windows.

`county_data.csv` missing

Check `county_fips` and `model_input_root`. The pipeline expects:

```text
{model_input_root}/{county_fips}/county_data.csv
```

R package load errors

The model script uses `tidyverse`, `brms`, `cmdstanr`, `chkptstanr`,
`posterior`, and `jsonlite`. Confirm the HPCC R library paths in
`Neighborhood_fit_chkptstanr.R` match the account/environment running the job.

Immediate model failure after startup

Inspect the input CSV columns. The script expects at least:

```text
unscaled_yield
tile_field_ID
tillage_0_prop
tillage_1_prop
nccpi3corn
vpdmax_7
year
```

No final `output_model`

Check `fit_summary.json` and `run/chkpt/run_status.txt`. A partial run may have
valid checkpoints and summaries even if the final model file has not been
written yet.

## Practical Working Rule

Change one thing at a time:

1. Validate the copied pipeline.
2. Run a tiny smoke test for one county.
3. Confirm checkpoints and `fit_summary.json` are written.
4. Resume once without `reset`.
5. Increase sampling values.
6. Only then start additional counties or longer runs.

The system is pre-1.0 and still has some sharp edges. A slow, boring first run is
usually faster than debugging a large batch of broken jobs.

## Controller For County Waves

After one county works, use the controller for many counties. In this repo the
directory is currently spelled:

```text
controler/
```

The controller is a lightweight wave submitter. It:

- scans per-county checkpoint/status files
- skips counties that look complete or already active
- writes a per-wave manifest CSV under `controler/waves/`
- submits one SLURM job array
- runs one manifest row per array task
- launches the child ETL pipeline with `--var county_fips=<fips>`

Read the local controller docs:

```text
controler/README.md
controler/config.example.yml
```

The active config shape should look like this in spirit:

```yaml
controller:
  checkpoints_glob: "/mnt/scratch/weave151/fits/*/run/chkpt/checkpoint.json"
  status_files_glob: "/mnt/scratch/weave151/fits/*/run/chkpt/run_status.txt"
  local_wave_dir: "controler/waves"
  remote_wave_dir: "/mnt/gs21/scratch/weave151/etl/controler/waves"
  remote_log_dir: "/mnt/gs21/scratch/weave151/etl/controler/logs"
  seed_county_dir: "/mnt/scratch/weave151/etl/data/risk_model/model_input/county_model_input"
  max_submit: 350

slurm:
  environments_config: "config/environments.yml"
  environment: "hpcc_msu"
  job_name_prefix: "risk-fit-wave"
  time: "04:00:00"
  cpus_per_task: 4
  mem: "16G"
  array_max_parallel: 100

worker:
  mode: "etl_pipeline"
  repo_root: "/mnt/gs21/scratch/weave151/etl/src/research-etl"
  python_bin: "/mnt/gs21/scratch/weave151/etl/src/research-etl/.venv/bin/python"
  cli_path: "cli.py"
  pipeline_path: "/mnt/gs21/scratch/weave151/etl/src/landcore-etl-pipelines/pipelines/risk_model/neighborhood_fit_chkptstanr_child.yml"
  executor: "local"
  environments_config: "config/environments.yml"
  env: "hpcc_local"
  project_id: "land_core"
  flags:
    - "--allow-workspace-source"
    - "--allow-dirty-git"
  vars:
    county_fips: "{fips}"
    fit_root: "/mnt/scratch/weave151/fits"
```

Controller dry checks:

```bash
python -m controler.main status --config controler/config.yml
python -m controler.main doctor --config controler/config.yml
python -m controler.main preview --config controler/config.yml --fips 17019
```

Submit exactly one county:

```bash
python -m controler.main run-one --config controler/config.yml --fips 17019
```

Submit one eligible wave:

```bash
python -m controler.main run-once --config controler/config.yml
```

For first use, prefer `run-one`, confirm the generated SLURM log and county
checkpoint state, then use `run-once` with a small `max_submit` before opening
the full wave size.

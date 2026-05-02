# HPCC ETL Setup

Assumed checkout root:

```bash
cd /mnt/research/Viens_AgroEco_Lab/projects
```

## Setup

```bash
module load Python/3.11.3-GCCcore-12.3.0 rclone/1.63.1-amd64 PostgreSQL/16.1-GCCcore-12.3.0
umask 0002
export ETL_BOOTSTRAP_LOG_DIR="$HOME/.cache/research-etl/bootstrap_logs"

git config --global user.name "Your Name"
git config --global user.email "you@example.edu"

git clone git@github.com:josephweaver/research-etl.git
git clone git@github.com:josephweaver/shared-etl-pipelines.git
git clone git@github.com:josephweaver/landcore-etl-pipelines.git

cd research-etl
python -m venv --copies .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Rclone

```bash
rclone config
rclone listremotes
rclone lsd <remote-name>:
```

If HPCC cannot open a browser, run `rclone authorize "drive"` locally and paste
the token into the HPCC `rclone config` prompt.

## Smoke Tests

From Windows/macOS/Linux workstation:

```bash
python -m cli run ../shared-etl-pipelines/pipelines/sample.yml \
  --environments-config config/environments.yml \
  --env hpcc_msu \
  --control-env auto \
  --project-id land_core \
  --ignore-dependencies \
  --verbose
```

From an HPCC shell:

```bash
cd /mnt/research/Viens_AgroEco_Lab/projects/research-etl
source .venv/bin/activate

python -m cli run ../shared-etl-pipelines/pipelines/sample.yml \
  --env hpcc_msu \
  --control-env auto \
  --project-id land_core \
  --ignore-dependencies \
  --verbose
```

Check jobs:

```bash
squeue -u "$USER"
sacct -j <job_ids> --format=JobID,JobName%40,State,ExitCode,Elapsed -P
```

Expected log line:

```text
[step_0] [INFO] [echo] message=hello
```

## Appendix: SSH Keys

User machine to HPCC:

```bash
ssh-keygen -t ed25519 -C "you@example.edu"
ssh-copy-id your_netid@hpcc.msu.edu
ssh your_netid@hpcc.msu.edu
```

If `ssh-copy-id` is unavailable, append the local `~/.ssh/id_ed25519.pub`
contents to `~/.ssh/authorized_keys` on HPCC.

HPCC to GitHub:

```bash
ssh-keygen -t ed25519 -C "you@example.edu"
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
cat ~/.ssh/id_ed25519.pub
ssh -T git@github.com
```

Add the printed public key to GitHub under `Settings -> SSH and GPG keys`.

HPCC jobs to HPCC login node:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_hpc -C "hpcc-etl"
cat ~/.ssh/id_ed25519_hpc.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys ~/.ssh/id_ed25519_hpc
ssh -i ~/.ssh/id_ed25519_hpc your_netid@hpcc.msu.edu hostname
```

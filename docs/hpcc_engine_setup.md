# HPCC ETL Setup

Assumed checkout root:

```bash
cd /mnt/research/Viens_AgroEco_Lab/projects
```

## Setup

```bash
module load Python/3.11.3-GCCcore-12.3.0 rclone/1.63.1-amd64 PostgreSQL/16.1-GCCcore-12.3.0
umask 0002

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

## Smoke Test

```bash
cd /mnt/research/Viens_AgroEco_Lab/projects/research-etl
source .venv/bin/activate

python -m cli run ../shared-etl-pipelines/pipelines/sample.yml \
  --global-config config/global.hpcc_dev.yml \
  --projects-config config/projects.hpcc_workspace.yml \
  --environments-config config/environments.hpcc_workspace.yml \
  --env hpcc_msu \
  --control-env hpcc_msu_local \
  --project-id land_core \
  --execution-mode workspace \
  --execution-source workspace \
  --allow-workspace-source \
  --allow-dirty-git \
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

```bash
ssh-keygen -t ed25519 -C "you@example.edu"
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
cat ~/.ssh/id_ed25519.pub
ssh -T git@github.com
```

Add the printed public key to GitHub under `Settings -> SSH and GPG keys`.

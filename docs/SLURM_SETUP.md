# SLURM Setup Notes

## Submission
- Use a login node where `sbatch`/`sacct` are available.
- The repo and data must be on a filesystem mounted on the compute nodes (no code/data shipping).
- Current executor can submit locally or via SSH. If `sync: true` and `remote_repo` are set in `config/execution.yml`, it will scp the repo to the remote path before submission (simple recursive copy; prefer shared mounts/rsync for large repos).

## Auth
- Prefer SSH keys (deploy/personal) or your existing Kerberos session; do not store passwords in configs.
- If submitting from CI (GitHub Actions), use SSH with keys in secrets; avoid copying data through CI.

## Paths
- Set `logdir` and `workdir` in `config/execution.yml` to cluster-visible locations.
- Logs default to `<logdir>/etl-<run_id>-%j.out`.

## Resources
- Configure `partition`, `account`, `time`, `cpus_per_task`, `mem`, and optional `modules`/`conda_env` per environment.

## Limitations (current)
- Single-job submission; no job arrays/dependency graph yet.
- Status polling is not implemented; cached status only.
- No packaging/shipping of plugins; assumes they exist on cluster path.

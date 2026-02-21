# Builder -> Landcore Repo -> HPCC SLURM Plan

## Goal
When Pipeline Builder runs (full run or step test) for `project_id=land_core`:
1. Commit/push pipeline changes to `landcore-etl-pipelines` (project-scoped repo), not `research-etl`.
2. On `hpcc_msu`, run with:
   - latest `research-etl` checkout
   - matching `landcore-etl-pipelines` branch from step 1
3. Execute pipeline on HPCC via SLURM with the selected env config.

## Why this must be staged
- Current builder git sync is global via `ETL_BUILDER_GIT_SYNC_REPO` (not project-scoped).
- Builder project source views currently resolve to cache clones under `.pipeline_assets_cache`, not an explicit working clone path.
- Remote SLURM path mapping assumes pipeline/scripts are inside the main repo checkout.
- Step scripts referenced from external pipeline repos are not guaranteed to resolve on remote runtime.

## Stage 1: Project-scoped git sync target
- [ ] Add project vars keys in `config/projects.yml`:
  - `pipeline_assets_local_repo_path`
  - `pipeline_assets_git_sync_enabled` (bool, default false)
  - optional `pipeline_assets_git_default_branch`
- [ ] Update `/api/builder/git-status` and `/api/builder/git-sync` to accept `project_id`.
- [ ] Resolve git sync repo root by project first, env var fallback second.
- [ ] Keep `research-etl` manual-only by default.

Acceptance:
- For `land_core`, builder git-sync creates `builder/...` branch in local `landcore-etl-pipelines` clone and pushes it.
- For projects without explicit config, git-sync stays disabled unless env var override is present.

## Stage 2: Builder read/write against project working clone
- [ ] Update builder pipeline source views to prefer `pipeline_assets_local_repo_path` for project sources.
- [ ] Update create/update endpoints so saves go to project working clone path (not cache clone).
- [ ] Keep cache clone path as read-only fallback when local path is missing.

Acceptance:
- Editing/saving a land-core pipeline updates files under `../landcore-etl-pipelines/pipelines/...`.

## Stage 3: Propagate created asset branch into run context
- [ ] Return synced branch info from git-sync to UI (already present), persist in builder session state.
- [ ] Include `pipeline_assets_ref_override=<branch>` in run and step-test payloads.
- [ ] Server injects override into action context for remote executors.

Acceptance:
- Run payload carries the branch created by git-sync for that builder session.

## Stage 4: Remote SLURM support for external pipeline asset repo
- [ ] Extend SLURM setup script generation to optionally clone/pin external pipeline-asset repo(s) at requested ref/branch.
- [ ] Add remote path mapping for pipeline path when pipeline is external to `research-etl` repo.
- [ ] Pass remote pipeline path + project context to `run_batch.py`.

Acceptance:
- `hpcc_msu` run uses `research-etl` checkout plus checked-out landcore assets branch.
- Pipeline file resolves on remote without local workspace assumptions.

## Stage 5: External scripts path resolution
- [ ] Ensure `exec_script.py` can resolve scripts relative to pipeline file directory (not only `ETL_REPO_ROOT`).
- [ ] Pass pipeline file absolute path (or dir) into runtime context/env.

Acceptance:
- A pipeline stored in landcore repo can call `scripts/...` from that repo on HPCC successfully.

## Stage 6: Remove/relax remote external-path guard
- [ ] Replace current web API guard that blocks external pipeline paths for `slurm`/`hpcc_direct`.
- [ ] Keep validation to reject ambiguous/missing source configs.

Acceptance:
- External project pipeline path is allowed for remote executor when project source config is complete.

## Stage 7: Tests
- [ ] Unit tests for project-scoped git-sync repo resolution.
- [ ] API tests for builder save/update + git-sync with `project_id=land_core`.
- [ ] Integration tests for action run payload carrying `pipeline_assets_ref_override`.
- [ ] SLURM script render tests for external asset clone/checkout commands.
- [ ] Step-test path resolution tests for external scripts.

## Rollout order
1. Stage 1 + 2 (safe, local behavior)
2. Stage 3 (branch propagation)
3. Stage 4 + 5 (remote execution mechanics)
4. Stage 6 + 7 (enable + harden)

## Immediate next implementation (recommended)
- Implement Stage 1 + 2 first so builder edits and git branches are definitively in `landcore-etl-pipelines`.

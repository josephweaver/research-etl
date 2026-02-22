# Future TODO

Last updated: 2026-02-21

## New Feature Request
- [ ] Add a first-class "Create New Project" feature with dedicated Project UI and selector.
  - Include a Project UI selector in the app navigation.
  - Allow creating a new `config/projects.yml` entry from UI.
  - Allow editing project variables from UI.
  - Allow selecting or creating associated GitHub repos:
    - main repo
    - shared repo(s)
  - Allow associating users to the new project.
  - Auto-add the creating user to project membership by default.

## From README.md
- [ ] Add explicit `--allow-dirty-git` support for remote execution by checking out pinned commit remotely, then overlaying local dirty files before run start.

### CI -> SLURM Handoff (future)
- [ ] Trigger ETL on pipeline YAML commit and submit to HPCC via SSH from GitHub Actions.
- [ ] Capture SLURM job ID back to PR/workflow status.
- [ ] Use least-privilege deploy keys and keep compute/storage on HPCC.
- [ ] Add fallback trigger path (campus webhook/queue or HPCC-side cron poller) when SSH from GitHub is blocked.

## From PROJECT_STATUS.md
### Suggested next steps
- [ ] Add SSH-backed remote artifact retrieval for SLURM paths in web API.
- [ ] Add DB-backed pipeline draft/version model.
- [ ] Add auth guard for web UI if multi-user exposure is planned.
- [ ] Persist config/catalog snapshots into DB catalog tables at run start.
- [ ] Add offline event buffering strategy for runs without DB connectivity.
- [ ] Improve AI generation with constrained output schema and optional additional repair retries.
- [ ] Add remote dirty-overlay support for `--allow-dirty-git`.
- [ ] Wire `geo_vector_filter.py` into `pipelines/yanroy/tiles_of_interest.yml` before tile intersection logic.

### Possible future features
- [ ] Dynamic chained fan-out from prior fan-out outputs with deterministic persisted expansion manifests.
- [ ] Resolved dynamic execution plans materialized before run start.
- [ ] Adaptive SLURM execution packing using historical runtime telemetry.

## From notes.md
### Future cleanup (path resolution)
- [ ] Centralize path/glob normalization in shared runtime utilities (post-resolution), instead of per-plugin ad hoc handling.
- [ ] Keep plugin traversal logic, but move common `path`/`*_glob` handling to one resolver shared by runner + builder test paths.
- [ ] SLURM path model cleanup: remove implicit step workdir/logdir generation and support explicit per-step templates (for example `{workdir}/{step.name}` with co-located logs).

### Things to fix later
- [ ] Add first-class `Project` object with repo bindings and project-level variables.
- [ ] Improve runtime stats collection from plugin-level to pipeline-step-level sizing.
- [ ] Add GitHub data dictionary write workflow (branch -> update YAML -> commit/push -> PR -> human review).
- [ ] Pipeline builder: expose `hpcc_direct` everywhere executor is selected.
- [ ] Pipeline builder: allow overriding `sys.*` runtime values for deterministic testing.
- [ ] Evaluate single-step execution mode with persisted step state/outputs for incremental continuation.
- [ ] Add optional step-scoped dependency environments (`env_mode: shared|per_step`) with cache-by-dependency-hash.
- [ ] Investigate first-class masked variable objects for secrets.
- [ ] Add structured progress reporting/progress bars for long-running steps in CLI + web.

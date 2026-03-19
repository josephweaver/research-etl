# FUTURE

Items in this file are explicitly out of current delivery scope unless they are promoted back into `needed-now core`.

Last updated: 2026-03-14

## What this file is for

Use this file as the parking lot for:

- architecture ideas that are not needed to unblock current dataset delivery
- product/UI expansion
- framework cleanup and abstractions
- future pipeline/plugin programs that are not part of the current crop-insurance and landcore execution push

`README.md` defines scope.
`PROJECT_STATUS.md` tracks active work.
`FUTURE.md` tracks deferred work.

## Backlog classification from working notes

The files under `FUTURE/` are backlog/reference documents, not the active delivery plan.

Unless an item is promoted because it is blocking crop-insurance or landcore execution in one of the seven `needed-now core` areas, treat it as deferred here.

## Deferred architecture and platform tracks

- Parallel execution model expansion:
  - parent-child run linkage
  - cancellation propagation
  - richer `foreach` mode, placement, and backend policy
- Runtime environment reuse:
  - avoid rebuilding `.venv` for every new immutable `research-etl-<sha>` checkout when dependencies are unchanged
  - keep this deferred unless it becomes a direct blocker for crop-insurance or landcore execution
- Transport-layer abstraction and cleanup:
  - transport lifecycle design
  - reusable command/file/session channels
  - executor transport decoupling
- Job-spec and provisioner architecture work:
  - `FUTURE/JOB_SPEC.md`
  - `FUTURE/PROVISIONER.md`
  - run/job/workload spec separation beyond the current execution needs
- Source-control abstraction beyond current Git behavior:
  - provider registry/factory
  - second provider proof
  - provider UX/config surfacing
- Portability program:
  - diagnostics command for dependencies
  - plugin capability metadata
  - optional extras cleanup
  - container-first runtime images and CI
- Query platform expansion:
  - additional query endpoints
  - UI integration
  - governance, allowlists, and audit hardening
- Dataset-platform redesign from `FUTURE/data.notes.md`:
  - dataset-first registry model
  - dictionary/DB split evolution
  - dataset service/routing/transport surface expansion
  - broader datasets UX and CLI additions beyond immediate delivery needs
- Builder step-test session system:
  - persisted session state
  - shared context across steps
  - builder session APIs/UI

## Deferred pipeline/plugin expansion

- YanRoy and PRISM migration backlog from `FUTURE/pipe.plug.notes.md`
- YanRoy geo plugin and pipeline backlog from `FUTURE/geo.plugins.todo.md`
- generalized data-zone redesign (`work`, `cache`, `publish`) beyond immediate delivery needs
- generic validation-plugin templates, retention policy automation, checksum manifest systems, and similar framework work unless needed for current dataset publication

## Deferred product and UX backlog

- broad web/UI expansion from `FUTURE/notes.md`
- first-class project management UI and repo binding workflows
- installer/preflight wizard flows
- metrics/KPI framework
- CI-to-SLURM handoff automation
- AI/data-dictionary workflow expansion
- dataset-level dashboard and operational analytics work

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

## First-Run Installer / Preflight (Reduce Manual Setup)
- [ ] Add `etl install` (or `etl doctor --fix`) to auto-bootstrap first-run environment for local/HPCC workflows.
- [ ] Detect execution context (`local`, `slurm/hpcc_msu`, `hpcc_direct`) and apply environment-specific setup.
- [ ] SSH setup automation:
  - verify gateway connectivity in batch mode (`ssh -o BatchMode=yes ...`),
  - detect missing/invalid key config and offer guided key generation/install,
  - write/update `~/.ssh/config` host alias entries (`IdentityFile`, `IdentitiesOnly`, timeouts).
- [ ] Secrets bootstrap automation:
  - create `~/.secrets` + `~/.secrets/etl` with secure permissions,
  - validate `ETL_DATABASE_URL` format,
  - validate Neon endpoint options exist (`options=endpoint%3D...`),
  - avoid secret clobbering (preview + confirm before overwrite).
- [ ] DB tunnel preflight:
  - validate tunnel command reaches configured gateway,
  - verify local forwarded port opens (`127.0.0.1:6543` by default),
  - validate rewritten DB URL can run a smoke query (`select 1` / `select now()`),
  - fix local schema bootstrap error handling in `etl/db.py` so an SSL/tunnel disconnect during migration checks preserves the original psycopg error and does not mask it with a secondary `the connection is closed` failure from advisory-unlock cleanup.
- [ ] Environment config bootstrap/validation:
  - validate required keys in `config/environments.yml` for selected env,
  - provide safe defaults for tunnel rewrite options (`db_tunnel_rewrite_database_url`, host/port),
  - warn on conflicting settings (`propagate_db_secret` vs manual secret management expectations).
- [ ] Pipeline asset setup validation:
  - verify sibling asset cache root and access permissions,
  - verify expected pipeline asset repo/ref exists and is reachable,
  - confirm step-stage cache-only mode is active for remote jobs.
- [ ] One-command diagnostics bundle:
  - emit a redacted report (checks passed/failed + fixes applied),
  - include exact follow-up commands for any unresolved checks.
- [ ] Optional interactive wizard mode:
  - ask minimal questions once,
  - persist answers to env/project config and mark install state.

## Incremental Reuse / Skip Reexecution Plan
Goal: avoid re-running prerequisite pipelines unless relevant inputs/logic changed, with explicit force overrides.

### Phase 0 (run-level prerequisite skip, minimal risk)
- [ ] Define a `pipeline_fingerprint` spec for prerequisite reuse decisions:
  - pipeline YAML checksum (resolved file content)
  - referenced plugin/script checksums
  - effective config checksums (`global`, `env`, `project`)
  - execution identity fields (`executor`, selected env name)
- [ ] Fix remote prerequisite reuse for `slurm`/`hpcc_direct`:
  - current dependency auto-run checks only local `runs.jsonl` under the resolved workdir,
  - remote successful prerequisite runs recorded in the DB are therefore invisible and get rerun,
  - dependency skip should prefer DB-backed run history for remote executors and fall back to local JSONL only when DB is unavailable.
- [ ] Persist `pipeline_fingerprint` (and fingerprint components) with run metadata/events.
- [ ] Update dependency auto-run logic:
  - if latest successful prerequisite run has matching `pipeline_fingerprint`, skip reexecution
  - otherwise reexecute prerequisite
  - support recurring daily pipelines where prerequisites may also be daily and reuse decisions must distinguish "same dependency already satisfied for this date/input window" from "rerun required for a new day"
- [ ] Add CLI controls:
  - `--force` (force target + dependencies)
  - `--force-deps` (force prerequisite reexecution only)
  - optional `--no-skip-deps` alias for clarity
- [ ] Emit explicit decision logs before dependency handling:
  - `skip` vs `rerun`
  - exact mismatch reasons (which fingerprint component changed)
- [ ] Add web/API parity for run actions (same default skip behavior + force override fields).

Acceptance criteria:
- [ ] Re-running the same pipeline with unchanged code/config skips prerequisite runs.
- [ ] Any fingerprint component change triggers prerequisite rerun.
- [ ] `--force` and `--force-deps` override skip behavior deterministically.
- [ ] Run logs/UI clearly explain every skip/rerun decision.

### Phase 1 (input/data-aware invalidation)
- [ ] Add optional input fingerprinting for prerequisite pipelines:
  - dataset version ids when using dataset registry
  - optional file fingerprints (size/mtime or checksum policy)
- [ ] Add plugin/pipeline metadata flags:
  - `cacheable: true|false`
  - `nondeterministic: true|false`
  - `side_effect_only: true|false`
- [ ] Enforce conservative default:
  - if determinism cannot be established, rerun (or require explicit opt-in to skip).

Acceptance criteria:
- [ ] Prerequisite skip decisions can include data version/fingerprint checks.
- [ ] Pipelines marked nondeterministic are never auto-skipped unless explicitly forced.

### Phase 2 (step-level incremental execution)
- [ ] Introduce step fingerprints and reusable step output manifests.
- [ ] Reuse prior successful step outputs when step fingerprint matches.
- [ ] Add per-step invalidation traces in UI/CLI diagnostics.

Acceptance criteria:
- [ ] Partial rerun works for modified pipelines without recomputing unchanged steps.
- [ ] UI shows "why step reran" and "why step reused".

### Guardrails / policy
- [ ] Treat `sys.now.*`, random seeds, external "latest" APIs as nondeterministic inputs unless pinned.
- [ ] Include Python/runtime dependency signature (or lockfile checksum) in fingerprint policy.
- [ ] Keep skip decisions auditable: persist `decision_reason` + component diffs.
- [ ] Default to safety over speed when fingerprint data is incomplete.

### Effort estimate
- [ ] Phase 0: ~2-4 dev days
- [ ] Phase 1: ~3-7 dev days
- [ ] Phase 2: ~1-2+ weeks

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
- [ ] Add dataset-level ops metrics (finite dataset scope) and expose in SQL view/materialized view:
  - turnaround time = `etl_runs.ended_at - etl_runs.started_at` for runs linked to dataset versions (`etl_dataset_versions.created_by_run_id`).
  - failed-run recovery time = first failure -> next success for same dataset/pipeline lineage.
  - reproducibility rate = reruns with same provenance (`git_commit_sha` + checksums) and matching output validation/checksum outcomes.
- [ ] Add dashboard/API endpoint for dataset-level p50/p90 turnaround, recovery time, and reproducibility trend.

### Possible future features
- [ ] Dynamic chained fan-out from prior fan-out outputs with deterministic persisted expansion manifests.
- [ ] Resolved dynamic execution plans materialized before run start.
- [ ] Adaptive SLURM execution packing using historical runtime telemetry.
- [ ] Revisit nested child-pipeline execution on HPCC/SLURM.
  - Current PRISM state-stage attempt exposed a multi-part failure chain when a parent SLURM batch step used `pipeline_execute.py` to launch a child pipeline.
  - Fixed pieces already identified:
    - child relative pipeline paths must resolve from `ETL_REPO_ROOT`, not the step scratch workdir
    - child CLI runs must inherit `global/projects/environments` config paths and current `project_id`
    - pinned asset refs that are already commit SHAs must not be re-resolved through `git ls-remote <sha>`
  - Remaining design issue:
    - child runs launched with `--env hpcc_msu` recursively attempted SLURM submission from inside an active SLURM job
    - switching to an on-node local env (`hpcc_msu_local`) is the right direction, but this path should be revalidated end-to-end before restoring multi-state/state-year fanout
  - Resume point:
    - parent pipeline: `shared-etl-pipelines/pipelines/prism/stage-ts-ppt-day-state.yml`
    - child pipeline: `shared-etl-pipelines/pipelines/prism/stage-ts-ppt-day-state-child.yml`
    - parent HPCC run inspected: `/mnt/scratch/weave151/etl/work/prism_stage_ts_ppt_day_state/260319/215945-8804ae6f`
    - representative failure logs:
      - `logs/01_run_state_year_child/etl-8804ae6f4b4f419fae314212d09471f2-3827109.0.out`
      - `logs/01_run_state_year_child_18/2647ae67f96341b8ad298ea7587c03f8/logs/step.log`

## From FUTURE/notes.md
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

## KPI Framework (effectiveness evidence)
- [ ] Add Change Failure Rate KPI:
  - % of pipeline/config changes that lead to failed runs or regressions.
- [ ] Add MTTD KPI:
  - time from failure event to detection/alert acknowledgment.
- [ ] Add MTTR KPI:
  - time from failure event to first restored successful run.
- [ ] Add Automation Rate KPI:
  - % of successful runs completed without manual intervention.
- [ ] Add Data Freshness Lag KPI:
  - source data availability time -> published dataset version time.
- [ ] Add Cost per Successful Build KPI:
  - normalized compute/storage/transfer cost per successful dataset version.
- [ ] Add Throughput KPI:
  - successful dataset versions published per week/month.
- [ ] Add Reuse Yield KPI:
  - % runs that reused cached/prerequisite outputs vs full recompute.
- [ ] Add Validation Defect Escape Rate KPI:
  - % published datasets later marked with validation defects.
- [ ] Add Downstream Value Proxy KPI:
  - count of downstream analyses/models/publications consuming generated datasets.

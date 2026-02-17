# 3-Week Product Plan (ResearchETL v0)

Last updated: 2026-02-16

## End-of-day handoff (2026-02-16)

Done today:
- Added new reusable plugins:
  - `combine_files.py` (csv/json/yaml/xml/text merge),
  - `exec_script.py`,
  - `gdrive_upload.py`.
- Added `pipelines/yanroy/tiles_of_interest.yml` and moved script to `scripts/yanroy/build_tiles_of_interest.py`.
- Updated `pipelines/yanroy/extract_fields.yml`:
  - switched step 2 to native `foreach: tiles` with `raster_facts.py`,
  - parallel combine support for consolidated outputs.
- Added runner fallback resource telemetry (CPU/memory) when plugins do not emit metrics.
- Added Plugins page/nav in web UI and stabilized `/api/plugins/stats`.
- SLURM/runtime UX updates:
  - setup job default time `00:10:00` (`setup_time` override),
  - safe verbose logs in generated SLURM scripts,
  - `run_batch.py --verbose` and SLURM pass-through,
  - SLURM now honors pipeline `dirs.logdir` over env logdir.

Carry-over for tomorrow:
- Run full Yanroy `download -> extract_fields -> tiles_of_interest` chain on HPCC and verify artifact locations.
- Add fast-path tiles-of-interest mode using precomputed state/tile lookup + combined Yanroy facts.
- Decide policy for dirty-worktree fallback to clean parallel checkout when strict mode is enabled.

ask;

I noticed that twe provide both a executor and an enviroment,  I belive the environment imples an executor.  do you agree?  am I missing something?  

## New updates (2026-02-13)
- [x] `P0` Added configurable variable resolution depth guard (`resolve_max_passes`, default `20`, clamped `1..100`).
- [x] `P0` Unified resolver cap across parser, builder preview/test-step, and runtime runner.
- [x] `P0` Builder namespace now reports:
  - [x] `resolution.max_passes`
  - [x] `resolution.passes_used`
  - [x] `resolution.stable`
- [x] `P0` Fixed recursive workdir growth bug (`workdir: "{workdir}/..."` expansion loop).
- [x] `P0` Fixed `dirs` sibling precedence so `{cachedir}` resolves from `dirs.workdir` as expected.
- [x] `P0` Fixed builder/run unresolved template workdir leakage (`{env.workdir}/...` directories no longer created on fallback paths).
- [x] `P0` Step logs now honor configured `dirs.logdir` when present.
- [x] `P0` Updated builder default dirs to non-recursive templates.

## Future cleanup (path resolution)
- [ ] `P1` Centralize path/glob normalization in shared runtime utilities (after variable resolution), so plugins do not each implement custom path resolution logic.
- [ ] `P1` Keep plugin-side directory/file traversal logic, but move common `path`/`*_glob` handling to one consistent resolver used by runner + builder test paths.

## Current status snapshot
- [x] `P0` Core engine + retry/resume/provenance + SLURM event tracking.
- [x] `P0` Minimal web UI/API scaffold is live.
- [x] `P0` Web supports run list/detail, artifact browsing, resume action (local), and run/validate actions.
- [x] `P0` Test suite green locally (`47 passed`).
- [x] `P0` Pipeline-centric web UX (catalog/detail/builder/live view) implemented.
- [x] `P0` Project partitioning scaffold added (`project_id` on runs/validations/artifacts + user/project membership tables).
- [x] `P0` Service-mode access controls added (scoped by `X-ETL-User` or `as_user`), with seeded users:
  - [x] `land-core`
  - [x] `gee-lee`
  - [x] `admin` (access to both projects)

## Product direction update (agreed)
- [x] `P0` Treat `Pipeline` as primary object in the web app.
- [x] `P0` Keep `Runs` view as operations inbox (failed/running triage).
- [x] `P0` Put "run now" inside pipeline detail instead of a disconnected run page.

## Ordered implementation backlog (next execution order)
- [x] `Task 1 (P0)` Add `GET /api/pipelines` summary endpoint (last status, last started, runs, failure rate).
- [x] `Task 2 (P0)` Add `/pipelines` UI mode with pipeline table + click-to-prefill run form and run filter.
- [x] `Task 3 (P0)` Add pipeline detail route `/pipelines/{pipeline_id}` with:
  - [x] scoped run history
  - [x] scoped validate/run actions
  - [x] provenance summary
- [x] `Task 4 (P0)` Add operations dashboard on `/` for failed/running first view.
- [x] `Task 5 (P0)` Add live run status API + UI (`/runs/{run_id}/live`) with event timeline.
- [x] `Task 6 (P1)` Add draft pipeline builder (new/edit/test-step flow).
  - [x] save draft via API (`POST /api/pipelines`, `PUT /api/pipelines/{id}`)
  - [x] AI draft generation via API (`POST /api/builder/generate`)

## Recent pipeline/plugin updates
- [x] `P0` Iterative hierarchical variable resolver with namespace precedence:
  - [x] `global.*` + flat globals
  - [x] `env.*` + flat env overrides (from execution config)
  - [x] `pipe.*` + flat pipeline overrides
- [x] `P0` Pipeline dependencies via `requires_pipelines` with auto-run of missing successful prerequisites.
- [x] `P0` Dependency cycle detection for prerequisite pipelines.
- [x] `P0` New Google Drive staging plugin: `plugins/gdrive_download.py` (wraps `tools/gdrv/download.R`).
- [x] `P0` YanRoy scaffold split:
  - [x] `pipelines/yanroy_base.yml` for data staging dependency
  - [x] `pipelines/yanroy.yml` for main flow with `requires_pipelines`

## Week 1 - Core hardening (P0)
- [x] `P0` Packaging/install flow complete (`pyproject.toml`, `etl` console entrypoint, install docs validated).
- [x] `P0` Retry/resume edge cases verified for `parallel_with`, `foreach`, and SLURM array batches.
- [x] `P0` Error handling pass: classify common failures and return clear actionable messages.
- [x] `P0` Provenance completeness check: all run paths (local/slurm/run_batch/resume) persist Git+checksum metadata.
- [x] `P0` Integration tests for SLURM event transitions (`queued -> running -> completed/failed`).
- [x] `P0` Integration tests for resume from partial success.
- [x] `P0` Integration tests for retry attempts persisted with `attempt_no > 1`.

## Week 2 - Web interface MVP (P0/P1)
- [x] `P0` Status view/table with run states and step-attempt details.
- [x] `P0` Add a single "resume failed run" action.
- [x] `P0` Add "validate + run" action in web UI/API.
- [x] `P0` Add run artifact tree + text viewer.
- [ ] `P0` Add pipeline catalog view (list/search pipelines with health badges).
- [ ] `P0` Add pipeline detail view:
  - [x] run history for selected pipeline
  - [ ] validation history for selected pipeline
  - [x] embedded run form (executor/env/retries/config)
  - [x] provenance summary per run
- [x] `P0` Add operations landing page (failed/running triage + quick actions).
- [x] `P0` Add live run view (event timeline + currently active step + log tail).
- [x] `P0` Add top-nav user selector for scoped service views (`admin`, `land-core`, `gee-lee`).
- [ ] `P1` Add data dictionary draft generator path (LLM-backed; can be template-first).

## Week 3 - Demo + validation + release prep (P0/P1)
- [ ] `P0` Implement and verify 2-3 full workflows (include PRISM county case).
- [ ] `P0` Benchmark study (manual vs system):
  - [ ] authoring time
  - [ ] failure recovery time
  - [ ] reproducibility rerun consistency
- [ ] `P0` Repro package:
  - [ ] sample configs/pipelines
  - [ ] step-by-step commands
  - [ ] expected outputs
- [ ] `P1` Documentation polish:
  - [ ] 10-minute quickstart
  - [ ] troubleshooting section
  - [ ] architecture + limitations
- [ ] `P1` v0 release candidate:
  - [ ] tag
  - [ ] changelog
  - [ ] demo screenshots/video

## Pipeline-centric web IA (v1 target)
- [x] `P0` Route: `/` -> Operations dashboard (failed/running runs + quick resume/retry).
- [ ] `P0` Route: `/pipelines` -> pipeline catalog with search and status badges.
- [x] `P0` Route: `/pipelines/{pipeline_id}` -> pipeline detail + run/validate actions.
- [ ] `P0` Route: `/runs/{run_id}` -> run detail + artifacts + attempts/events.
- [x] `P1` Route: `/pipelines/new` and `/pipelines/{pipeline_id}/edit` -> draft builder with step test execution.
- [x] `P0` Route: `/runs/{run_id}/live` -> active run visualization.

## API backlog for web IA
- [ ] `P0` `GET /api/pipelines` (catalog + last status + failure rate window).
- [x] `P0` `GET /api/pipelines/{id}` (definition + summary stats).
- [x] `P0` `GET /api/pipelines/{id}/runs` (history + filters + pagination).
- [x] `P0` `GET /api/pipelines/{id}/validations` (latest and historical validation results).
- [x] `P0` `POST /api/pipelines/{id}/validate` (pipeline-scoped validate endpoint alias).
- [x] `P0` `POST /api/pipelines/{id}/run` (pipeline-scoped run endpoint alias).
- [x] `P0` `GET /api/runs/{id}/live` (polling or SSE payload for timeline/status).
- [x] `P1` `GET /api/builder/source` + `POST /api/builder/validate` + `POST /api/builder/test-step` for draft authoring loop.
- [x] `P1` `POST /api/pipelines` + `PUT /api/pipelines/{id}` for draft/save flows.
- [x] `P1` `POST /api/builder/generate` for LLM-assisted YAML draft generation.

## Cut list if schedule slips (defer first)
- [ ] `P2` Advanced UI styling
- [ ] `P2` Full authentication integration (OIDC/JWT/session).  
- [ ] `P2` Additional non-SLURM backends
- [ ] `P2` Advanced LLM planning heuristics
- [ ] `P2` Auto plugin catalog enrichment

## Exit criteria for 3-week success
- [ ] `P0` At least one external user can install and run end-to-end in under 30 minutes.
- [ ] `P0` Failed run recovery works via resume without manual DB edits.
- [ ] `P0` Every run has reproducibility metadata (Git + checksums).
- [ ] `P0` CI is green for unit + integration test lanes.
- [ ] `P0` Offline mode: run_batch continues when DB is unavailable, then backfills missing tracking/events later.
- [ ] `P0` Runs should check code into github, deploy code to working location, then run with explicit github version always. This will enable rerunning code at a specific version.

# things to fix later
- [ ] Add first-class `Project` object with repo bindings and project-level variables.
  - Goal:
    - represent a project as a durable object (not just `project_id` tags on runs);
    - bind project to source repos (pipelines/scripts and data dictionary);
    - add project-scoped variable layer in resolver chain.
  - Proposed data model:
    - keep `etl_projects` as canonical project identity;
    - add `etl_project_repos` table:
      - `project_id` (FK -> `etl_projects.project_id`, PK/unique);
      - `pipelines_repo_provider`, `pipelines_repo_owner`, `pipelines_repo_name`, `pipelines_repo_ref` (branch/tag/sha default);
      - `scripts_repo_provider`, `scripts_repo_owner`, `scripts_repo_name`, `scripts_repo_ref` (optional; can equal pipelines repo);
      - `dictionary_repo_key` (FK-ish logical key to `etl_dictionary_repos.repo_key`);
      - `is_active`, timestamps;
    - add `etl_project_vars` table for key/value config:
      - `project_id`, `var_key`, `var_value_json`, `is_secret`, timestamps;
      - unique (`project_id`, `var_key`);
      - store JSON to support scalar/list/object project vars.
  - Resolver precedence change (target):
    - `global -> environment -> project -> pipeline -> commandline`;
    - commandline includes explicit run overrides (`--project-id`, `--workdir`, future `--set key=value` / `--vars-file`);
    - expose namespaces in context as:
      - `global.*`, `env.*`, `project.*`, `pipe.*`, plus final flat overlay for backward compatibility.
  - Parser/runner/API changes:
    - add optional pipeline metadata key `project_vars` only for local defaults; DB-backed `project` values take precedence over these defaults;
    - runner resolves project config once per run and stamps effective project config hash in run provenance;
    - web/API/CLI add project CRUD endpoints/commands (`etl projects list/show/upsert`, repo binding update, vars update);
    - update project access checks so project metadata and repo bindings respect `etl_user_projects`.
  - Integration behavior:
    - `dataset_dictionary_pr` can default `repo_key` from project binding when omitted;
    - catalog sync/resolve plugins can default `catalog_repo`/dictionary repo from project binding;
    - remote execution source selection can prefer project repo bindings when enabled.
  - Migration and rollout:
    - backfill default bindings for existing `land_core`, `gee_lee`, and `default`;
    - keep current behavior if project bindings/vars are absent;
    - emit warnings when pipeline uses hardcoded repo paths that conflict with project binding;
    - add tests for precedence ordering and project binding fallback paths.
- [ ] Stat collection on plugin run times is not complete.  We need runtime by pipeline-step, not plugin to right size steps.  case. unzip 250mb file, or 1mb file.  they take significantly different runtimes/hardware.
- [ ] Add GitHub data dictionary write workflow: auto-create branch, write/update dataset YAML, commit, push, open PR, require human review before merge.
- [ ] Pipeline builder: add `hpcc_direct` as an executor option everywhere executor is selected.
- [ ] Pipeline builder: allow overriding `sys.*` runtime values with static user-provided values for deterministic testing.
- [ ] Evaluate single-step execution mode with persisted step state/outputs so users can resume and continue from current state into next steps.
- [ ] Add optional step-scoped dependency environments to reduce `.venv` bloat and dependency collisions:
  - keep current shared environment mode as default;
  - add `env_mode: shared|per_step` (executor/pipeline setting);
  - resolve per-step deps from plugin metadata (`meta.deps`) + optional step overrides;
  - build/reuse cached envs by dependency hash;
  - execute each step inside its resolved env and log env hash/provenance for reproducibility.
- [ ] Investigate first-class masked variable objects for secrets (replace raw string dict model).
  - Problem: current masking relies on string replacement dictionaries and can miss unsafe stringification paths.
  - Goal: represent sensitive values as typed variable objects (for example `SecretValue`) with controlled `__str__`/print/log behavior.
  - Design direction:
    - keep real secret value accessible only via explicit method (`reveal()` / `value()`), not implicit string conversion;
    - default string/render output should be masked token (for example `"[REDACTED]"`);
    - provide safe serialization modes (`masked`, `for_runtime_env`) to prevent accidental leakage.
  - Integration points:
    - variable resolver namespaces (`secret.*`, future project/env secret scopes),
    - runner logs and plugin logs,
    - diagnostics/error reports, web API payloads, and tracking DB writes.
  - Migration:
    - add compatibility layer so existing plugins expecting strings still work in runtime env injection;
    - incrementally adopt typed secrets in parser/runner first, then plugin contract updates.
- [ ] Add structured progress reporting/progress bars for long-running steps.
  - Goal: show live step progress in CLI/web for long tasks instead of only start/end logs.
  - Add plugin progress API (for example `ctx.progress(current, total, message=None)`), plus optional named phases.
  - Support loop-style reporting (`iteration i of N`) and unknown-total mode (`spinner` / indeterminate progress).
  - Persist progress events in run tracking so progress survives refresh/reconnect and is visible in live run views.
  - Render as:
    - CLI: single-line progress bar with percent + current/total + ETA when total is known.
    - Web: per-step progress bar + latest progress message.
  - Backward compatibility:
    - plugins that do not report progress continue to work unchanged.

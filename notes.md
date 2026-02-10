# 3-Week Product Plan (ResearchETL v0)

Last updated: 2026-02-09

## Current status snapshot
- [x] `P0` Core engine + retry/resume/provenance + SLURM event tracking.
- [x] `P0` Minimal web UI/API scaffold is live.
- [x] `P0` Web supports run list/detail, artifact browsing, resume action (local), and run/validate actions.
- [x] `P0` Test suite green locally (`47 passed`).
- [ ] `P0` Pipeline-centric web UX (catalog/detail/builder/live view) not yet implemented.

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
- [ ] `P0` `GET /api/pipelines/{id}/validations` (latest and historical validation results).
- [x] `P0` `POST /api/pipelines/{id}/validate` (pipeline-scoped validate endpoint alias).
- [x] `P0` `POST /api/pipelines/{id}/run` (pipeline-scoped run endpoint alias).
- [x] `P0` `GET /api/runs/{id}/live` (polling or SSE payload for timeline/status).
- [x] `P1` `GET /api/builder/source` + `POST /api/builder/validate` + `POST /api/builder/test-step` for draft authoring loop.
- [x] `P1` `POST /api/pipelines` + `PUT /api/pipelines/{id}` for draft/save flows.
- [x] `P1` `POST /api/builder/generate` for LLM-assisted YAML draft generation.

## Cut list if schedule slips (defer first)
- [ ] `P2` Advanced UI styling
- [ ] `P2` Multi-user auth
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

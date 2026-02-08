# 3-Week Product Plan (ResearchETL v0)

## Week 1 - Core hardening (P0)
- [x] `P0` Packaging/install flow complete (`pyproject.toml`, `etl` console entrypoint, install docs validated).
- [ ] `P0` Retry/resume edge cases verified for `parallel_with`, `foreach`, and SLURM array batches.
- [ ] `P0` Error handling pass: classify common failures and return clear actionable messages.
- [ ] `P0` Provenance completeness check: all run paths (local/slurm/run_batch/resume) persist Git+checksum metadata.
- [ ] `P0` Integration tests for:
  - [ ] `P0` SLURM event transitions (`queued -> running -> completed/failed`).
  - [ ] `P0` Resume from partial success.
  - [ ] `P0` Retry attempts persisted with `attempt_no > 1`.

## Week 2 - Minimal product interface (P0/P1)
- [ ] `P0` Build thin interface (CLI wizard or minimal web UI) for:
  - [ ] intent input
  - [ ] generated pipeline preview/edit
  - [ ] validate + run action
- [ ] `P0` Status view/table with run states and step-attempt details.
- [ ] `P0` Add a single "resume failed run" action.
- [ ] `P1` Add data dictionary draft generator path (LLM-backed; can be template-first).
- [ ] `P1` Add provenance view (commit/branch/dirty/checksums) per run.

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

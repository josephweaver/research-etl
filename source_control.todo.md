# Source Control Abstraction TODO

## Objective
Introduce a provider interface so execution/provenance can support multiple source-control systems (Git first, others later) without rewriting executors.

## Why
- Current implementation is Git-specific in multiple places (`resolve_execution_spec`, repo-relative mapping, checkout preparation).
- We want a stable contract so swapping providers (or adding archive-only providers) is isolated to one adapter class.

## Phase 1: Interface + Git Adapter (No Behavior Change)

- [x] Add provider interface (`SourceProvider`) with normalized spec model.
- [x] Add Git-backed adapter (`GitSourceProvider`) that wraps existing `etl.git_checkout` functions.
- [x] Keep all existing runtime paths unchanged (executors still call existing helpers).
- [x] Add focused adapter tests.

### Phase 1 Deliverables
- `etl/source_control/base.py`
- `etl/source_control/git_provider.py`
- `etl/source_control/__init__.py`
- tests for provider behavior and conversion.

## Phase 2: Integrate Providers Into Runtime

- [ ] Add provider factory/registry (`provider=git` default).
- [x] Update executors (`local/slurm/hpcc_direct`) to use provider interface (Git provider shim, compatibility preserved).
- [x] Update provenance collection to consume provider metadata (backward-compatible `git_*` fields retained).
- [ ] Preserve existing CLI/config behavior and outputs.

## Phase 3: Add Second Provider (Proof)

- [ ] Implement one non-Git provider (for example `archive` or `workspace_only`) to validate abstraction.
- [ ] Add compatibility tests across providers.

## Phase 4: Configuration + UX

- [ ] Add config field (for example `source_control.provider: git`).
- [ ] Add diagnostics endpoint/command to show selected provider and capabilities.
- [ ] Surface provider info in run provenance/UI.

## Design Notes
- Keep a normalized `SourceExecutionSpec` model:
  - `provider`
  - `revision`
  - `origin_url`
  - `repo_name`
  - `is_dirty`
  - `extra` (provider-specific)
- Provider methods should cover:
  - resolve execution spec
  - repo-relative path mapping
  - checkout preparation (remote/bundle/snapshot/workspace modes)

## Non-Goals (Phase 1)
- No executor behavior changes.
- No schema changes.
- No CLI flag changes yet.

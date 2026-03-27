# Parallel Execution TODO

## Objective
Support mixed execution patterns where:
- coarse units (for example years) run in parallel,
- fine units (for example days in a year) run sequentially inside one job,
- execution backend can be selected (`slurm`, `process`, `thread`, `serial`) per fan-out context.

## Gap Closures (Current Direction)
- [x] Add child pipeline execution plugin concept with two modes:
  - `synchronized` (block until child finishes)
  - `fire_and_forget` (submit child and return immediately)
- [x] Keep child execution step parallelizable so parent `foreach` can fan out by year.
- [ ] Persist parent-child run linkage in tracking/UI (currently plugin can return metadata, but linkage is not first-class in DB model yet).
- [ ] Add cancellation propagation semantics (parent stop should optionally stop spawned child runs).
- [ ] Add stable plan + policy fields (`foreach_mode`, `placement`, backend selector) in parser/schema.

## Problem Statement
- Current `foreach` fan-out is scheduler-oriented; even with low concurrency, each item may still become an independent scheduled unit.
- For workloads like PRISM daily (365 files/year, ~10s each), scheduler overhead dominates if each day is a separate SLURM task.
- Desired pattern:
  - `20 years` in parallel,
  - each year processes `365 days` sequentially in one job.

## Candidate Features

### 1) Child pipeline step (synchronous)
- [ ] Add a step type/plugin that runs another pipeline and blocks until child completion.
- [ ] Parent step returns child run metadata (`child_run_id`, `status`, `artifact_dir`, summary metrics).
- [ ] Support passing scoped vars/dirs/env into child execution.

Why:
- Provides hierarchical orchestration without forcing nested `foreach` semantics immediately.
- Clean fit for year-level fanout: one parent item -> one child pipeline run.

### 2) Foreach mode controls
- [ ] Add explicit foreach execution mode:
  - `mode: parallel | serial`
- [ ] Add item placement policy:
  - `placement: separate_jobs | same_job`

Expected combinations:
- `parallel + separate_jobs` -> current SLURM-style fanout.
- `serial + same_job` -> one job loops all items in order.

### 3) Backend selection for parallelism
- [ ] Introduce backend selector for fanout groups:
  - `foreach_backend: slurm_array | process_pool | thread_pool | serial`
- [ ] Keep `max_concurrency` semantics backend-aware.

Guidance:
- `thread_pool`: I/O-bound plugin steps.
- `process_pool`: CPU-bound Python/native work.
- `slurm_array`: cluster-scale distribution.
- `serial`: deterministic single-worker loop.

## Proposed YAML Direction (Draft)
```yaml
steps:
  - name: process_year
    foreach: years
    foreach_mode: parallel
    foreach_backend: slurm_array
    max_concurrency: 20
    plugin: pipeline_run.py
    args:
      pipeline: pipelines/prism/year_daily_aggregate.yml
      vars:
        year: "{item}"

# child pipeline:
steps:
  - name: aggregate_day
    foreach: days
    foreach_mode: serial
    foreach_backend: serial
    placement: same_job
    plugin: geo/raster_aggregate_by_polygon.py
    args: ...
```

## Safety / Determinism Guardrails
- [ ] No pipeline recursion/cycles (`A -> B -> A`).
- [ ] Persist parent-child linkage in tracking (`parent_run_id`, `parent_step_id`, `child_run_id`).
- [ ] Single retry owner policy:
  - either parent retries child as a unit, or child handles retries internally.
- [ ] Materialize and persist resolved plan before execution for resume/audit consistency.
- [ ] Enforce caps (`max_depth`, `max_items`, `max_concurrency`).

## SLURM-Specific Requirements
- [ ] For `serial + same_job`, generate one SLURM job script that loops item list.
- [ ] For `parallel + separate_jobs`, keep current array/batch generation.
- [ ] Add telemetry-driven optional packing later (bundle short tasks into target walltime blocks).

## Tracking / UI
- [ ] Show nested run relationships in API/UI (parent run -> child runs).
- [ ] Surface backend and placement choice in step details.
- [ ] Report planned vs actual fanout counts.

## Implementation Phases

### Phase 1 (lowest risk)
- [ ] Child pipeline step (sync) for local executor.
- [ ] Tracking linkage fields + cycle detection.

### Phase 2
- [ ] SLURM support for child pipeline step.
- [ ] `foreach_mode` + `placement` schema/validation.

### Phase 3
- [ ] `thread_pool` / `process_pool` backend selection.
- [ ] Backend-aware scheduling and per-backend retry policy docs.

### Phase 4
- [ ] Plan materialization UX + preview.
- [ ] Optional runtime-history-based packing optimization.

## Open Questions
- [ ] Should child pipeline run share parent workdir root or isolated sibling run dir?
- [ ] How should cancellation propagate (parent stop -> child stop)?
- [ ] Do we allow child pipeline to inherit `requires_pipelines`, or flatten dependencies at parent level?
- [ ] What is the default retry owner for parent/child orchestration?

# Stepper TODO (Builder Step-Test Session)

## Goal
- Make builder `Test Step` behave like one contiguous run when executed step-by-step (especially with `hpcc_direct`).
- Preserve and reload variable-solver state across step tests and UI refresh/restart.

## Phase 1: Session Model
- Define a `builder step-test session` identity:
  - `session_id` (default to `run_id`)
  - `run_id`
  - `run_started_at`
  - `pipeline`
  - `project_id`
  - `env`
  - `executor`
- Add session lifecycle:
  - `new session`
  - `load session`
  - `close/archive session`

## Phase 2: Persisted Solver Layers
- Persist solver state as layers (not just flattened resolved output):
  - `global`
  - `env`
  - `project`
  - `pipe` (pipeline vars/dirs snapshot at session start)
  - `step_outputs` (mutable)
  - `step_meta` (optional: last success/run metadata)
- File location:
  - `.runs/builder/sessions/<session_id>/solver_state.json`
- Store both:
  - `layers_raw` (authoritative)
  - `context_resolved` (cache only; rebuildable)
- On successful step test:
  - upsert/overwrite `step_outputs[output_var] = outputs`
  - update `step_meta[step_name]`
  - write `solver_state.json` atomically

## Phase 3: Shared Context Between Steps
- Use one context file per session:
  - `.runs/builder/sessions/<session_id>/context.json`
- Ensure remote step execution reuses same context file:
  - plumb `context_file` through builder test-step payload/context
  - update `hpcc_direct` executor to pass `--context-file` to `etl.run_batch`
- Continue using `step_indices=[selected_step]` per click.

## Phase 4: Backend APIs
- Add builder session endpoints:
  - `GET /api/builder/sessions` (sorted started_at desc)
  - `POST /api/builder/sessions` (create)
  - `GET /api/builder/sessions/{id}` (load state)
  - `POST /api/builder/sessions/{id}/select` (optional convenience)
- Add backend helpers:
  - `load_solver_state(session_id)`
  - `save_solver_state(session_id, state)`
  - `merge_step_success(session_id, step, outputs, metadata)`

## Phase 5: UI/UX
- Add session selector (combobox) in builder:
  - first option: `New Session`
  - then prior sessions sorted by date desc
- On session select:
  - load `run_id/run_started_at`
  - load solver layers
  - refresh builder namespace/preview with restored context
- Show session status summary:
  - active session id
  - last successful step
  - last updated timestamp

## Phase 6: Tracking and Optional DB Mirror
- Keep filesystem as required baseline source of truth.
- Optional DB mirror tables for multi-user/history:
  - `etl_builder_sessions`
  - `etl_builder_session_steps`
- DB mirror should be best-effort and not block step testing.

## Phase 7: Correctness/Behavior
- Treat builder session completion separately from normal pipeline run completion.
- Guardrails:
  - if pipeline hash changed, warn before reusing old session
  - if env/project changed, require explicit confirmation/reset
  - if selected step depends on missing output_var, show clear preflight message

## Phase 8: Tests
- Unit tests:
  - solver layer save/load roundtrip
  - merge overwrite semantics for step rerun
  - namespace rebuild from persisted layers
- API tests:
  - session create/list/load
  - step success updates persisted state
  - step test with same session uses same run seed/context path
- Executor tests:
  - `hpcc_direct` command includes `--context-file`

## Suggested Delivery Order
1. `hpcc_direct` `--context-file` plumbing + session folder creation.
2. Solver state persistence helpers and backend integration on step success.
3. Session list/load APIs.
4. UI combobox + restore behavior.
5. Tests + polish.

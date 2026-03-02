# Query TODO

## Current Progress (2026-03-02)

Completed:

- Executor capability discovery implemented in code:
  - `Executor.capabilities()` added with default flags.
  - Implemented in:
    - `etl/executors/base.py`
    - `etl/executors/local.py`
    - `etl/executors/hpcc_direct.py`
    - `etl/executors/slurm/executor.py`
- Read API endpoint implemented:
  - `GET /api/executors/capabilities`
  - Wired through:
    - `etl/web/routes/api_read.py`
    - `etl/web_api.py`
- Endpoint sanity-tested locally (returns 200 and expected capability payload).
- Query foundation implemented:
  - shared query package and exports:
    - `etl/query/__init__.py`
  - shared typed query errors:
    - `etl/query/errors.py`
    - `QueryError`, `QueryPlannerError`, `QueryExecutionError`, `QueryTransportError`
  - `query_spec` validation/normalization:
    - `etl/query/spec.py`
  - DuckDB planner (`query_spec` -> SQL + params):
    - `etl/query/planner_duckdb.py`
  - shared DuckDB runner:
    - `etl/query/runners/duckdb_runner.py`
- Executor query contract + local implementation:
  - `Executor.query_data(...)` added:
    - `etl/executors/base.py`
  - local executor query enabled + wired to spec/planner/runner:
    - `etl/executors/local.py`
- Query API endpoint implemented:
  - `POST /api/query/preview`
  - wired through:
    - `etl/web/routes/api_query.py`
    - `etl/web/query_handlers.py`
    - `etl/web_api.py`
  - includes capability preflight (`query_data`) and typed error mapping:
    - `planner_error` -> `400`
    - `execution_error` -> `422`
    - `transport_error` -> `502`
- Tests added for query foundation and API behavior:
  - `tests/test_query_spec.py`
  - `tests/test_query_planner_duckdb.py`
  - `tests/test_query_runner_duckdb.py`
  - `tests/test_local_executor_query.py`
  - `tests/test_web_api.py` (`/api/query/preview` cases)

Current capability state:

- `local`: `artifact_tree=true`, `artifact_file=true`, `cancel=false`, `query_data=true`
- `slurm`: `artifact_tree=true`, `artifact_file=true`, `cancel=false`, `query_data=false`
- `hpcc_direct`: `artifact_tree=false`, `artifact_file=false`, `cancel=false`, `query_data=false`

Not started yet:

- `hpcc_direct.query_data` implementation (remote query execution path)
- `etl/query/remote_entry.py` for structured remote JSON in/out
- remote error propagation preserving `error_code` + `detail`
- optional additional query API endpoints (`/api/query/schema`, `/api/query/profile`)
- UI integration for capability-driven enable/disable and live query preview
- query governance hardening (path allowlists, row/time limits, audit logging)

## Goal

Make data querying a first-class ETL capability for interactive UI workflows, with an engine-agnostic interface and executor-specific implementation.

## Core Direction

- Add `query_data` as a formal executor capability (not just `run`/`status`).
- Keep UI/API query requests engine-agnostic via a predefined `query_spec` format.
- Compile `query_spec` into DuckDB SQL in shared query modules.
- Reuse one shared DuckDB runner module so executors do not duplicate query logic.
- Store reusable query/transform specs in `queries/` directories in pipeline asset repos:
  - `../shared-etl-pipelines/queries`
  - `../landcore-etl-pipelines/queries`

## Why

- Interactive transform-builder preview requires low-latency query responses.
- SLURM queue latency is too high for exploratory UI operations.
- `hpcc_direct` is a good fit for remote interactive execution where data lives.
- Artifact retrieval is useful, but insufficient alone for query-preview UX.

## Proposed Architecture

1. `etl/query/spec.py`
   - Define/validate query DSL (`query_spec`) schema.
2. `etl/query/planner_duckdb.py`
   - Translate `query_spec` into SQL + parameters.
3. `etl/query/runners/duckdb_runner.py`
   - Execute SQL in DuckDB and return normalized result payload.
4. `etl/query/remote_entry.py`
   - Remote entrypoint for executor calls (JSON in/out).
5. Executor integration
   - `local`: in-process `query_data`.
   - `hpcc_direct`: remote command execution (`python -m etl.query.remote_entry ...`).
   - `slurm`: optional later for non-interactive/heavy query jobs.

## Executor Contract (proposed)

- Keep current `submit/status` flow unchanged for pipeline runs.
- Add capability discovery on executors:
  - `capabilities() -> dict[str, bool]`
  - minimum keys:
    - `query_data`
    - `artifact_tree`
    - `artifact_file`
    - `cancel`
- Add optional query method:
  - `query_data(query_spec, ctx) -> query_result`
- Capability checks happen before calling optional methods:
  - UI/API can preflight once and disable unsupported actions.
  - Executors that do not support a method still raise `NotImplementedError` as a safety fallback.
- Initial expected support:
  - `local`: `query_data=true`
  - `hpcc_direct`: `query_data=true`
  - `slurm`: `query_data=false` (until async/heavy-query mode is added)

## API/UI Plan

- New API endpoint family:
  - `POST /api/query/preview`
  - optional `POST /api/query/schema`, `POST /api/query/profile`
- UI sends `query_spec`, never raw SQL.
- Response shape (example):
  - `columns`
  - `rows`
  - `row_count_estimate`
  - `elapsed_ms`
  - `engine` / `executor`

## Security + Governance

- No arbitrary SQL from UI.
- Strict allowlist of operations/functions in `query_spec`.
- Enforce project/user scope on data locations.
- Enforce path allowlists and optional dataset registry mapping.
- Query time/row/size limits for interactive endpoints.
- Log query requests and responses metadata for provenance/audit.

## Phased Delivery

1. Phase 1 (MVP)
   - `query_spec` v1 (select/filter/limit/order/derive basic).
   - DuckDB planner + runner.
   - `hpcc_direct` + local executor support.
   - `POST /api/query/preview`.
2. Phase 2
   - Transform-builder UI integration (live preview widgets).
   - Persist/replay query specs in builder sessions.
3. Phase 3
   - Extended operations (joins, group agg, window, pivot).
   - Optional asynchronous heavy-query mode.

## Open Questions

- Where to store query specs/versioning (builder sessions only vs dedicated table)?
- Should `query_data` support parameterized source aliases from dataset registry?
- Should query capability metadata be exposed at `GET /api/executors/capabilities` only, or also on builder/env endpoints?

## Error Model Clarification

Goal: avoid returning generic failures by distinguishing where a query failed.

Suggested categories:

1. Planner errors
   - Invalid/unsupported `query_spec`, missing columns, illegal operations.
   - Example class: `QueryPlannerError`.
2. Runner/engine errors
   - DuckDB execution failures, file read issues, SQL runtime failures.
   - Example class: `QueryExecutionError`.
3. Transport/executor errors
   - SSH/connectivity/timeouts/remote invocation failures.
   - Example class: `QueryTransportError`.

API can map these to stable error codes for UI:
- `planner_error`
- `execution_error`
- `transport_error`

Recommended HTTP mapping:
- `planner_error` -> `400`
- `execution_error` -> `422` (or `500` for unexpected engine faults)
- `transport_error` -> `502`/`504`

## Error Capture Implementation (high priority)

Implement this early; low effort and high operational value.

1. Define shared query exceptions
   - `QueryError` (base)
   - `QueryPlannerError(QueryError)`
   - `QueryExecutionError(QueryError)`
   - `QueryTransportError(QueryError)`
2. Raise planner errors only from spec validation/planning layer
   - invalid operation names
   - missing required fields
   - bad references (columns/functions)
3. Wrap DuckDB/runtime failures as execution errors
   - SQL runtime error
   - file/path read failures
   - type conversion/runtime expression issues
4. Wrap executor/remote failures as transport errors
   - SSH failures
   - remote process non-zero with no engine payload
   - timeouts/network disconnects
5. Standardize API error response shape
   - `error_code`: `planner_error|execution_error|transport_error`
   - `message`: user-facing summary
   - `detail`: optional structured debug payload
   - `run_id` / `request_id`: correlation ids when available
6. Ensure logs include layer + error class + correlation id
   - Example log fields:
     - `query_error_class`
     - `query_error_code`
     - `executor`
     - `project_id`
     - `query_request_id`
7. Add explicit error boundary points
   - planner boundary: `spec -> logical plan/sql`
   - runner boundary: `logical plan/sql -> result`
   - transport boundary: `executor call -> remote process/io`
8. Preserve machine-readable causes across boundaries
   - remote runner returns structured error payload
   - executor preserves `error_code` and nested `detail` when rethrowing
9. UI handling
   - `planner_error`: show inline form/spec validation message
   - `execution_error`: show query/runtime message with step context
   - `transport_error`: show retry guidance / connectivity status

## Capability Flags Rationale

- This is low-effort and high-leverage for developer UX.
- It avoids trial-and-error calls for unsupported features.
- It creates a stable contract for future optional features (query, profiling, schema inspect, exports).
- This mirrors established feature-query patterns (device/runtime capability probing).

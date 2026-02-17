# Dataset-First ETL Redesign Plan

## Goal
Implement a dataset-first model in ETL so users can call `get_data()` and `store_data()` with qualified dataset IDs while protocol, environment routing, and operational tracking are handled by the platform.

## Why This Plan
- Keep human-governed meaning in a dictionary (Git-reviewed YAML).
- Keep mutable operational truth in DB tables (versions, locations, checksums, lineage).
- Present a unified Datasets UX by composing both at read time.

## Guiding Principles
- Dataset IDs are stable and human-meaningful.
- Dataset versions are explicit and immutable once published.
- Location/protocol details are internal; analysts never need transport knowledge.
- AI-authored dictionary content is draft-first with validation + review gates.
- Runtime events must be queryable and audit-safe.

## Scope Boundaries
- In scope: dataset registry, resolve/get/store/publish flow, UI views, migration path.
- Out of scope (v1): full automatic schema inference for every format, cross-org auth model redesign, hard delete lifecycle automation.

## Target Model

### Logical Metadata (Dictionary, Git)
- `dataset_id` (stable key, e.g. `serve.yanroy`)
- title, description, steward/owner, domain tags
- expected schema/profile references
- quality/usage notes
- policy hints (class, canonical location type)
- AI provenance fields (`generated_by`, model, prompt/spec version, reviewed_by)

### Operational Metadata (DB)
- Dataset existence/status
- Version records (`v1`, timestamped, or semantic labels)
- Physical locations per version (uri/path, env, location type, canonical flag)
- Artifact facts (checksum, bytes, created_at, retention status)
- Run lineage (which run produced/promoted the version)
- Events (published, promoted, moved, deprecated, policy violation)

## Proposed DB Schema (v1)

### 1) `etl_datasets`
- `dataset_id` (PK)
- `data_class`
- `owner_user`
- `status` (`active|deprecated|retired`)
- `created_at`, `updated_at`

### 2) `etl_dataset_versions`
- `dataset_version_id` (PK)
- `dataset_id` (FK -> `etl_datasets`)
- `version_label`
- `is_immutable` (bool)
- `schema_hash` (nullable)
- `created_by_run_id` (nullable FK -> `etl_runs`)
- `created_at`
- Unique: (`dataset_id`, `version_label`)

### 3) `etl_dataset_locations`
- `dataset_location_id` (PK)
- `dataset_version_id` (FK -> `etl_dataset_versions`)
- `environment` (e.g. `local`, `hpcc_alpha`)
- `location_type` (e.g. `local_cache`, `gdrive`, `hpcc_cache`)
- `uri`
- `is_canonical` (bool)
- `checksum` (nullable)
- `size_bytes` (nullable)
- `created_at`

### 4) `etl_dataset_events`
- `dataset_event_id` (PK)
- `dataset_id`
- `version_label` (nullable)
- `event_type` (`created|stored|published|promoted|moved|deprecated|policy_violation`)
- `run_id` (nullable FK -> `etl_runs`)
- `payload_json`
- `created_at`

### 5) `etl_dataset_dictionary_refs` (optional but useful)
- `dataset_id` (PK/FK)
- `dict_path` (yaml file path)
- `dict_sha` (git commit or blob hash)
- `last_validated_at`

## Dictionary File Design (v1)
- Directory: `catalog/datasets/<dataset_id>.yml`
- Rule: one file per dataset ID.
- No mutable physical location fields in dictionary files.
- Required fields:
  - `dataset_id`
  - `title`
  - `owner`
  - `data_class`
  - `description`
  - `expected_schema` (or explicit null with rationale)
  - `ai_provenance` block when AI-generated
  - `review` block (`status`, `reviewed_by`, `reviewed_at`)

## API and Service Layer

### Core service module
- `etl/datasets/service.py`
- Responsibilities:
  - `resolve(dataset_id, version="latest", intent="analysis")`
  - `get_data(...)`
  - `store_data(...)`
  - `publish_version(...)`
  - policy and route validation

### Routing module
- `etl/datasets/routing.py`
- Maintain deterministic transfer matrix:
  - source runtime context + target location type -> transport
  - Fail fast on unmapped routes.
- Optional override: explicit transport for edge cases.

### Transport adapters
- `etl/datasets/transports/*.py`
- Start with:
  - local filesystem copy/move
  - `rclone` adapter
  - `scp/rsync` adapter
- Uniform interface: `put()`, `get()`, `verify()`.

## `get_data()` / `store_data()` UX Contract
- Minimal analyst-facing signatures:
  - `get_data(dataset_id, version="latest", intent="analysis") -> local_path_or_handle`
  - `store_data(path, dataset_id, stage="staging", version=None) -> receipt`
- No protocol args required for analysts.
- Return receipt includes:
  - resolved version
  - target URI
  - transport used
  - checksum/status

## CLI Additions
- `etl datasets list`
- `etl datasets show <dataset_id>`
- `etl datasets get <dataset_id> [--version latest] [--intent analysis]`
- `etl datasets store <dataset_id> --path <local_path> [--stage staging]`
- `etl datasets publish <dataset_id> --version <label>`
- `etl datasets doctor <dataset_id>` (consistency/drift checks)

## UI Additions: Datasets
- New nav route: `/datasets`
- Dataset detail tabs:
  - Definition (dictionary content)
  - Versions
  - Locations
  - Lineage
  - Health (policy violations, drift, freshness)
- Data source approach: JIT composed read model by `dataset_id`.

## AI-Generated Dictionary Governance
- AI output lands as draft (`review.status=draft`).
- Validation gates before merge:
  - schema compliance
  - required fields
  - ID formatting
  - owner/data_class validity
- Human review required to mark `approved`.
- Persist provenance fields for audit.

## Migration Strategy (Phased)

### Phase 0 - Discovery and Design Freeze
- Inventory existing dataset IDs in pipelines/plugins/catalog.
- Define canonical ID and version rules.
- Decide `latest` resolution policy.

### Phase 1 - Schema + Read-Only Registry
- Add DB migrations for dataset tables.
- Build read-only service endpoints and CLI list/show.
- No pipeline behavior changes yet.

### Phase 2 - `store_data()` Backbone
- Implement routing matrix + transport adapters.
- Register stored outputs to dataset tables/events.
- Add checksums and policy validation at write time.

### Phase 3 - `get_data()` Resolution
- Implement version/location resolver.
- Add environment-aware retrieval strategy.
- Introduce cache rules (optional) for local pulls.

### Phase 4 - Pipeline Integration
- Allow step args to accept dataset refs in addition to file paths.
- Add helper plugins/wrappers for dataset IO.
- Preserve backward compatibility for current pipelines.

### Phase 5 - Datasets UI
- Implement `/datasets` list/detail with JIT composition.
- Add health/drift indicators and event timeline.

### Phase 6 - AI Dictionary Workflow
- Add AI draft generation pipeline target for dictionary YAML.
- Enforce review/approval gate in CI.
- Add drift reporting (dictionary schema claims vs observed runtime).

## Acceptance Criteria
- Analysts can move data using only dataset IDs via `get_data/store_data`.
- Location/protocol is inferred or explicitly rejected with clear errors.
- Every stored dataset version has lineage to run/event records.
- Dictionary and operational state are visible in one UI view.
- AI-authored entries cannot become authoritative without validation + review.
- Existing pipelines continue working during migration.

## Risks and Mitigations
- Risk: version semantics confusion.
  - Mitigation: define one labeling strategy and enforce uniqueness.
- Risk: route gaps across environments.
  - Mitigation: explicit transfer matrix + startup validation.
- Risk: dictionary/runtime drift.
  - Mitigation: scheduled drift checks + UI health flags.
- Risk: transport tool variability (`rclone`, `scp`, etc.).
  - Mitigation: adapter capability checks and actionable error messages.

## Suggested Implementation Order for Codex Sessions
1. Create DB migrations and tests for core dataset tables.
2. Add `etl/datasets/service.py` with list/show/resolve.
3. Add `etl datasets list/show` CLI commands.
4. Implement routing matrix + local/rclone transport adapters.
5. Implement `store_data()` and event recording.
6. Implement `get_data()` with `latest` resolver and location preference rules.
7. Add `/datasets` UI list/detail and read model joins.
8. Add dictionary schema validator + AI draft review gate.
9. Add integration tests across local + SLURM-like environment configs.

## Immediate Next Task (Recommended)
Implement Phase 1 in one PR:
- DB migrations for `etl_datasets`, `etl_dataset_versions`, `etl_dataset_locations`, `etl_dataset_events`
- minimal dataset service (`list/show`)
- CLI commands (`etl datasets list/show`)
- tests for migration + basic queries

## Post-First-Publish Checklist
After first real dataset publish, complete these in order:

1. Seed dictionary repo configuration
- Insert production rows in `etl_dictionary_repos`.
- Verify `local_path` exists and is writable on runner hosts.

2. Run full end-to-end workflow
- Execute a real pipeline path: `dataset_store` -> `dataset_get` -> `dataset_dictionary_pr`.
- Verify DB rows in dataset/version/location/event and dictionary entry tables.
- Verify boundary logs and failure traces are captured in step logs.

3. Finalize GitHub auth and policy
- Choose auth mode (`GITHUB_TOKEN` or GitHub App) for PR creation.
- Confirm org SSO/approval and minimum permissions.
- Finalize branch naming, PR template, and required reviewers/checks.

4. UI/API operational completion
- Add trigger path in UI/API for dictionary PR creation from dataset detail.
- Show PR state (`open/merged/closed`) and repo/file mapping in datasets detail.

5. Reliability hardening
- Add retry/backoff for GitHub API calls.
- Improve error classification for transfer/git/github failure types.
- Add alerts/reporting for failed boundary transfers and failed PR creation.

6. Governance and documentation
- Lock versioning policy (`latest` semantics, immutability rules).
- Lock multi-repo ownership and dataset-to-repo mapping conventions.
- Update runbook/docs with recovery steps and operational examples.

CREATE TABLE IF NOT EXISTS etl_projects (
    project_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_user_projects (
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES etl_projects(project_id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, project_id)
);

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS project_id TEXT;

CREATE INDEX IF NOT EXISTS etl_runs_project_started_idx
    ON etl_runs (project_id, started_at DESC);

ALTER TABLE etl_pipeline_validations
    ADD COLUMN IF NOT EXISTS project_id TEXT;

CREATE INDEX IF NOT EXISTS etl_pipeline_validations_project_pipeline_requested_idx
    ON etl_pipeline_validations (project_id, pipeline, requested_at DESC);

ALTER TABLE etl_artifacts
    ADD COLUMN IF NOT EXISTS project_id TEXT;

CREATE INDEX IF NOT EXISTS etl_artifacts_project_created_idx
    ON etl_artifacts (project_id, created_at DESC);

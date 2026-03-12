CREATE TABLE IF NOT EXISTS etl_query_workspaces (
    workspace_id BIGSERIAL PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES etl_projects(project_id) ON DELETE CASCADE,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('project', 'user')),
    scope_key TEXT NOT NULL DEFAULT '',
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source TEXT,
    updated_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_query_workspaces_unique_scope UNIQUE (project_id, scope_type, scope_key)
);

CREATE INDEX IF NOT EXISTS idx_etl_query_workspaces_project
    ON etl_query_workspaces (project_id, scope_type, updated_at DESC);

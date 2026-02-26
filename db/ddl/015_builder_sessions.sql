CREATE TABLE IF NOT EXISTS etl_builder_sessions (
    session_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    run_started_at TIMESTAMPTZ NULL,
    pipeline TEXT NULL,
    project_id TEXT NULL,
    env_name TEXT NULL,
    executor TEXT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    context_file TEXT NOT NULL,
    last_step_name TEXT NULL,
    last_step_index INTEGER NULL,
    last_result TEXT NULL,
    last_error TEXT NULL,
    last_updated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_builder_sessions_project_updated
    ON etl_builder_sessions (project_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_builder_sessions_pipeline_updated
    ON etl_builder_sessions (pipeline, updated_at DESC);

CREATE TABLE IF NOT EXISTS etl_builder_session_steps (
    step_event_id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES etl_builder_sessions(session_id) ON DELETE CASCADE,
    run_id TEXT NULL,
    step_name TEXT NULL,
    step_index INTEGER NULL,
    success BOOLEAN NOT NULL,
    error TEXT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_builder_session_steps_session_time
    ON etl_builder_session_steps (session_id, recorded_at DESC);

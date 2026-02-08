CREATE TABLE IF NOT EXISTS etl_runs (
    run_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    executor TEXT,
    artifact_dir TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_run_steps (
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    script TEXT,
    success BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    outputs_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, step_name),
    CONSTRAINT etl_run_steps_run_fk
        FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS etl_run_events (
    event_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details_json JSONB,
    CONSTRAINT etl_run_events_run_fk
        FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

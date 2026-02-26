CREATE TABLE IF NOT EXISTS etl_run_context_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    pipeline TEXT NULL,
    project_id TEXT NULL,
    executor TEXT NULL,
    event_type TEXT NOT NULL,
    step_name TEXT NULL,
    step_index INTEGER NULL,
    context_json JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_run_context_snapshots_run_time
    ON etl_run_context_snapshots (run_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_run_context_snapshots_run_step
    ON etl_run_context_snapshots (run_id, step_name, recorded_at DESC);

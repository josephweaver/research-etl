CREATE TABLE IF NOT EXISTS etl_run_step_attempts (
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    script TEXT,
    success BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    outputs_json JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, step_name, attempt_no),
    CONSTRAINT etl_run_step_attempts_run_fk
        FOREIGN KEY (run_id) REFERENCES etl_runs(run_id) ON DELETE CASCADE
);

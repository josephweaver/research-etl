CREATE TABLE IF NOT EXISTS etl_pipeline_validations (
    validation_id BIGSERIAL PRIMARY KEY,
    pipeline TEXT NOT NULL,
    valid BOOLEAN NOT NULL,
    step_count INTEGER NOT NULL DEFAULT 0,
    step_names_json JSONB,
    error TEXT,
    source TEXT NOT NULL DEFAULT 'web',
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS etl_pipeline_validations_pipeline_requested_idx
    ON etl_pipeline_validations (pipeline, requested_at DESC);

CREATE TABLE IF NOT EXISTS etl_artifacts (
    artifact_id BIGSERIAL PRIMARY KEY,
    artifact_key TEXT NOT NULL UNIQUE,
    artifact_class TEXT NOT NULL,
    pipeline TEXT,
    run_id TEXT,
    step_name TEXT,
    size_bytes BIGINT,
    checksum TEXT,
    metadata_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS etl_artifacts_class_created_idx
    ON etl_artifacts (artifact_class, created_at DESC);

CREATE INDEX IF NOT EXISTS etl_artifacts_pipeline_created_idx
    ON etl_artifacts (pipeline, created_at DESC);

CREATE TABLE IF NOT EXISTS etl_artifact_locations (
    location_id BIGSERIAL PRIMARY KEY,
    artifact_id BIGINT NOT NULL REFERENCES etl_artifacts(artifact_id) ON DELETE CASCADE,
    location_type TEXT NOT NULL,
    location_uri TEXT NOT NULL,
    is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL DEFAULT 'present',
    expires_at TIMESTAMPTZ,
    last_verified_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_artifact_locations_unique UNIQUE (artifact_id, location_type, location_uri)
);

CREATE INDEX IF NOT EXISTS etl_artifact_locations_artifact_idx
    ON etl_artifact_locations (artifact_id);

CREATE INDEX IF NOT EXISTS etl_artifact_locations_state_idx
    ON etl_artifact_locations (state, location_type);

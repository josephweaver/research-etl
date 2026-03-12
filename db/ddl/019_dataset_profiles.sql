CREATE TABLE IF NOT EXISTS etl_dataset_profiles (
    dataset_profile_id BIGSERIAL PRIMARY KEY,
    dataset_version_id BIGINT NOT NULL REFERENCES etl_dataset_versions(dataset_version_id) ON DELETE CASCADE,
    profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    inferred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_dataset_profiles_unique_version UNIQUE (dataset_version_id)
);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_profiles_version
    ON etl_dataset_profiles (dataset_version_id, updated_at DESC);

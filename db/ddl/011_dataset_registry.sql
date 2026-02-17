CREATE TABLE IF NOT EXISTS etl_datasets (
    dataset_id TEXT PRIMARY KEY,
    data_class TEXT,
    owner_user TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_dataset_versions (
    dataset_version_id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES etl_datasets(dataset_id) ON DELETE CASCADE,
    version_label TEXT NOT NULL,
    is_immutable BOOLEAN NOT NULL DEFAULT TRUE,
    schema_hash TEXT,
    created_by_run_id TEXT REFERENCES etl_runs(run_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_dataset_versions_unique UNIQUE (dataset_id, version_label)
);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_versions_dataset_created
    ON etl_dataset_versions (dataset_id, created_at DESC);

CREATE TABLE IF NOT EXISTS etl_dataset_locations (
    dataset_location_id BIGSERIAL PRIMARY KEY,
    dataset_version_id BIGINT NOT NULL REFERENCES etl_dataset_versions(dataset_version_id) ON DELETE CASCADE,
    environment TEXT,
    location_type TEXT NOT NULL,
    uri TEXT NOT NULL,
    is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
    checksum TEXT,
    size_bytes BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_dataset_locations_unique UNIQUE (dataset_version_id, environment, location_type, uri)
);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_locations_version
    ON etl_dataset_locations (dataset_version_id);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_locations_canonical
    ON etl_dataset_locations (is_canonical, location_type);

CREATE TABLE IF NOT EXISTS etl_dataset_events (
    dataset_event_id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES etl_datasets(dataset_id) ON DELETE CASCADE,
    version_label TEXT,
    event_type TEXT NOT NULL,
    run_id TEXT REFERENCES etl_runs(run_id) ON DELETE SET NULL,
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_events_dataset_time
    ON etl_dataset_events (dataset_id, created_at DESC);

CREATE TABLE IF NOT EXISTS etl_dictionary_repos (
    repo_id BIGSERIAL PRIMARY KEY,
    repo_key TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL DEFAULT 'github',
    owner TEXT NOT NULL,
    repo_name TEXT NOT NULL,
    default_branch TEXT NOT NULL DEFAULT 'main',
    local_path TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_dictionary_repos_active
    ON etl_dictionary_repos (is_active, provider, owner, repo_name);

CREATE TABLE IF NOT EXISTS etl_dataset_dictionary_entries (
    entry_id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES etl_datasets(dataset_id) ON DELETE CASCADE,
    repo_id BIGINT NOT NULL REFERENCES etl_dictionary_repos(repo_id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    file_sha TEXT,
    pr_url TEXT,
    pr_number BIGINT,
    review_status TEXT NOT NULL DEFAULT 'none',
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etl_dataset_dictionary_entries_unique UNIQUE (dataset_id, repo_id, file_path)
);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_dictionary_entries_dataset
    ON etl_dataset_dictionary_entries (dataset_id, review_status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_dataset_dictionary_entries_repo
    ON etl_dataset_dictionary_entries (repo_id, updated_at DESC);

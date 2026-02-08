ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS git_commit_sha TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS git_branch TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS git_tag TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS git_is_dirty BOOLEAN;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS cli_command TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS pipeline_checksum TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS global_config_checksum TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS execution_config_checksum TEXT;

ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS plugin_checksums_json JSONB;

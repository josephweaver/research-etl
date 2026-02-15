ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS plugin_name TEXT;

ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS plugin_version TEXT;

ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS failure_category TEXT;

ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS runtime_seconds DOUBLE PRECISION;

ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS memory_gb DOUBLE PRECISION;

ALTER TABLE IF EXISTS etl_run_step_attempts
    ADD COLUMN IF NOT EXISTS cpu_cores DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_etl_attempts_plugin_version
    ON etl_run_step_attempts (plugin_name, plugin_version, started_at DESC);


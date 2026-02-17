-- Project/user referential integrity hardening
ALTER TABLE IF EXISTS etl_user_projects
    ADD CONSTRAINT etl_user_projects_user_fk
    FOREIGN KEY (user_id) REFERENCES etl_users(user_id) ON DELETE CASCADE;

ALTER TABLE IF EXISTS etl_runs
    ADD CONSTRAINT etl_runs_project_fk
    FOREIGN KEY (project_id) REFERENCES etl_projects(project_id) ON DELETE SET NULL;

ALTER TABLE IF EXISTS etl_pipeline_validations
    ADD CONSTRAINT etl_pipeline_validations_project_fk
    FOREIGN KEY (project_id) REFERENCES etl_projects(project_id) ON DELETE SET NULL;

ALTER TABLE IF EXISTS etl_artifacts
    ADD CONSTRAINT etl_artifacts_project_fk
    FOREIGN KEY (project_id) REFERENCES etl_projects(project_id) ON DELETE SET NULL;

ALTER TABLE IF EXISTS etl_datasets
    ADD CONSTRAINT etl_datasets_owner_user_fk
    FOREIGN KEY (owner_user) REFERENCES etl_users(user_id) ON DELETE SET NULL;

-- Enforce uniqueness even when environment is NULL
CREATE UNIQUE INDEX IF NOT EXISTS idx_etl_dataset_locations_unique_env_norm
    ON etl_dataset_locations (dataset_version_id, COALESCE(environment, ''), location_type, uri);

-- Query-path indexes for operational timelines
CREATE INDEX IF NOT EXISTS idx_etl_run_events_run_time
    ON etl_run_events (run_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_etl_run_step_attempts_run_step_end
    ON etl_run_step_attempts (run_id, step_name, ended_at DESC);

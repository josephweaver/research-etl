ALTER TABLE etl_global_configs
    DROP COLUMN config_yaml;

ALTER TABLE etl_execution_configs
    DROP COLUMN config_yaml;

ALTER TABLE etl_pipelines
    DROP COLUMN pipeline_yaml;

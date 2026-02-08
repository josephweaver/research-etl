CREATE TABLE IF NOT EXISTS etl_global_configs (
    config_name TEXT PRIMARY KEY,
    config_yaml TEXT NOT NULL,
    checksum TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_execution_configs (
    env_name TEXT PRIMARY KEY,
    config_yaml TEXT NOT NULL,
    checksum TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_pipelines (
    pipeline_name TEXT PRIMARY KEY,
    pipeline_yaml TEXT NOT NULL,
    checksum TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_plugin_catalog (
    plugin_name TEXT NOT NULL,
    plugin_version TEXT NOT NULL,
    module_path TEXT NOT NULL,
    meta_json JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plugin_name, plugin_version)
);

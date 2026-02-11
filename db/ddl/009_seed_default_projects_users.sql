CREATE TABLE IF NOT EXISTS etl_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO etl_projects (project_id, display_name, is_active)
VALUES
    ('land_core', 'Land-Core', TRUE),
    ('gee_lee', 'Gee-Lee', TRUE)
ON CONFLICT (project_id) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO etl_users (user_id, display_name, is_active)
VALUES
    ('admin', 'Admin', TRUE),
    ('land-core', 'Land-Core Owner', TRUE),
    ('gee-lee', 'Gee-Lee Owner', TRUE)
ON CONFLICT (user_id) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO etl_user_projects (user_id, project_id, role)
VALUES
    ('land-core', 'land_core', 'admin'),
    ('gee-lee', 'gee_lee', 'admin'),
    ('admin', 'land_core', 'admin'),
    ('admin', 'gee_lee', 'admin')
ON CONFLICT (user_id, project_id) DO UPDATE
SET
    role = EXCLUDED.role,
    updated_at = NOW();

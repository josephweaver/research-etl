INSERT INTO etl_projects (project_id, display_name, is_active)
VALUES ('default', 'Default', TRUE)
ON CONFLICT (project_id) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO etl_user_projects (user_id, project_id, role)
VALUES ('admin', 'default', 'admin')
ON CONFLICT (user_id, project_id) DO UPDATE
SET
    role = EXCLUDED.role,
    updated_at = NOW();

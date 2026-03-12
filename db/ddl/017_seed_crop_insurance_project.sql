INSERT INTO etl_projects (project_id, display_name, is_active)
VALUES
    ('crop_insurance', 'Crop Insurance', TRUE)
ON CONFLICT (project_id) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

UPDATE etl_projects
SET
    is_active = FALSE,
    updated_at = NOW()
WHERE project_id = 'gee_lee';

INSERT INTO etl_users (user_id, display_name, is_active)
VALUES
    ('crop-insurance', 'Crop Insurance Owner', TRUE)
ON CONFLICT (user_id) DO UPDATE
SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

UPDATE etl_users
SET
    is_active = FALSE,
    updated_at = NOW()
WHERE user_id = 'gee-lee';

INSERT INTO etl_user_projects (user_id, project_id, role)
VALUES
    ('crop-insurance', 'crop_insurance', 'admin'),
    ('admin', 'crop_insurance', 'admin'),
    ('admin', 'default', 'admin')
ON CONFLICT (user_id, project_id) DO UPDATE
SET
    role = EXCLUDED.role,
    updated_at = NOW();

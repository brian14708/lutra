-- name: GetProject :one
SELECT
  project_id,
  name,
  metadata,
  create_time,
  update_time
FROM lutra.projects
WHERE project_id = $1
  AND status = 'active';

-- name: UpdateProject :one
UPDATE lutra.projects
SET name = $2,
  metadata = $3,
  update_time = now()
WHERE project_id = $1
  AND status = 'active'
RETURNING project_id, name, metadata, create_time, update_time;

-- name: ArchiveProject :exec
UPDATE lutra.projects
SET status = 'archived',
  update_time = now()
WHERE project_id = $1;

-- name: CreateProject :one
INSERT INTO lutra.projects (project_id, name, metadata)
VALUES ($1, $2, $3)
RETURNING project_id, name, metadata, create_time, update_time;

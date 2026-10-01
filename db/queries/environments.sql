-- name: InsertEnvironment :one
INSERT INTO lutra.task_environments (id, namespace_id, name, version, provider, spec, image_key)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (namespace_id, name, version) DO NOTHING
RETURNING id;

-- name: GetEnvironment :one
SELECT *
FROM lutra.task_environments
WHERE namespace_id = $1 AND name = $2 AND version = $3;

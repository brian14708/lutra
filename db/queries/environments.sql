-- name: InsertEnvironment :one
INSERT INTO lutra.task_environments (id, project, domain, name, version, provider, spec, image_key)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (project, domain, name, version) DO NOTHING
RETURNING id;

-- name: GetEnvironment :one
SELECT * FROM lutra.task_environments
WHERE project=$1 AND domain=$2 AND name=$3 AND version=$4;

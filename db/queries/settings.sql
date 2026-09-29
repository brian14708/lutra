-- name: CreateProject :one
INSERT INTO lutra.projects (id, slug, name) VALUES ($1, $2, $3)
RETURNING id, slug, name, created_at;

-- name: ListProjects :many
SELECT id, slug, name, created_at FROM lutra.projects ORDER BY slug;

-- name: CreateDomain :one
INSERT INTO lutra.domains (id, project_id, slug, name) VALUES ($1, $2, $3, $4)
RETURNING id, project_id, slug, name, created_at;

-- name: ListDomains :many
SELECT id, project_id, slug, name, created_at FROM lutra.domains WHERE project_id = $1 ORDER BY slug;

-- name: UpsertSetting :one
INSERT INTO lutra.settings (project_id, domain_id, path, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project_id, domain_id, path) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
RETURNING project_id, domain_id, path, value, updated_at;

-- name: DeleteSetting :execrows
DELETE FROM lutra.settings WHERE project_id = $1 AND domain_id IS NOT DISTINCT FROM $2 AND path = $3;

-- name: ListSettings :many
SELECT project_id, domain_id, path, value, updated_at FROM lutra.settings
WHERE project_id = $1 AND (domain_id IS NOT DISTINCT FROM $2) ORDER BY path;

-- name: ResolveSettings :many
SELECT project_id, domain_id, path, value, updated_at FROM lutra.settings
WHERE project_id = $1 AND (domain_id IS NULL OR domain_id = $2)
  AND (cardinality($3::text[]) = 0 OR path = ANY($3::text[]))
ORDER BY path, domain_id NULLS FIRST;

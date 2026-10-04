-- name: CreateNamespace :one
INSERT INTO lutra.namespaces (id, slug, name) VALUES ($1, $2, $3) RETURNING *;

-- name: ListNamespaces :many
SELECT * FROM lutra.namespaces ORDER BY slug;

-- name: GetNamespaceByID :one
SELECT * FROM lutra.namespaces WHERE id = $1;

-- name: UpsertSetting :one
INSERT INTO lutra.settings (namespace_id, path, value, sensitive) VALUES ($1, $2, $3, $4) ON CONFLICT (namespace_id, path) DO UPDATE SET value = excluded.value, sensitive = excluded.sensitive, updated_at = now() RETURNING *;

-- name: DeleteSetting :execrows
DELETE FROM lutra.settings WHERE namespace_id = $1 AND path = $2;

-- name: ListSettings :many
SELECT *
FROM lutra.settings
WHERE namespace_id = $1
  AND (coalesce(cardinality(sqlc.arg(paths)::text[]), 0) = 0 OR path IN (SELECT unnest(sqlc.arg(paths)::text[])))
ORDER BY path;

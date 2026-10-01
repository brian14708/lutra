-- name: ExpireImageBuilds :exec
UPDATE lutra.image_builds
SET status = 'failed', error = 'build lease expired'
WHERE image_key = $1 AND status = 'building' AND lease_until <= now();

-- name: SweepExpiredImageBuilds :exec
UPDATE lutra.image_builds
SET status = 'failed', error = 'build lease expired'
WHERE status = 'building' AND lease_until <= now();

-- name: GetImageBuild :one
SELECT * FROM lutra.image_builds WHERE id = $1;

-- name: LatestImageBuild :one
SELECT *
FROM lutra.image_builds
WHERE image_key = $1
ORDER BY created_at DESC, id DESC
LIMIT 1;

-- name: ReadyImageBuild :one
SELECT *
FROM lutra.image_builds
WHERE image_key = $1 AND status = 'ready'
ORDER BY created_at DESC, id DESC
LIMIT 1;

-- name: DeleteUnneededImageBuild :exec
DELETE FROM lutra.image_builds WHERE id = $1 AND status = 'building';

-- name: InsertImageBuild :one
INSERT INTO lutra.image_builds (id, image_key, status, claim_token, lease_until, created_at)
VALUES ($1, $2, 'building', $3, now() + '30 seconds'::interval, clock_timestamp())
ON CONFLICT (image_key) WHERE status = 'building' DO NOTHING
RETURNING *;

-- name: FinishImageBuild :execrows
UPDATE lutra.image_builds
SET status = sqlc.arg(status)::lutra.image_build_status,
  artifact_uri = sqlc.arg(artifact_uri)::text,
  error = sqlc.arg(error)::text
WHERE id = sqlc.arg(id)::uuid AND claim_token = sqlc.arg(claim_token)::uuid AND status = 'building' AND lease_until > now();

-- name: RenewImageBuild :execrows
UPDATE lutra.image_builds
SET lease_until = now() + '30 seconds'::interval
WHERE id = $1 AND claim_token = $2 AND status = 'building' AND lease_until > now();

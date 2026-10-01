-- name: ExpireCacheClaim :exec
UPDATE lutra.cache_entries
SET status = 'failed', result_cbor = '\xf6'::bytea
WHERE key = $1 AND status = 'building' AND lease_until <= clock_timestamp();

-- name: DeleteExpiredTaskClaim :exec
DELETE FROM lutra.cache_entries
WHERE key = $1 AND status = 'building' AND lease_until <= clock_timestamp();

-- name: ActiveCacheEntry :one
SELECT * FROM lutra.cache_entries
WHERE key = $1 AND status IN ('building', 'ready');

-- name: CacheGeneration :one
SELECT * FROM lutra.cache_entries WHERE id = $1;

-- name: ClaimCacheEntry :one
INSERT INTO lutra.cache_entries (id, key, status, claim_token, lease_until)
VALUES ($1, $2, 'building', $3, clock_timestamp() + interval '30 seconds')
ON CONFLICT (key) WHERE status IN ('building', 'ready') DO NOTHING
RETURNING *;

-- name: RenewCacheClaim :execrows
UPDATE lutra.cache_entries SET lease_until = clock_timestamp() + interval '30 seconds'
WHERE id = $1 AND claim_token = $2 AND status = 'building' AND lease_until > clock_timestamp();

-- name: CompleteCacheEntry :execrows
UPDATE lutra.cache_entries
SET status = 'ready', result_cbor = sqlc.arg(result_cbor)::bytea
WHERE id = sqlc.arg(id)::uuid AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'building' AND lease_until > clock_timestamp();

-- name: FailImageCache :execrows
UPDATE lutra.cache_entries SET status = 'failed', result_cbor = $3
WHERE id = $1 AND claim_token = $2 AND status = 'building' AND lease_until > clock_timestamp();

-- name: ReleaseCacheClaim :execrows
DELETE FROM lutra.cache_entries
WHERE id = $1 AND claim_token = $2 AND status = 'building' AND lease_until > clock_timestamp();

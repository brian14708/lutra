-- name: ClaimRun :one
UPDATE lutra.runs
SET status = 'running', attempts = attempts + 1,
    claim_token = sqlc.arg(claim_token)::uuid,
    lease_until = now() + sqlc.arg(lease_seconds)::integer * interval '1 second',
    updated_at = now()
WHERE id = (
    SELECT id FROM lutra.runs
    WHERE (status = 'queued' AND next_attempt_at <= now())
       OR (status IN ('running', 'waiting') AND lease_until <= now())
    ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
)
RETURNING id, attempts;

-- name: LoadClaimedRun :one
SELECT r.project, r.domain, r.name, r.version, t.module, t.qualname,
       t.source_sha256, t.image, b.object_key, r.input_cbor
FROM lutra.runs r
JOIN lutra.task_specs t USING (project, domain, name, version)
JOIN lutra.blobs b ON b.sha256 = t.source_sha256
WHERE r.id = sqlc.arg(run_id)::uuid
  AND r.claim_token = sqlc.arg(claim_token)::uuid
  AND r.status = 'running';

-- name: RenewRunLease :execrows
UPDATE lutra.runs
SET lease_until = now() + sqlc.arg(lease_seconds)::integer * interval '1 second'
WHERE id = sqlc.arg(run_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status IN ('running', 'waiting');

-- name: FinishRun :execrows
UPDATE lutra.runs
SET status = sqlc.arg(status)::text,
    output_cbor = sqlc.narg(output_cbor)::bytea,
    error = sqlc.arg(error)::text,
    next_attempt_at = COALESCE(sqlc.narg(next_attempt_at)::timestamptz, next_attempt_at),
    claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE id = sqlc.arg(run_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND attempts = sqlc.arg(attempt)::integer
  AND status IN ('running', 'waiting');

-- name: MarkRunWaiting :execrows
UPDATE lutra.runs SET status = 'waiting', updated_at = now()
WHERE id = sqlc.arg(run_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'running';

-- name: RestoreRunRunning :execrows
UPDATE lutra.runs SET status = 'running', updated_at = now()
WHERE id = sqlc.arg(run_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'waiting';

-- name: GetRunStatus :one
SELECT status FROM lutra.runs WHERE id = sqlc.arg(run_id)::uuid;

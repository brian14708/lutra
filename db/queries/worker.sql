-- name: ClaimTaskAction :one
UPDATE lutra.task_actions
SET status = 'running', attempts = attempts + 1,
    claim_token = sqlc.arg(claim_token)::uuid,
    lease_until = now() + sqlc.arg(lease_seconds)::integer * interval '1 second',
    updated_at = now()
WHERE id = (
    SELECT id FROM lutra.task_actions
    WHERE (status = 'queued' AND next_attempt_at <= now())
       OR (status IN ('running', 'waiting') AND lease_until <= now())
    ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
)
RETURNING id, run_id, attempts;

-- name: LoadClaimedTaskAction :one
SELECT a.project, a.domain, a.name, a.version, t.module, t.qualname,
       t.source_sha256, t.image, b.object_key, a.input_cbor
FROM lutra.task_actions a
JOIN lutra.task_specs t USING (project, domain, name, version)
JOIN lutra.blobs b ON b.sha256 = t.source_sha256
WHERE a.id = sqlc.arg(action_id)::uuid
  AND a.claim_token = sqlc.arg(claim_token)::uuid
  AND a.status = 'running';

-- name: RenewTaskActionLease :execrows
UPDATE lutra.task_actions
SET lease_until = now() + sqlc.arg(lease_seconds)::integer * interval '1 second'
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status IN ('running', 'waiting');

-- name: FinishTaskAction :execrows
UPDATE lutra.task_actions
SET status = sqlc.arg(status)::lutra.task_action_status,
    output_cbor = sqlc.narg(output_cbor)::bytea,
    error = sqlc.arg(error)::text,
    next_attempt_at = COALESCE(sqlc.narg(next_attempt_at)::timestamptz, next_attempt_at),
    claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND attempts = sqlc.arg(attempt)::integer
  AND status IN ('running', 'waiting');

-- name: MarkTaskActionWaiting :execrows
UPDATE lutra.task_actions SET status = 'waiting', updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'running';

-- name: RestoreTaskActionRunning :execrows
UPDATE lutra.task_actions SET status = 'running', updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'waiting';

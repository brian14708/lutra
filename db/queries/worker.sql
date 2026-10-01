-- name: ClaimTaskAction :one
WITH candidate AS MATERIALIZED (
    SELECT id, status FROM lutra.task_actions
    WHERE (status = 'queued' AND next_attempt_at <= now())
       OR (status IN ('running', 'waiting') AND lease_until <= now())
    ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE lutra.task_actions a
SET status = 'running', attempts = attempts + 1,
    claim_token = sqlc.arg(claim_token)::uuid,
    lease_until = now() + sqlc.arg(lease_seconds)::integer * interval '1 second',
    updated_at = now()
FROM candidate c
WHERE a.id = c.id
RETURNING a.id, a.run_id, a.caller_action_id, a.attempts, c.status AS previous_status;

-- name: LoadClaimedTaskAction :one
SELECT
  e.namespace_id,
  a.entrypoint_id,
  e.name AS environment_name,
  e.version,
  e.id AS environment_id,
  e.provider,
  e.spec,
  e.image_key,
  a.input_cbor
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
WHERE a.id = sqlc.arg(action_id)::uuid
  AND a.claim_token = sqlc.arg(claim_token)::uuid
  AND a.status = 'running';

-- name: RenewTaskActionLease :execrows
UPDATE lutra.task_actions
SET lease_until = now() + sqlc.arg(lease_seconds)::integer * '1 second'::interval
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status IN ('running', 'waiting');

-- name: SetTaskActionJob :execrows
UPDATE lutra.task_actions
SET job_id = sqlc.arg(job_id)::text, updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status IN ('running', 'waiting');

-- name: FinishTaskAction :execrows
UPDATE lutra.task_actions
SET status = sqlc.arg(status)::lutra.task_action_status,
  output_cbor = sqlc.narg(output_cbor)::bytea,
  error = sqlc.arg(error)::text,
  next_attempt_at = coalesce(sqlc.narg(next_attempt_at)::timestamptz, next_attempt_at),
  claim_token = NULL,
  lease_until = NULL,
  updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND attempts = sqlc.arg(attempt)::integer
  AND status IN ('running', 'waiting');

-- name: MarkTaskActionWaiting :execrows
UPDATE lutra.task_actions
SET status = 'waiting', updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'running';

-- name: RestoreTaskActionRunning :execrows
UPDATE lutra.task_actions
SET status = 'running', updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND status = 'waiting';

-- name: InsertRun :one
INSERT INTO lutra.runs (id, namespace_id, root_idempotency_key)
VALUES ($1, $2, $3)
ON CONFLICT (namespace_id, root_idempotency_key) WHERE root_idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetRunByIdempotencyKey :one
SELECT id, root_action_id
FROM lutra.runs
WHERE namespace_id = $1 AND root_idempotency_key = $2;

-- name: InsertRootAction :one
INSERT INTO lutra.task_actions (id, run_id, environment_id, entrypoint_id, action_spec, status)
VALUES ($1, $2, $3, $4, $5, 'queued')
RETURNING id;

-- name: UpdateRunRootAction :exec
UPDATE lutra.runs SET root_action_id = $1 WHERE id = $2;

-- name: InsertTaskAction :one
INSERT INTO lutra.task_actions (id, run_id, caller_action_id, environment_id, entrypoint_id, action_spec, status, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7)
ON CONFLICT (run_id, caller_action_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetTaskActionByIdempotency :one
SELECT id, run_id, caller_action_id, environment_id, entrypoint_id, action_spec
FROM lutra.task_actions
WHERE run_id = $1 AND caller_action_id = $2 AND idempotency_key = $3;

-- name: ReadRun :one
SELECT
  r.root_action_id,
  e.namespace_id,
  r.created_at AS run_created_at,
  a.id,
  a.status,
  a.result_cbor,
  a.updated_at,
  e.name AS environment_name,
  e.version,
  a.entrypoint_id,
  a.environment_id
FROM lutra.runs AS r
JOIN lutra.task_actions AS a ON a.run_id = r.id AND a.id = r.root_action_id
JOIN lutra.task_environments AS e ON e.id = a.environment_id
WHERE r.id = $1;

-- name: ReadTaskAction :one
SELECT
  a.id,
  a.run_id,
  a.caller_action_id,
  e.namespace_id,
  e.name AS environment_name,
  e.version,
  a.entrypoint_id,
  a.environment_id,
  a.action_spec,
  a.result_cbor,
  a.status,
  a.attempts,
  a.created_at,
  a.updated_at
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
WHERE a.id = $1;

-- name: LoadRunTasks :many
SELECT
  a.id,
  a.caller_action_id,
  a.action_spec,
  a.result_cbor,
  a.status,
  a.attempts,
  a.next_attempt_at,
  a.failures,
  a.environment_id,
  a.entrypoint_id,
  e.namespace_id,
  e.name AS environment_name,
  e.version,
  e.provider,
  e.spec AS environment_spec,
  e.image_key
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
WHERE a.run_id = sqlc.arg(run_id)::uuid
ORDER BY a.created_at, a.id;

-- name: ListTaskActions :many
SELECT a.id, a.run_id, a.caller_action_id, e.namespace_id,
       e.name AS environment_name, e.version, a.entrypoint_id, a.environment_id,
       a.action_spec, a.result_cbor, a.status, a.attempts,
       a.created_at, a.updated_at
FROM lutra.task_actions a
JOIN lutra.task_environments e ON e.id = a.environment_id
WHERE a.run_id = sqlc.arg(run_id)::uuid
  AND (a.created_at, a.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid)
ORDER BY a.created_at, a.id
LIMIT sqlc.arg(page_size)::integer;

-- name: GetTaskActionCaller :one
SELECT run_id, caller_action_id FROM lutra.task_actions WHERE id = $1;

-- name: GetActiveCaller :one
SELECT a.run_id, a.id, a.status, r.claim_token, a.attempts, a.environment_id, e.spec AS environment_spec
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
JOIN lutra.runs AS r ON r.id = a.run_id
WHERE a.id = $1 AND a.status IN ('running', 'waiting') AND r.claim_token IS NOT NULL AND r.lease_until > clock_timestamp();

-- name: LockRun :one
SELECT id, root_action_id, namespace_id FROM lutra.runs WHERE id = $1 FOR UPDATE;

-- name: ClaimRun :one
WITH candidate AS MATERIALIZED (
    SELECT id
    FROM lutra.runs
    WHERE (claim_token IS NULL OR lease_until <= clock_timestamp())
      AND root_action_id IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM lutra.task_actions a
        WHERE a.run_id = lutra.runs.id
          AND a.status NOT IN ('succeeded', 'failed', 'canceled')
      )
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE lutra.runs r
SET claim_token = sqlc.arg(claim_token)::uuid,
    lease_until = clock_timestamp() + sqlc.arg(lease_seconds)::integer * interval '1 second'
FROM candidate c
WHERE r.id = c.id
RETURNING r.id, r.root_action_id, r.namespace_id;

-- name: RenewRunLease :execrows
UPDATE lutra.runs
SET lease_until = clock_timestamp() + sqlc.arg(lease_seconds)::integer * '1 second'::interval
WHERE id = sqlc.arg(run_id)::uuid
  AND claim_token = sqlc.arg(claim_token)::uuid
  AND lease_until > clock_timestamp();

-- name: ReleaseRunClaim :execrows
UPDATE lutra.runs
SET claim_token = NULL, lease_until = NULL
WHERE id = sqlc.arg(run_id)::uuid AND claim_token = sqlc.arg(claim_token)::uuid;

-- name: ResetRunActions :execrows
UPDATE lutra.task_actions
SET status = 'queued',
  updated_at = now()
WHERE run_id = sqlc.arg(run_id)::uuid
  AND status IN ('building', 'running', 'waiting')
  AND EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.id = sqlc.arg(run_id)::uuid
      AND r.claim_token = sqlc.arg(claim_token)::uuid
      AND r.lease_until > clock_timestamp()
  );

-- name: ClearRunClaim :execrows
UPDATE lutra.runs
SET claim_token = NULL, lease_until = NULL
WHERE id = $1;

-- name: TransitionRunTaskAction :execrows
WITH RECURSIVE ancestors AS (
  SELECT caller_action_id FROM lutra.task_actions WHERE id = sqlc.arg(action_id)::uuid
  UNION ALL
  SELECT a.caller_action_id FROM lutra.task_actions a
  JOIN ancestors parent ON a.id = parent.caller_action_id
)
UPDATE lutra.task_actions AS a
SET status = sqlc.arg(status)::lutra.task_action_status,
  attempts = sqlc.arg(attempt)::integer,
  failures = sqlc.arg(failures)::integer,
  result_cbor = sqlc.narg(result_cbor)::bytea,
  next_attempt_at = sqlc.narg(next_attempt_at)::timestamptz,
  updated_at = now()
FROM lutra.runs AS r
WHERE a.id = sqlc.arg(action_id)::uuid
  AND a.run_id = r.id
  AND r.id = sqlc.arg(run_id)::uuid
  AND r.claim_token = sqlc.arg(claim_token)::uuid
  AND r.lease_until > clock_timestamp()
  AND a.attempts = sqlc.arg(expected_attempt)::integer
  AND (sqlc.arg(attempt)::integer <= a.attempts OR NOT EXISTS (
    SELECT 1 FROM ancestors JOIN lutra.task_actions parent ON parent.id = ancestors.caller_action_id
    WHERE parent.status IN ('succeeded', 'failed', 'canceled')
  ))
  AND a.status NOT IN ('succeeded', 'failed', 'canceled');

-- name: GetRootStatus :one
SELECT status FROM lutra.task_actions WHERE id = $1;

-- name: CancelRunIfActive :many
UPDATE lutra.task_actions SET status = 'canceled', result_cbor = sqlc.arg(result_cbor)::bytea, updated_at = now()
WHERE lutra.task_actions.run_id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled')
  AND EXISTS (SELECT 1 FROM lutra.runs r JOIN lutra.task_actions root ON root.id = r.root_action_id
              WHERE r.id = $1 AND root.status NOT IN ('succeeded', 'failed', 'canceled'))
RETURNING id;

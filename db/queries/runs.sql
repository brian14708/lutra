-- name: InsertRun :one
INSERT INTO lutra.runs (id, namespace_id, root_idempotency_key)
VALUES ($1, $2, $3)
ON CONFLICT (namespace_id, root_idempotency_key) WHERE root_idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetRunByIdempotencyKey :one
SELECT id, root_action_id
FROM lutra.runs
WHERE namespace_id = $1 AND root_idempotency_key = $2;

-- name: EnqueueActionDispatch :exec
INSERT INTO lutra.dispatch_outbox (action_id) VALUES ($1)
ON CONFLICT (action_id) DO NOTHING;

-- name: PendingActionDispatches :many
SELECT action_id FROM lutra.dispatch_outbox
WHERE next_attempt_at <= now()
ORDER BY next_attempt_at, action_id
LIMIT 100;

-- name: CompleteActionDispatch :exec
DELETE FROM lutra.dispatch_outbox WHERE action_id = $1;

-- name: DeferActionDispatch :exec
UPDATE lutra.dispatch_outbox SET next_attempt_at = now() + interval '5 seconds'
WHERE action_id = $1;

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
  e.spec AS environment_spec,
  a.action_spec,
  a.result_cbor,
  a.status,
  a.attempts,
  a.created_at,
  a.updated_at
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
WHERE a.id = $1;

-- name: LoadAction :one
SELECT
  a.id,
  a.run_id,
  a.caller_action_id,
  a.action_spec,
  a.result_cbor,
  a.status,
  a.attempts,
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
WHERE a.id = $1;

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
SELECT a.run_id, a.caller_action_id, caller.attempts AS caller_attempts
FROM lutra.task_actions AS a
JOIN lutra.task_actions AS caller ON caller.id = a.caller_action_id
WHERE a.id = $1;

-- name: GetActiveCaller :one
SELECT a.run_id, a.id, a.status, a.attempts, a.environment_id, a.entrypoint_id, e.spec AS environment_spec
FROM lutra.task_actions AS a
JOIN lutra.task_environments AS e ON e.id = a.environment_id
JOIN lutra.runs AS r ON r.id = a.run_id
WHERE a.id = $1 AND a.status IN ('running', 'waiting');

-- name: LockRun :one
SELECT id, root_action_id, namespace_id FROM lutra.runs WHERE id = $1 FOR UPDATE;

-- name: StartTaskAttempt :one
UPDATE lutra.task_actions SET status = 'running', attempts = attempts + 1, updated_at = now()
WHERE id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled')
RETURNING attempts;

-- name: ProjectTaskAction :execrows
UPDATE lutra.task_actions
SET status = sqlc.arg(status)::lutra.task_action_status,
    result_cbor = sqlc.narg(result_cbor)::bytea, updated_at = now()
WHERE id = sqlc.arg(action_id)::uuid
  AND status <> sqlc.arg(status)::lutra.task_action_status
  AND status NOT IN ('succeeded', 'failed', 'canceled');

-- name: CanceledActions :many
SELECT id FROM lutra.task_actions
WHERE run_id = $1 AND status = 'canceled'
ORDER BY id;

-- name: CancelDescendants :many
WITH RECURSIVE descendants AS (
  SELECT id FROM lutra.task_actions WHERE caller_action_id = sqlc.arg(parent_id)::uuid
  UNION ALL
  SELECT a.id FROM lutra.task_actions a JOIN descendants d ON a.caller_action_id = d.id
)
UPDATE lutra.task_actions SET status = 'canceled', result_cbor = sqlc.arg(result_cbor)::bytea, updated_at = now()
WHERE id IN (SELECT id FROM descendants) AND status NOT IN ('succeeded', 'failed', 'canceled')
RETURNING id;

-- name: GetRootStatus :one
SELECT status FROM lutra.task_actions WHERE id = $1;

-- name: CancelRunIfActive :many
UPDATE lutra.task_actions SET status = 'canceled', result_cbor = sqlc.arg(result_cbor)::bytea, updated_at = now()
WHERE lutra.task_actions.run_id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled')
  AND EXISTS (SELECT 1 FROM lutra.runs r JOIN lutra.task_actions root ON root.id = r.root_action_id
              WHERE r.id = $1 AND root.status NOT IN ('succeeded', 'failed', 'canceled'))
RETURNING id;

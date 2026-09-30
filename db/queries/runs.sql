-- name: InsertTaskSpec :exec
INSERT INTO lutra.task_specs (project, domain, name, version, source_sha256, image, module, qualname)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (project, domain, name, version) DO NOTHING;

-- name: GetTaskSpec :one
SELECT module, qualname, source_sha256, image FROM lutra.task_specs
WHERE project = $1 AND domain = $2 AND name = $3 AND version = $4;

-- name: InsertRun :one
INSERT INTO lutra.runs (id, project, domain, root_idempotency_key)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project, domain, root_idempotency_key) WHERE root_idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetRunByIdempotencyKey :one
SELECT id, root_action_id FROM lutra.runs
WHERE project = $1 AND domain = $2 AND root_idempotency_key = $3;

-- name: InsertRootAction :one
INSERT INTO lutra.task_actions (id, run_id, project, domain, name, version, input_cbor, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued')
RETURNING id;

-- name: UpdateRunRootAction :exec
UPDATE lutra.runs SET root_action_id = $1 WHERE id = $2;

-- name: InsertTaskAction :one
INSERT INTO lutra.task_actions (id, run_id, caller_action_id, project, domain, name, version, input_cbor, status, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', $9)
ON CONFLICT (run_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetTaskActionByIdempotency :one
SELECT id, run_id, caller_action_id, project, domain, name, version, input_cbor
FROM lutra.task_actions
WHERE run_id = $1 AND idempotency_key = $2;

-- name: InsertTaskActionEdge :exec
INSERT INTO lutra.task_action_edges (run_id, source_action_id, dependent_action_id)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;

-- name: ReadRun :one
SELECT r.root_action_id, r.project, r.domain, r.created_at AS run_created_at,
       a.id, a.status, a.output_cbor, a.error, a.updated_at,
       t.name, t.version, t.module, t.qualname, t.source_sha256, t.image
FROM lutra.runs r
JOIN lutra.task_actions a ON a.run_id = r.id AND a.id = r.root_action_id
JOIN lutra.task_specs t ON t.project = a.project AND t.domain = a.domain
  AND t.name = a.name AND t.version = a.version
WHERE r.id = $1;

-- name: ReadTaskAction :one
SELECT a.id, a.run_id, a.caller_action_id, a.project, a.domain,
       a.name, a.version, t.module, t.qualname, t.source_sha256, t.image,
       a.input_cbor, a.output_cbor, a.status, a.error, a.attempts,
       a.created_at, a.updated_at
FROM lutra.task_actions a
JOIN lutra.task_specs t USING (project, domain, name, version)
WHERE a.id = $1;

-- name: ListTaskActions :many
SELECT a.id, a.run_id, a.caller_action_id, a.project, a.domain,
       a.name, a.version, t.module, t.qualname, t.source_sha256, t.image,
       a.input_cbor, a.output_cbor, a.status, a.error, a.attempts,
       a.created_at, a.updated_at
FROM lutra.task_actions a
JOIN lutra.task_specs t USING (project, domain, name, version)
WHERE a.run_id = sqlc.arg(run_id)::uuid
  AND (a.created_at, a.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid)
ORDER BY a.created_at, a.id
LIMIT sqlc.arg(page_size)::integer;

-- name: ListActionUpstreams :many
SELECT dependent_action_id, source_action_id FROM lutra.task_action_edges
WHERE run_id = $1 AND dependent_action_id = ANY(sqlc.arg(action_ids)::uuid[])
ORDER BY dependent_action_id, source_action_id;

-- name: GetTaskActionCaller :one
SELECT run_id, caller_action_id FROM lutra.task_actions WHERE id = $1;

-- name: GetActiveCaller :one
SELECT a.run_id, a.id, a.status, a.claim_token, t.source_sha256
FROM lutra.task_actions a
JOIN lutra.task_specs t USING (project, domain, name, version)
WHERE a.id = $1 AND a.status IN ('running', 'waiting') AND a.claim_token IS NOT NULL;

-- name: LockRun :one
SELECT id, root_action_id, project, domain FROM lutra.runs WHERE id = $1 FOR UPDATE;

-- name: GetRootStatus :one
SELECT status FROM lutra.task_actions WHERE id = $1;

-- name: GetActionStatus :one
SELECT status FROM lutra.task_actions WHERE id = $1;

-- name: GetRunRootAction :one
SELECT root_action_id FROM lutra.runs WHERE id = $1;

-- name: CancelRunActions :execrows
UPDATE lutra.task_actions SET status = 'canceled', error = 'run canceled',
       claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE run_id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled');

-- name: CancelRunIfActive :execrows
UPDATE lutra.task_actions SET status = 'canceled', error = 'run canceled',
       claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE lutra.task_actions.run_id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled')
  AND EXISTS (SELECT 1 FROM lutra.runs r JOIN lutra.task_actions root ON root.id = r.root_action_id
              WHERE r.id = $1 AND root.status NOT IN ('succeeded', 'failed', 'canceled'));

-- name: CloseRunDescendants :execrows
UPDATE lutra.task_actions SET status = 'canceled', error = 'root action completed',
       claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE run_id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled') AND id <> $2;

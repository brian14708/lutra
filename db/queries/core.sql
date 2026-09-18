-- name: CreateArtifact :one
INSERT INTO lutra.artifacts (artifact_id, project_id, mime_type)
VALUES ($1, $2, $3)
RETURNING artifact_id, project_id, mime_type, size_bytes, digest, create_time, seal_time;

-- name: GetArtifact :one
SELECT
  artifact_id,
  project_id,
  mime_type,
  size_bytes,
  digest,
  create_time,
  seal_time
FROM lutra.artifacts
WHERE artifact_id = $1
  AND project_id = $2;

-- name: LockArtifact :one
SELECT
  artifact_id,
  project_id,
  mime_type,
  size_bytes,
  digest,
  create_time,
  seal_time
FROM lutra.artifacts
WHERE artifact_id = $1
  AND project_id = $2
FOR UPDATE;

-- name: UpsertArtifactChunk :one
WITH writable AS (
  SELECT a.artifact_id
  FROM lutra.artifacts AS a
  WHERE a.artifact_id = sqlc.arg(artifact_id)
    AND a.seal_time IS NULL
  FOR UPDATE
)
INSERT INTO lutra.artifact_chunks (
  artifact_id,
  chunk_index,
  size_bytes,
  digest,
  upload_id
)
SELECT
  writable.artifact_id,
  sqlc.arg(chunk_index)::integer,
  sqlc.arg(size_bytes)::bigint,
  sqlc.arg(digest)::text,
  sqlc.arg(upload_id)::uuid
FROM writable
ON CONFLICT (artifact_id, chunk_index) DO UPDATE SET
  size_bytes = excluded.size_bytes,
  digest = excluded.digest,
  upload_id = excluded.upload_id,
  complete_time = NULL
RETURNING artifact_id, chunk_index, size_bytes, digest, upload_id, complete_time;

-- name: GetArtifactChunk :one
SELECT
  artifact_id,
  chunk_index,
  size_bytes,
  digest,
  upload_id,
  complete_time
FROM lutra.artifact_chunks
WHERE artifact_id = $1
  AND chunk_index = $2;

-- name: CompleteArtifactChunk :one
WITH writable AS (
  SELECT a.artifact_id
  FROM lutra.artifacts AS a
  WHERE a.artifact_id = sqlc.arg(artifact_id)
    AND a.seal_time IS NULL
  FOR UPDATE
)
UPDATE lutra.artifact_chunks AS c
SET complete_time = coalesce(c.complete_time, now())
FROM writable
WHERE c.artifact_id = writable.artifact_id
  AND c.chunk_index = sqlc.arg(chunk_index)
  AND c.upload_id = sqlc.arg(upload_id)
RETURNING c.artifact_id, c.chunk_index, c.size_bytes, c.digest, c.upload_id, c.complete_time;

-- name: ListArtifactChunks :many
SELECT
  artifact_id,
  chunk_index,
  size_bytes,
  digest,
  upload_id,
  complete_time
FROM lutra.artifact_chunks
WHERE artifact_id = $1
ORDER BY chunk_index;

-- name: SealArtifact :one
UPDATE lutra.artifacts
SET size_bytes = $3,
  digest = $4,
  seal_time = now()
WHERE artifact_id = $1
  AND project_id = $2
  AND seal_time IS NULL
RETURNING artifact_id, project_id, mime_type, size_bytes, digest, create_time, seal_time;

-- name: CreateTask :one
INSERT INTO lutra.tasks (
  task_id,
  project_id,
  name,
  version,
  entrypoint_argv,
  code_bundle_artifact_id,
  input_slots,
  output_slots
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING task_id, project_id, name, version, entrypoint_argv, code_bundle_artifact_id, input_slots, output_slots, create_time;

-- name: GetTask :one
SELECT
  task_id,
  project_id,
  name,
  version,
  entrypoint_argv,
  code_bundle_artifact_id,
  input_slots,
  output_slots,
  create_time
FROM lutra.tasks
WHERE task_id = $1
  AND project_id = $2;

-- name: GetTaskByIdentity :one
SELECT
  task_id,
  project_id,
  name,
  version,
  entrypoint_argv,
  code_bundle_artifact_id,
  input_slots,
  output_slots,
  create_time
FROM lutra.tasks
WHERE project_id = $1
  AND name = $2
  AND version = $3;

-- name: ListTasks :many
SELECT
  task_id,
  project_id,
  name,
  version,
  entrypoint_argv,
  code_bundle_artifact_id,
  input_slots,
  output_slots,
  create_time
FROM lutra.tasks
WHERE project_id = $1
ORDER BY create_time;

-- name: CreateRun :one
INSERT INTO lutra.runs (run_id, project_id, idempotency_key)
VALUES (
  sqlc.arg(run_id),
  sqlc.arg(project_id),
  NULLIF(sqlc.arg(idempotency_key)::text, '')
)
ON CONFLICT (project_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO UPDATE SET
  update_time = lutra.runs.update_time
RETURNING run_id, project_id, idempotency_key, state, create_time, update_time;

-- name: GetRun :one
SELECT
  run_id,
  project_id,
  idempotency_key,
  state,
  create_time,
  update_time
FROM lutra.runs
WHERE run_id = $1
  AND project_id = $2;

-- name: ListRuns :many
SELECT
  sqlc.embed(r),
  a.task_id AS root_task_id
FROM lutra.runs AS r
JOIN lutra.actions AS a ON a.run_id = r.run_id
  AND a.parent_action_id IS NULL
WHERE r.project_id = $1
ORDER BY r.create_time DESC;

-- name: UpdateRunState :one
UPDATE lutra.runs
SET
  state = $3,
  update_time = now()
WHERE
  run_id = $1
  AND project_id = $2
  AND state NOT IN ('succeeded', 'failed', 'cancelled')
RETURNING
  run_id,
  project_id,
  idempotency_key,
  state,
  create_time,
  update_time;

-- name: MarkRunRunning :one
UPDATE lutra.runs
SET state = 'running',
  update_time = now()
WHERE run_id = $1
  AND project_id = $2
  AND state IN ('queued', 'running')
RETURNING run_id, project_id, idempotency_key, state, create_time, update_time;

-- name: CreateAction :one
INSERT INTO lutra.actions (
  action_id,
  project_id,
  run_id,
  parent_action_id,
  operation_id,
  task_id,
  inputs
)
VALUES (
  sqlc.arg(action_id),
  sqlc.arg(project_id),
  sqlc.arg(run_id),
  sqlc.arg(parent_action_id),
  sqlc.arg(operation_id),
  sqlc.arg(task_id),
  sqlc.arg(inputs)
)
ON CONFLICT (parent_action_id, operation_id) WHERE (parent_action_id IS NOT NULL AND operation_id IS NOT NULL) DO NOTHING
RETURNING action_id, project_id, run_id, parent_action_id, operation_id, task_id, state, inputs, outputs, failure_message, attempt_count, create_time, update_time, start_time, end_time;

-- name: GetAction :one
SELECT
  action_id,
  project_id,
  run_id,
  parent_action_id,
  operation_id,
  task_id,
  state,
  inputs,
  outputs,
  failure_message,
  attempt_count,
  create_time,
  update_time,
  start_time,
  end_time
FROM lutra.actions
WHERE action_id = $1
  AND project_id = $2
  AND run_id = $3;

-- name: ListActions :many
SELECT
  action_id,
  project_id,
  run_id,
  parent_action_id,
  operation_id,
  task_id,
  state,
  inputs,
  outputs,
  failure_message,
  attempt_count,
  create_time,
  update_time,
  start_time,
  end_time
FROM lutra.actions
WHERE project_id = $1
  AND run_id = $2
ORDER BY create_time;

-- name: ClaimAction :one
UPDATE lutra.actions
SET state = 'running',
  attempt_count = attempt_count + 1,
  start_time = coalesce(start_time, now()),
  update_time = now()
WHERE lutra.actions.action_id = $1
  AND lutra.actions.project_id = $2
  AND lutra.actions.state = 'ready'
  AND EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = lutra.actions.run_id
      AND r.project_id = lutra.actions.project_id
      AND r.state IN ('queued', 'running')
  )
RETURNING action_id, project_id, run_id, parent_action_id, operation_id, task_id, state, inputs, outputs, failure_message, attempt_count, create_time, update_time, start_time, end_time;

-- name: GetActionByOperation :one
SELECT
  action_id,
  project_id,
  run_id,
  parent_action_id,
  operation_id,
  task_id,
  state,
  inputs,
  outputs,
  failure_message,
  attempt_count,
  create_time,
  update_time,
  start_time,
  end_time
FROM lutra.actions
WHERE project_id = $1
  AND run_id = $2
  AND parent_action_id = $3
  AND operation_id = $4;

-- name: CreateActionAttempt :one
INSERT INTO lutra.action_attempts (
  action_id,
  attempt,
  lease_owner,
  lease_expires_at,
  fencing_token
)
VALUES ($1, $2, $3, $4, $5)
RETURNING action_id, attempt, state, lease_owner, lease_expires_at, fencing_token, start_time, end_time, failure_message;

-- name: GetActionAttempt :one
SELECT
  action_id,
  attempt,
  state,
  lease_owner,
  lease_expires_at,
  fencing_token,
  start_time,
  end_time,
  failure_message
FROM lutra.action_attempts
WHERE action_id = $1
  AND attempt = $2;

-- name: ListActionAttempts :many
SELECT
  action_id,
  attempt,
  state,
  lease_owner,
  lease_expires_at,
  fencing_token,
  start_time,
  end_time,
  failure_message
FROM lutra.action_attempts
WHERE action_id = $1
ORDER BY attempt;

-- name: ListActionAttemptsForRun :many
SELECT
  aa.action_id,
  aa.attempt,
  aa.state,
  aa.lease_owner,
  aa.lease_expires_at,
  aa.fencing_token,
  aa.start_time,
  aa.end_time,
  aa.failure_message
FROM lutra.action_attempts AS aa
JOIN lutra.actions AS a ON a.action_id = aa.action_id
WHERE a.run_id = $1
ORDER BY aa.action_id, aa.attempt;

-- name: FinishActionAttempt :execrows
UPDATE lutra.action_attempts
SET state = $4,
  end_time = now(),
  failure_message = $5
WHERE action_id = $1
  AND attempt = $2
  AND fencing_token = $3
  AND state = 'open';

-- name: AbandonActionAttempt :execrows
WITH abandoned AS (
  UPDATE lutra.action_attempts
  SET state = 'lost',
    end_time = now(),
    failure_message = $4
  WHERE lutra.action_attempts.action_id = $1
    AND lutra.action_attempts.attempt = $2
    AND lutra.action_attempts.fencing_token = $3
    AND lutra.action_attempts.state = 'open'
  RETURNING action_id
)
UPDATE lutra.actions AS a
SET state = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN 'canceled'::lutra.action_state ELSE 'ready'::lutra.action_state END,
  end_time = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN coalesce(a.end_time, now()) ELSE a.end_time END,
  update_time = now(),
  failure_message = $4
FROM abandoned
WHERE a.action_id = abandoned.action_id
  AND a.attempt_count = $2
  AND a.state = 'running';

-- name: RecoverOrphanedActions :many
UPDATE lutra.actions AS a
SET state = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN 'canceled'::lutra.action_state ELSE 'ready'::lutra.action_state END,
  end_time = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN coalesce(a.end_time, now()) ELSE a.end_time END,
  update_time = now(),
  failure_message = 'previous attempt had no durable lease'
WHERE a.state = 'running'
  AND a.attempt_count > 0
  AND NOT EXISTS (
    SELECT 1
    FROM lutra.action_attempts AS aa
    WHERE aa.action_id = a.action_id
      AND aa.attempt = a.attempt_count
  )
  AND EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
  )
RETURNING a.action_id, a.project_id, a.run_id, a.parent_action_id, a.operation_id, a.task_id, a.state, a.inputs, a.outputs, a.failure_message, a.attempt_count, a.create_time, a.update_time, a.start_time, a.end_time;

-- name: FinishAction :one
UPDATE lutra.actions AS a
SET state = $4,
  outputs = $5,
  failure_message = $6,
  end_time = now(),
  update_time = now()
WHERE a.action_id = $1
  AND a.project_id = $2
  AND a.attempt_count = $3
  AND a.state = 'running'
  AND EXISTS (
    SELECT 1
    FROM lutra.action_attempts AS aa
    WHERE aa.action_id = $1
      AND aa.attempt = $3
      AND aa.fencing_token = $7
      AND aa.state = 'open'
  )
RETURNING action_id, project_id, run_id, parent_action_id, operation_id, task_id, state, inputs, outputs, failure_message, attempt_count, create_time, update_time, start_time, end_time;

-- name: RenewActionAttempt :one
UPDATE lutra.action_attempts
-- Renewal must re-anchor the lease on the current time. Extending the
-- previous expiry instead pushes it further out on every renewal, so a
-- worker that later dies would not be recovered for hours.
SET lease_expires_at = now() + '10 minutes'::interval
WHERE action_id = $1
  AND attempt = $2
  AND fencing_token = $3
  AND state = 'open'
  AND lease_expires_at > now()
RETURNING action_id, attempt, state, lease_owner, lease_expires_at, fencing_token, start_time, end_time, failure_message;

-- name: GetRootAction :one
SELECT
  action_id,
  project_id,
  run_id,
  parent_action_id,
  operation_id,
  task_id,
  state,
  inputs,
  outputs,
  failure_message,
  attempt_count,
  create_time,
  update_time,
  start_time,
  end_time
FROM lutra.actions
WHERE run_id = $1
  AND parent_action_id IS NULL;

-- name: ListTasksByBundleArtifact :many
SELECT
  task_id,
  name,
  version
FROM lutra.tasks
WHERE project_id = $1
  AND code_bundle_artifact_id = $2;

-- name: CountChildActions :one
SELECT count(*)
FROM lutra.actions
WHERE parent_action_id = $1;

-- name: CancelActionsForRun :exec
UPDATE lutra.actions
SET
  state = 'canceled',
  end_time = COALESCE(end_time, now()),
  update_time = now()
WHERE
  run_id = $1
  AND state NOT IN ('succeeded', 'failed', 'canceled', 'timed_out');

-- name: CancelAttemptsForRun :exec
UPDATE lutra.action_attempts
SET state = 'canceled',
  end_time = coalesce(end_time, now()),
  failure_message = 'run canceled'
WHERE action_id IN (
    SELECT action_id
    FROM lutra.actions
    WHERE run_id = $1
  )
  AND state = 'open';

-- name: CancelRunState :one
UPDATE lutra.runs
SET
  state = 'cancelled',
  update_time = now()
WHERE
  run_id = $1
  AND project_id = $2
  AND state NOT IN ('succeeded', 'failed', 'cancelled')
RETURNING
  run_id,
  project_id,
  idempotency_key,
  state,
  create_time,
  update_time;

-- name: RecoverExpiredActions :many
WITH expired AS (
  UPDATE lutra.action_attempts
  SET state = 'lost',
    end_time = now(),
    failure_message = 'attempt lease expired'
  WHERE state = 'open'
    AND lease_expires_at <= now()
  RETURNING action_id, attempt
)
UPDATE lutra.actions AS a
SET state = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN 'canceled'::lutra.action_state ELSE 'ready'::lutra.action_state END,
  end_time = CASE WHEN EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
      AND r.state IN ('cancelled', 'failed', 'succeeded')
  ) THEN coalesce(a.end_time, now()) ELSE a.end_time END,
  update_time = now(),
  failure_message = 'previous attempt lease expired'
FROM expired
WHERE a.action_id = expired.action_id
  AND a.attempt_count = expired.attempt
  AND a.state = 'running'
  AND EXISTS (
    SELECT 1
    FROM lutra.runs AS r
    WHERE r.run_id = a.run_id
      AND r.project_id = a.project_id
  )
RETURNING a.action_id, a.project_id, a.run_id, a.parent_action_id, a.operation_id, a.task_id, a.state, a.inputs, a.outputs, a.failure_message, a.attempt_count, a.create_time, a.update_time, a.start_time, a.end_time;

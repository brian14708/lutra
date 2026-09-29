-- name: InsertTaskSpec :exec
INSERT INTO lutra.task_specs (project, domain, name, version, source_sha256, image, module, qualname)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (project, domain, name, version) DO NOTHING;

-- name: GetTaskSpec :one
SELECT module, qualname, source_sha256, image FROM lutra.task_specs
WHERE project = $1 AND domain = $2 AND name = $3 AND version = $4;

-- name: GetParentSourceDigest :one
SELECT t.source_sha256 FROM lutra.runs r
JOIN lutra.task_specs t USING (project, domain, name, version)
WHERE r.id = $1 AND r.status IN ('running', 'waiting') AND r.claim_token IS NOT NULL;

-- name: InsertRun :one
INSERT INTO lutra.runs (id, parent_id, idempotency_key, project, domain, name, version, input_cbor, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued')
ON CONFLICT (project, domain, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetRunByIdempotencyKey :one
SELECT id, parent_id, name, version, input_cbor FROM lutra.runs
WHERE project = $1 AND domain = $2 AND idempotency_key = $3;

-- name: ReadRun :one
SELECT r.parent_id, r.project, r.domain, r.name, r.version,
       t.module, t.qualname, t.source_sha256, t.image,
       r.status, r.attempts, r.output_cbor, r.error, r.created_at, r.updated_at
FROM lutra.runs r JOIN lutra.task_specs t USING (project, domain, name, version)
WHERE r.id = $1;

-- name: GetChildParentID :one
SELECT parent_id FROM lutra.runs WHERE id = $1;

-- name: CancelRun :execrows
UPDATE lutra.runs SET status = 'canceled', error = 'run canceled',
       claim_token = NULL, lease_until = NULL, updated_at = now()
WHERE id = $1 AND status NOT IN ('succeeded', 'failed', 'canceled');

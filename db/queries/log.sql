-- name: RunExists :one
SELECT EXISTS (SELECT 1 FROM lutra.runs WHERE id = $1);

-- name: LockLogStream :one
SELECT run_id, stream, next_seq
FROM lutra.run_log_streams
WHERE run_id = $1 AND stream = $2
FOR UPDATE;

-- name: CreateLogStream :exec
INSERT INTO lutra.run_log_streams (run_id, stream, next_seq)
VALUES ($1, $2, $3)
ON CONFLICT (run_id, stream) DO NOTHING;

-- name: UpdateLogStreamNextSeq :exec
UPDATE lutra.run_log_streams SET next_seq = $3, updated_at = now()
WHERE run_id = $1 AND stream = $2;

-- name: GetLogAppend :one
SELECT run_id, stream, append_id, batch_digest, first_seq, last_seq
FROM lutra.run_log_appends
WHERE run_id = $1 AND stream = $2 AND append_id = $3;

-- name: InsertLogAppend :exec
INSERT INTO lutra.run_log_appends
  (run_id, stream, append_id, batch_digest, first_seq, last_seq)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertLogRecord :exec
INSERT INTO lutra.run_log_records
  (run_id, stream, seq, value_cbor, value_uri, payload_size, created_at)
VALUES ($1, $2, $3, $4, $5, $6, now());

-- name: ReadLogRecords :many
SELECT stream, seq, value_cbor, value_uri, created_at
FROM lutra.run_log_records
WHERE run_id = $1 AND stream = $2 AND seq > $3
ORDER BY seq
LIMIT $4;

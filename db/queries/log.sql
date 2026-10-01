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
UPDATE lutra.run_log_streams
SET next_seq = $3, updated_at = now()
WHERE run_id = $1 AND stream = $2;

-- name: GetLogAppend :one
SELECT run_id, stream, append_id, batch_digest, first_seq, last_seq
FROM lutra.run_log_appends
WHERE run_id = $1 AND stream = $2 AND append_id = $3;

-- name: InsertLogAppend :exec
INSERT INTO lutra.run_log_appends (run_id, stream, append_id, batch_digest, first_seq, last_seq)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertLogRecord :exec
INSERT INTO lutra.run_log_records (run_id, stream, seq, value_cbor, value_uri, payload_size, key, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now());

-- name: ReadLogRecords :many
SELECT stream, seq, key, value_cbor, value_uri, created_at
FROM lutra.run_log_records
WHERE run_id = $1 AND stream = $2 AND seq > $3
ORDER BY seq
LIMIT $4;

-- name: LogStreamEnd :one
SELECT coalesce( (SELECT next_seq - 1 FROM lutra.run_log_streams WHERE run_id = $1 AND stream = $2), 0)::bigint AS seq;

-- name: ReadFilteredLogRecords :many
WITH boundary AS MATERIALIZED (
  SELECT COALESCE((SELECT s.next_seq - 1 FROM lutra.run_log_streams s
    WHERE s.run_id = sqlc.arg(run_id) AND s.stream = sqlc.arg(stream)), 0)::bigint AS end_seq
), matched AS MATERIALIZED (
  -- Separate branches keep key bounds indexable with generic prepared plans.
  SELECT candidates.* FROM (
    SELECT r.stream, r.seq, r.key, r.value_cbor, r.value_uri, r.created_at
    FROM lutra.run_log_records r, boundary b
    WHERE r.run_id = sqlc.arg(run_id) AND r.stream = sqlc.arg(stream)
      AND r.seq > sqlc.arg(after_seq) AND r.seq <= b.end_seq
      AND sqlc.narg(lower_key)::bytea IS NULL
    UNION ALL
    SELECT r.stream, r.seq, r.key, r.value_cbor, r.value_uri, r.created_at
    FROM lutra.run_log_records r, boundary b
    WHERE r.run_id = sqlc.arg(run_id) AND r.stream = sqlc.arg(stream)
      AND r.seq > sqlc.arg(after_seq) AND r.seq <= b.end_seq
      AND sqlc.narg(lower_key)::bytea IS NOT NULL AND sqlc.narg(upper_key)::bytea IS NULL
      AND r.key >= sqlc.narg(lower_key)
    UNION ALL
    SELECT r.stream, r.seq, r.key, r.value_cbor, r.value_uri, r.created_at
    FROM lutra.run_log_records r, boundary b
    WHERE r.run_id = sqlc.arg(run_id) AND r.stream = sqlc.arg(stream)
      AND r.seq > sqlc.arg(after_seq) AND r.seq <= b.end_seq
      AND r.key >= sqlc.narg(lower_key) AND r.key < sqlc.narg(upper_key)
  ) candidates
  ORDER BY candidates.seq LIMIT sqlc.arg(batch_size)::integer
)
SELECT m.stream, m.seq, m.key, m.value_cbor, m.value_uri, m.created_at,
  CASE WHEN (SELECT count(*) FROM matched) = sqlc.arg(batch_size)::integer
    THEN (SELECT max(seq) FROM matched) ELSE b.end_seq END::bigint AS inspect_seq
FROM boundary b LEFT JOIN matched m ON true
ORDER BY m.seq;

-- name: NotifyLogStream :exec
SELECT pg_notify('lutra_logs', sqlc.arg(identity)::text);

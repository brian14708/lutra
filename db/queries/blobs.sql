-- name: GetBlobBySHA256 :one
SELECT * FROM lutra.blobs WHERE sha256 = $1;

-- name: CreateBlobUpload :exec
INSERT INTO lutra.blob_uploads (session_id, sha256, object_key, size, mime_type, metadata, multipart_id)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetBlobUpload :one
SELECT * FROM lutra.blob_uploads WHERE session_id = $1 AND expires_at > now();

-- name: LockBlobUpload :one
SELECT * FROM lutra.blob_uploads WHERE session_id = $1 FOR UPDATE;

-- name: InsertBlobIfAbsent :execrows
INSERT INTO lutra.blobs (sha256, object_key)
VALUES ($1, $2)
ON CONFLICT (sha256) DO NOTHING;

-- name: DeleteBlobUpload :exec
DELETE FROM lutra.blob_uploads WHERE session_id = $1;

-- name: ListExpiredBlobUploads :many
SELECT * FROM lutra.blob_uploads WHERE expires_at <= now() LIMIT 100;

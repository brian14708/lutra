-- name: InsertRunSetting :exec
INSERT INTO lutra.run_settings (run_id, path, value_cbor, sensitive) VALUES ($1, $2, $3, $4);

-- name: ListRunSettings :many
SELECT path, value_cbor, sensitive FROM lutra.run_settings WHERE run_id = $1 ORDER BY path;

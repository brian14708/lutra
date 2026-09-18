-- name: ListActiveProjectMemberships :many
SELECT
  user_id,
  project_id,
  role
FROM lutra.project_memberships;

-- name: HasActiveProjectMembership :one
SELECT
  EXISTS (
    SELECT 1
    FROM lutra.project_memberships AS m
    JOIN lutra.projects AS p ON p.project_id = m.project_id
    WHERE m.user_id = $1
      AND m.project_id = $2
      AND p.status = 'active'
  );

-- name: TouchAPIKeyPrincipal :one
WITH candidate AS (
  SELECT
    k.api_key_id,
    k.user_id
  FROM lutra.api_keys AS k
  JOIN lutra.users AS u ON u.user_id = k.user_id
  WHERE k.secret_hash = $1
    AND k.revoke_time IS NULL
    AND (k.expire_time IS NULL OR k.expire_time > now())
    AND u.status = 'active'
),
touched AS (
  UPDATE lutra.api_keys AS k
  SET last_used_time = now()
  FROM candidate AS c
  WHERE k.api_key_id = c.api_key_id
    AND (k.last_used_time IS NULL OR k.last_used_time < now() - '5 minutes'::interval)
)
SELECT user_id
FROM candidate;

-- name: FindOIDCUser :one
SELECT i.user_id
FROM lutra.oidc_identities AS i
JOIN lutra.users AS u ON u.user_id = i.user_id
WHERE i.issuer = $1
  AND i.subject = $2
  AND u.status = 'active';

-- name: InsertOIDCIdentity :exec
INSERT INTO lutra.oidc_identities (issuer, subject, user_id)
VALUES ($1, $2, $3)
ON CONFLICT (issuer, subject) DO UPDATE SET
  user_id = excluded.user_id;

-- name: AcquireBootstrapLock :exec
SELECT pg_advisory_xact_lock(hashtext('github.com/brian14708/lutra:bootstrap'));

-- name: AdminExists :one
SELECT
  EXISTS (
    SELECT 1
    FROM lutra.project_memberships
    WHERE role = 'admin'
  );

-- name: UsersExist :one
SELECT
  EXISTS (
    SELECT 1
    FROM lutra.users
  );

-- name: InsertUser :one
INSERT INTO lutra.users (user_id, display_name, email)
VALUES ($1, $2, $3)
RETURNING user_id, display_name, email, status, create_time, update_time;

-- name: InsertProjectMembership :exec
INSERT INTO lutra.project_memberships (project_id, user_id, role)
VALUES ($1, $2, $3);

-- name: InsertAPIKey :one
INSERT INTO lutra.api_keys (
  api_key_id,
  user_id,
  name,
  prefix,
  secret_hash,
  expire_time
)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING api_key_id, user_id, name, prefix, create_time, expire_time, revoke_time;

-- name: ListUsers :many
SELECT
  user_id,
  display_name,
  email,
  status,
  create_time,
  update_time
FROM lutra.users
ORDER BY create_time;

-- name: GetUser :one
SELECT
  user_id,
  display_name,
  email,
  status,
  create_time,
  update_time
FROM lutra.users
WHERE user_id = $1;

-- name: UpdateUserStatus :one
UPDATE lutra.users
SET status = $2,
  update_time = now()
WHERE user_id = $1
RETURNING user_id, display_name, email, status, create_time, update_time;

-- name: GetAPIKeyOwner :one
SELECT
  user_id,
  name,
  expire_time
FROM lutra.api_keys
WHERE api_key_id = $1;

-- name: ListAPIKeys :many
SELECT
  api_key_id,
  user_id,
  name,
  prefix,
  create_time,
  expire_time,
  revoke_time
FROM lutra.api_keys
WHERE user_id = $1
ORDER BY create_time;

-- name: RevokeAPIKey :one
UPDATE lutra.api_keys
SET revoke_time = coalesce(revoke_time, now())
WHERE api_key_id = $1
RETURNING api_key_id, user_id, name, prefix, create_time, expire_time, revoke_time;

-- name: ListProjectsForUser :many
SELECT
  p.project_id,
  p.name,
  p.metadata,
  p.create_time,
  p.update_time
FROM lutra.projects AS p
JOIN lutra.project_memberships AS m ON m.project_id = p.project_id
WHERE p.status = 'active'
  AND m.user_id = $1
ORDER BY p.create_time;

-- name: AnyAdmin :one
SELECT
  EXISTS (
    SELECT 1
    FROM lutra.project_memberships
    WHERE user_id = $1
      AND role = 'admin'
  );

-- name: AssignProjectRole :one
INSERT INTO lutra.project_memberships (project_id, user_id, role)
VALUES ($1, $2, $3)
ON CONFLICT (project_id, user_id) DO UPDATE SET
  role = excluded.role,
  update_time = now()
RETURNING project_id, user_id, role;

-- +goose Up
CREATE TYPE lutra.user_status AS ENUM('active', 'suspended');

CREATE TYPE lutra.project_status AS ENUM('active', 'archived');

CREATE TYPE lutra.project_role AS ENUM(
  'viewer',
  'developer',
  'runner',
  'operator',
  'admin'
);

CREATE TABLE IF NOT EXISTS lutra.users (
  user_id uuid PRIMARY KEY,
  display_name text NOT NULL,
  email text NOT NULL DEFAULT '',
  status lutra.user_status NOT NULL DEFAULT 'active',
  create_time timestamptz NOT NULL DEFAULT now(),
  update_time timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lutra.projects (
  project_id uuid PRIMARY KEY,
  name text NOT NULL UNIQUE,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  status lutra.project_status NOT NULL DEFAULT 'active',
  create_time timestamptz NOT NULL DEFAULT now(),
  update_time timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lutra.project_memberships (
  project_id uuid NOT NULL REFERENCES lutra.projects (project_id) ON DELETE CASCADE,
  user_id uuid NOT NULL REFERENCES lutra.users (user_id) ON DELETE CASCADE,
  role lutra.project_role NOT NULL,
  expire_time timestamptz,
  revoke_time timestamptz,
  create_time timestamptz NOT NULL DEFAULT now(),
  update_time timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, user_id)
);

CREATE TABLE IF NOT EXISTS lutra.api_keys (
  api_key_id uuid PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES lutra.users (user_id) ON DELETE CASCADE,
  name text NOT NULL,
  prefix text NOT NULL,
  secret_hash bytea NOT NULL UNIQUE,
  create_time timestamptz NOT NULL DEFAULT now(),
  expire_time timestamptz,
  revoke_time timestamptz,
  last_used_time timestamptz
);

CREATE TABLE IF NOT EXISTS lutra.oidc_identities (
  issuer text NOT NULL,
  subject text NOT NULL,
  user_id uuid NOT NULL REFERENCES lutra.users (user_id) ON DELETE CASCADE,
  create_time timestamptz NOT NULL DEFAULT now(),
  last_seen_time timestamptz,
  PRIMARY KEY (issuer, subject)
);

CREATE TABLE IF NOT EXISTS lutra.bootstrap_state (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  completed_time timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lutra.audit_events (
  audit_id bigserial PRIMARY KEY,
  user_id text,
  action text NOT NULL,
  project_id text,
  resource_id text,
  create_time timestamptz NOT NULL DEFAULT now(),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS api_keys_user_idx ON lutra.api_keys (user_id);

CREATE INDEX IF NOT EXISTS project_memberships_user_project_idx ON lutra.project_memberships (user_id, project_id);

CREATE INDEX IF NOT EXISTS oidc_identities_user_idx ON lutra.oidc_identities (user_id);

-- +goose Down
DROP TABLE IF EXISTS lutra.audit_events;

DROP TABLE IF EXISTS lutra.bootstrap_state;

DROP TABLE IF EXISTS lutra.oidc_identities;

DROP TABLE IF EXISTS lutra.api_keys;

DROP TABLE IF EXISTS lutra.project_memberships;

DROP TABLE IF EXISTS lutra.projects;

DROP TABLE IF EXISTS lutra.users;

DROP TYPE IF EXISTS lutra.project_role;

DROP TYPE IF EXISTS lutra.project_status;

DROP TYPE IF EXISTS lutra.user_status;

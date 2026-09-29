-- +goose Up
-- Development baseline: schema changes stay in 0001; recreate databases that applied an older 0001.
CREATE SCHEMA IF NOT EXISTS lutra;

CREATE TABLE lutra.blobs (
    sha256 bytea PRIMARY KEY CHECK (length(sha256) = 32),
    object_key uuid NOT NULL
);

CREATE TABLE lutra.blob_uploads (
    session_id uuid PRIMARY KEY,
    sha256 bytea NOT NULL CHECK (length(sha256) = 32),
    object_key uuid NOT NULL UNIQUE,
    size bigint NOT NULL CHECK (size >= 0),
    mime_type text NOT NULL,
    multipart_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours'
);
CREATE INDEX blob_uploads_expiry_idx ON lutra.blob_uploads (expires_at);

CREATE TABLE lutra.projects (
    id uuid PRIMARY KEY,
    slug text UNIQUE NOT NULL CHECK (slug ~ '^[a-z][a-z0-9-]{0,62}$'),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lutra.domains (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES lutra.projects(id) ON DELETE CASCADE,
    slug text NOT NULL CHECK (slug ~ '^[a-z][a-z0-9-]{0,62}$'),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project_id, slug),
    UNIQUE (project_id, id)
);

CREATE TABLE lutra.settings (
    project_id uuid NOT NULL REFERENCES lutra.projects(id) ON DELETE CASCADE,
    domain_id uuid NULL,
    path text NOT NULL CHECK (path ~ '^[a-z][a-z0-9_-]{0,63}(/[a-z][a-z0-9_-]{0,63})*$'),
    value bytea NOT NULL CHECK (length(value) BETWEEN 1 AND 65536),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (project_id, domain_id, path),
    FOREIGN KEY (project_id, domain_id) REFERENCES lutra.domains(project_id, id) ON DELETE CASCADE
);
CREATE INDEX settings_project_path_idx ON lutra.settings (project_id, path);
CREATE INDEX settings_domain_path_idx ON lutra.settings (project_id, domain_id, path);

CREATE TABLE lutra.task_specs (
    project text NOT NULL,
    domain text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    source_sha256 bytea NOT NULL REFERENCES lutra.blobs(sha256),
    image text NOT NULL,
    module text NOT NULL,
    qualname text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project, domain, name, version)
);

CREATE TABLE lutra.runs (
    id uuid PRIMARY KEY,
    parent_id uuid REFERENCES lutra.runs(id),
    idempotency_key text,
    project text NOT NULL,
    domain text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    input_cbor bytea NOT NULL,
    output_cbor bytea,
    status text NOT NULL CHECK (status IN ('queued', 'running', 'waiting', 'succeeded', 'failed', 'canceled')),
    attempts integer NOT NULL DEFAULT 0,
    claim_token uuid,
    lease_until timestamptz,
    error text NOT NULL DEFAULT '',
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (project, domain, name, version) REFERENCES lutra.task_specs(project, domain, name, version)
);
CREATE INDEX runs_status_idx ON lutra.runs (status, created_at);
CREATE INDEX runs_parent_idx ON lutra.runs (parent_id);
CREATE UNIQUE INDEX runs_idempotency_idx ON lutra.runs (project, domain, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS lutra.runs;
DROP TABLE IF EXISTS lutra.task_specs;
DROP TABLE IF EXISTS lutra.blob_uploads;
DROP TABLE IF EXISTS lutra.blobs;
DROP TABLE IF EXISTS lutra.settings;
DROP TABLE IF EXISTS lutra.domains;
DROP TABLE IF EXISTS lutra.projects;
DROP SCHEMA IF EXISTS lutra;

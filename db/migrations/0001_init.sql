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

CREATE TYPE lutra.task_action_status AS ENUM ('queued', 'running', 'waiting', 'succeeded', 'failed', 'canceled');

CREATE TABLE lutra.runs (
    id uuid PRIMARY KEY,
    project text NOT NULL,
    domain text NOT NULL,
    root_idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, project, domain)
);
CREATE UNIQUE INDEX runs_idempotency_idx ON lutra.runs (project, domain, root_idempotency_key) WHERE root_idempotency_key IS NOT NULL;

CREATE TABLE lutra.task_actions (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES lutra.runs(id) ON DELETE CASCADE,
    caller_action_id uuid,
    project text NOT NULL,
    domain text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    input_cbor bytea NOT NULL,
    output_cbor bytea,
    status lutra.task_action_status NOT NULL DEFAULT 'queued',
    error text NOT NULL DEFAULT '',
    attempts integer NOT NULL DEFAULT 0,
    claim_token uuid,
    lease_until timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (project, domain, name, version) REFERENCES lutra.task_specs(project, domain, name, version),
    FOREIGN KEY (run_id, project, domain) REFERENCES lutra.runs(id, project, domain),
    FOREIGN KEY (run_id, caller_action_id) REFERENCES lutra.task_actions(run_id, id),
    UNIQUE (run_id, id)
);
CREATE UNIQUE INDEX task_actions_one_root_idx ON lutra.task_actions (run_id) WHERE caller_action_id IS NULL;
CREATE UNIQUE INDEX task_actions_idempotency_idx ON lutra.task_actions (run_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX task_actions_claim_idx ON lutra.task_actions (status, next_attempt_at, created_at);
CREATE INDEX task_actions_run_caller_idx ON lutra.task_actions (run_id, caller_action_id, created_at, id);

ALTER TABLE lutra.runs
    ADD COLUMN root_action_id uuid,
    ADD CONSTRAINT runs_root_action_fk FOREIGN KEY (id, root_action_id) REFERENCES lutra.task_actions(run_id, id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE lutra.task_action_edges (
    run_id uuid NOT NULL REFERENCES lutra.runs(id) ON DELETE CASCADE,
    source_action_id uuid NOT NULL,
    dependent_action_id uuid NOT NULL,
    PRIMARY KEY (run_id, source_action_id, dependent_action_id),
    CHECK (source_action_id <> dependent_action_id),
    FOREIGN KEY (run_id, source_action_id) REFERENCES lutra.task_actions(run_id, id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, dependent_action_id) REFERENCES lutra.task_actions(run_id, id) ON DELETE CASCADE
);
CREATE INDEX task_action_edges_dependent_idx ON lutra.task_action_edges (run_id, dependent_action_id);

CREATE TABLE lutra.run_log_streams (
    run_id uuid NOT NULL REFERENCES lutra.runs(id) ON DELETE CASCADE,
    stream text NOT NULL CHECK (stream ~ '^[a-zA-Z_][a-zA-Z0-9_.-]{0,127}$'),
    next_seq bigint NOT NULL DEFAULT 1 CHECK (next_seq >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stream)
);

CREATE TABLE lutra.run_log_appends (
    run_id uuid NOT NULL,
    stream text NOT NULL,
    append_id text NOT NULL CHECK (length(append_id) BETWEEN 1 AND 200),
    batch_digest bytea NOT NULL CHECK (length(batch_digest) = 32),
    first_seq bigint NOT NULL CHECK (first_seq >= 1),
    last_seq bigint NOT NULL CHECK (last_seq >= first_seq),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stream, append_id),
    FOREIGN KEY (run_id, stream) REFERENCES lutra.run_log_streams(run_id, stream) ON DELETE CASCADE
);

CREATE TABLE lutra.run_log_records (
    run_id uuid NOT NULL,
    stream text NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 1),
    key bytea NOT NULL CHECK (length(key) <= 1024),
    value_cbor bytea,
    value_uri text,
    payload_size bigint NOT NULL CHECK (payload_size > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stream, seq),
    FOREIGN KEY (run_id, stream) REFERENCES lutra.run_log_streams(run_id, stream) ON DELETE CASCADE,
    CHECK ((value_cbor IS NOT NULL) <> (value_uri IS NOT NULL))
);
CREATE INDEX run_log_records_key_idx ON lutra.run_log_records (run_id, stream, key, seq);

-- +goose Down
DROP TABLE IF EXISTS lutra.run_log_records;
DROP TABLE IF EXISTS lutra.run_log_appends;
DROP TABLE IF EXISTS lutra.run_log_streams;
DROP TABLE IF EXISTS lutra.task_action_edges;
ALTER TABLE IF EXISTS lutra.runs DROP CONSTRAINT IF EXISTS runs_root_action_fk;
DROP TABLE IF EXISTS lutra.task_actions;
DROP TABLE IF EXISTS lutra.runs;
DROP TABLE IF EXISTS lutra.task_specs;
DROP TYPE IF EXISTS lutra.task_action_status;
DROP TABLE IF EXISTS lutra.blob_uploads;
DROP TABLE IF EXISTS lutra.blobs;
DROP TABLE IF EXISTS lutra.settings;
DROP TABLE IF EXISTS lutra.domains;
DROP TABLE IF EXISTS lutra.projects;
DROP SCHEMA IF EXISTS lutra;

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
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    multipart_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours'
);
CREATE INDEX blob_uploads_expiry_idx ON lutra.blob_uploads (expires_at);

CREATE TABLE lutra.namespaces (
    id uuid PRIMARY KEY,
    slug text UNIQUE NOT NULL CHECK (slug ~ '^[a-z][a-z0-9-]{0,62}$'),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO lutra.namespaces (id, slug, name) VALUES (gen_random_uuid(), 'default', 'Default');

CREATE TABLE lutra.settings (
    namespace_id uuid NOT NULL REFERENCES lutra.namespaces(id) ON DELETE CASCADE,
    path text NOT NULL CHECK (path ~ '^[a-z][a-z0-9_-]{0,63}(/[a-z][a-z0-9_-]{0,63})*$'),
    value bytea NOT NULL CHECK (length(value) BETWEEN 1 AND 65536),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, path)
);

CREATE TABLE lutra.task_environments (
    id uuid PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES lutra.namespaces(id),
    name text NOT NULL,
    version text NOT NULL,
    provider text NOT NULL,
    spec bytea NOT NULL,
    image_key bytea NOT NULL CHECK (length(image_key) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name, version)
);

-- Mutable generations are deliberately excluded from environment identity.
CREATE TYPE lutra.image_build_status AS ENUM ('building', 'ready', 'failed');

CREATE TABLE lutra.image_builds (
    id uuid PRIMARY KEY,
    image_key bytea NOT NULL CHECK (length(image_key) = 32),
    status lutra.image_build_status NOT NULL,
    claim_token uuid NOT NULL,
    lease_until timestamptz NOT NULL,
    artifact_uri text NOT NULL DEFAULT '',
    error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'ready' OR artifact_uri <> '')
);
CREATE UNIQUE INDEX image_building_idx ON lutra.image_builds(image_key) WHERE status = 'building';

CREATE TYPE lutra.task_action_status AS ENUM ('queued', 'running', 'waiting', 'succeeded', 'failed', 'canceled');

CREATE TYPE lutra.task_cache_status AS ENUM ('building', 'ready');
CREATE TABLE lutra.task_cache (
    cache_key bytea PRIMARY KEY CHECK (length(cache_key) = 32),
    status lutra.task_cache_status NOT NULL,
    claim_token uuid NOT NULL,
    lease_until timestamptz NOT NULL,
    output_cbor bytea,
    error_code text NOT NULL DEFAULT '',
    error_details bytea,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'ready' OR output_cbor IS NOT NULL OR error_code <> '')
);
CREATE INDEX task_cache_lease_idx ON lutra.task_cache (lease_until);

CREATE TABLE lutra.runs (
    id uuid PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES lutra.namespaces(id),
    root_idempotency_key text,
    claim_token uuid,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX runs_idempotency_idx ON lutra.runs (namespace_id, root_idempotency_key) WHERE root_idempotency_key IS NOT NULL;
CREATE INDEX runs_claim_idx ON lutra.runs (lease_until, created_at);

CREATE TABLE lutra.task_actions (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES lutra.runs(id) ON DELETE CASCADE,
    caller_action_id uuid,
    environment_id uuid NOT NULL REFERENCES lutra.task_environments(id),
    entrypoint_id bigint NOT NULL CHECK (entrypoint_id BETWEEN 1 AND 4294967295),
    action_spec bytea NOT NULL,
    output_cbor bytea,
    status lutra.task_action_status NOT NULL DEFAULT 'queued',
    error text NOT NULL DEFAULT '',
    attempts integer NOT NULL DEFAULT 0,
    failures integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, caller_action_id) REFERENCES lutra.task_actions(run_id, id),
    UNIQUE (run_id, id)
);
CREATE UNIQUE INDEX task_actions_one_root_idx ON lutra.task_actions (run_id) WHERE caller_action_id IS NULL;
CREATE UNIQUE INDEX task_actions_idempotency_idx ON lutra.task_actions (run_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
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
DROP TABLE IF EXISTS lutra.task_cache;
DROP TYPE IF EXISTS lutra.task_cache_status;
DROP TABLE IF EXISTS lutra.image_builds;
DROP TABLE IF EXISTS lutra.task_environments;
DROP TYPE IF EXISTS lutra.image_build_status;
DROP TYPE IF EXISTS lutra.task_action_status;
DROP TABLE IF EXISTS lutra.blob_uploads;
DROP TABLE IF EXISTS lutra.blobs;
DROP TABLE IF EXISTS lutra.settings;
DROP TABLE IF EXISTS lutra.namespaces;
DROP SCHEMA IF EXISTS lutra;

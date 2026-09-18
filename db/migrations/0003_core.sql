-- +goose Up
CREATE TYPE lutra.run_state AS ENUM(
  'queued',
  'running',
  'succeeded',
  'failed',
  'cancelled'
);

CREATE TYPE lutra.action_state AS ENUM(
  'ready',
  'running',
  'succeeded',
  'failed',
  'canceled',
  'timed_out'
);

CREATE TYPE lutra.attempt_state AS ENUM('open', 'succeeded', 'failed', 'canceled', 'lost');

-- Core workflow state. Entity identifiers use PostgreSQL UUID values so the
-- database, SQLC models, and Go services share the same type.
-- Digest values are algorithm-qualified (for example sha256:<hex>) so the
-- storage format can evolve without changing the schema. Syntax and algorithm
-- support are validated by the application layer.
CREATE TABLE IF NOT EXISTS lutra.artifacts (
  artifact_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES lutra.projects (project_id) ON DELETE CASCADE,
  mime_type text NOT NULL DEFAULT '',
  size_bytes bigint NOT NULL DEFAULT 0,
  digest text,
  create_time timestamptz NOT NULL DEFAULT now(),
  seal_time timestamptz,
  CONSTRAINT artifacts_project_artifact_key UNIQUE (project_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS lutra.artifact_chunks (
  artifact_id uuid NOT NULL REFERENCES lutra.artifacts (artifact_id) ON DELETE CASCADE,
  chunk_index integer NOT NULL CHECK (chunk_index >= 0),
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  digest text NOT NULL,
  upload_id uuid NOT NULL,
  complete_time timestamptz,
  PRIMARY KEY (artifact_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS lutra.tasks (
  task_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES lutra.projects (project_id) ON DELETE CASCADE,
  name text NOT NULL,
  version text NOT NULL,
  entrypoint_argv jsonb NOT NULL,
  code_bundle_artifact_id uuid NOT NULL,
  input_slots jsonb NOT NULL DEFAULT '[]'::jsonb,
  output_slots jsonb NOT NULL DEFAULT '[]'::jsonb,
  create_time timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tasks_project_task_key UNIQUE (project_id, task_id),
  CONSTRAINT tasks_project_bundle_fkey FOREIGN KEY (project_id, code_bundle_artifact_id) REFERENCES lutra.artifacts (project_id, artifact_id),
  UNIQUE (project_id, name, version)
);

CREATE TABLE IF NOT EXISTS lutra.runs (
  run_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES lutra.projects (project_id) ON DELETE CASCADE,
  idempotency_key text,
  state lutra.run_state NOT NULL DEFAULT 'queued',
  create_time timestamptz NOT NULL DEFAULT now(),
  update_time timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT runs_project_run_key UNIQUE (project_id, run_id),
  CONSTRAINT runs_project_run_idempotency_key CHECK (
    idempotency_key IS NULL
    OR idempotency_key <> ''
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_project_idempotency_idx ON lutra.runs (project_id, idempotency_key)
WHERE
  idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS lutra.actions (
  action_id uuid PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES lutra.projects (project_id) ON DELETE CASCADE,
  run_id uuid NOT NULL,
  parent_action_id uuid,
  operation_id text CHECK (operation_id <> ''),
  task_id uuid NOT NULL,
  state lutra.action_state NOT NULL DEFAULT 'ready',
  inputs jsonb NOT NULL DEFAULT '[]'::jsonb,
  outputs jsonb NOT NULL DEFAULT '[]'::jsonb,
  failure_message text NOT NULL DEFAULT '',
  attempt_count integer NOT NULL DEFAULT 0,
  create_time timestamptz NOT NULL DEFAULT now(),
  update_time timestamptz NOT NULL DEFAULT now(),
  start_time timestamptz,
  end_time timestamptz,
  CONSTRAINT actions_project_run_fkey FOREIGN KEY (project_id, run_id) REFERENCES lutra.runs (project_id, run_id) ON DELETE CASCADE,
  CONSTRAINT actions_project_task_fkey FOREIGN KEY (project_id, task_id) REFERENCES lutra.tasks (project_id, task_id),
  CONSTRAINT actions_run_action_key UNIQUE (run_id, action_id),
  CONSTRAINT actions_parent_same_run_fkey FOREIGN KEY (run_id, parent_action_id) REFERENCES lutra.actions (run_id, action_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS actions_parent_operation_idx ON lutra.actions (parent_action_id, operation_id)
WHERE
  parent_action_id IS NOT NULL
  AND operation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS actions_one_root_per_run_idx ON lutra.actions (run_id)
WHERE
  parent_action_id IS NULL;

CREATE TABLE IF NOT EXISTS lutra.action_attempts (
  action_id uuid NOT NULL REFERENCES lutra.actions (action_id) ON DELETE CASCADE,
  attempt integer NOT NULL CHECK (attempt > 0),
  state lutra.attempt_state NOT NULL DEFAULT 'open',
  lease_owner text NOT NULL,
  lease_expires_at timestamptz NOT NULL,
  fencing_token uuid NOT NULL,
  start_time timestamptz NOT NULL DEFAULT now(),
  end_time timestamptz,
  failure_message text NOT NULL DEFAULT '',
  PRIMARY KEY (action_id, attempt),
  UNIQUE (action_id, fencing_token)
);

CREATE INDEX IF NOT EXISTS artifacts_project_idx ON lutra.artifacts (project_id);

CREATE INDEX IF NOT EXISTS tasks_project_idx ON lutra.tasks (project_id);

CREATE INDEX IF NOT EXISTS runs_project_idx ON lutra.runs (project_id, create_time DESC);

CREATE INDEX IF NOT EXISTS actions_run_idx ON lutra.actions (run_id, create_time);

CREATE INDEX IF NOT EXISTS actions_parent_idx ON lutra.actions (parent_action_id);

CREATE INDEX IF NOT EXISTS attempts_lease_idx ON lutra.action_attempts (state, lease_expires_at);

-- +goose Down
DROP TABLE IF EXISTS lutra.action_attempts;

DROP TABLE IF EXISTS lutra.actions;

DROP TABLE IF EXISTS lutra.runs;

DROP TABLE IF EXISTS lutra.tasks;

DROP TABLE IF EXISTS lutra.artifact_chunks;

DROP TABLE IF EXISTS lutra.artifacts;

DROP TYPE IF EXISTS lutra.attempt_state;

DROP TYPE IF EXISTS lutra.action_state;

DROP TYPE IF EXISTS lutra.run_state;

-- Job queue schema. Loaded once when the Postgres container is first created.
-- Every time comparison uses the database clock (now()), never a worker's clock.

CREATE TABLE jobs (
    id               BIGSERIAL   PRIMARY KEY,
    idempotency_key  TEXT        NOT NULL UNIQUE,      -- same key => same job
    payload          JSONB       NOT NULL DEFAULT '{}',
    payload_hash     TEXT        NOT NULL,             -- detects "same key, different body"
    state            TEXT        NOT NULL DEFAULT 'queued'
                     CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    attempt          INT         NOT NULL DEFAULT 0,   -- fencing token: +1 on every claim, never decreases
    max_attempts     INT         NOT NULL DEFAULT 5,
    lease_owner      TEXT,                             -- worker id holding the job (for humans/logs)
    lease_expires_at TIMESTAMPTZ,                      -- job is reclaimable after this moment
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ
);

-- claim(): queued jobs in id order.
CREATE INDEX jobs_queued_idx  ON jobs (id) WHERE state = 'queued';
-- claim() + reap(): running jobs whose lease has expired.
CREATE INDEX jobs_running_idx ON jobs (lease_expires_at) WHERE state = 'running';
-- list endpoint (cursor pagination, filter by state).
CREATE INDEX jobs_state_id_idx ON jobs (state, id);

-- One row every time a worker STARTS executing a job (at-least-once: may be > 1 per job).
CREATE TABLE executions (
    id         BIGSERIAL   PRIMARY KEY,
    job_id     BIGINT      NOT NULL REFERENCES jobs (id),
    attempt    INT         NOT NULL,
    worker_id  TEXT        NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX executions_job_idx ON executions (job_id);

-- The "real" side effect. PRIMARY KEY on job_id = the database refuses a second effect.
-- Written in the same statement that marks the job succeeded (see queries.COMPLETE).
CREATE TABLE side_effects (
    job_id     BIGINT      PRIMARY KEY REFERENCES jobs (id),
    attempt    INT         NOT NULL,
    worker_id  TEXT        NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

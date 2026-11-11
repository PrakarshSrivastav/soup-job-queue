"""Every SQL statement the system runs, in one place.

Rules that make these safe under concurrency (keep them when editing):
  1. Every decision is ONE statement. No "SELECT, decide in Python, then UPDATE".
  2. Every worker write is fenced: WHERE id = .. AND attempt = .. AND state = 'running'.
     If the job was re-claimed (attempt changed) or cancelled (state changed), 0 rows match.
  3. Time always comes from the database: now(). Worker clocks are never compared.
  4. attempt only ever goes up.
"""

# Take the oldest free job. "Free" = queued, or running with an expired lease (dead worker).
# FOR UPDATE SKIP LOCKED: a row another worker is claiming right now is skipped, not waited on.
# Jobs that used up their attempts are left for REAP_EXHAUSTED instead.
CLAIM = """
UPDATE jobs
SET state            = 'running',
    attempt          = attempt + 1,
    lease_owner      = %(worker_id)s,
    lease_expires_at = now() + make_interval(secs => %(lease_seconds)s),
    updated_at       = now()
WHERE id = (
    SELECT id FROM jobs
    WHERE (state = 'queued'
           OR (state = 'running' AND lease_expires_at < now()))
      AND attempt < max_attempts
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, attempt, payload
"""

# Record that execution started. Separate from side_effects on purpose:
# executions counts every run (may be > 1), side_effects must be exactly 1.
RECORD_EXECUTION = """
INSERT INTO executions (job_id, attempt, worker_id)
VALUES (%(job_id)s, %(attempt)s, %(worker_id)s)
"""

# Extend the lease. 0 rows => the job is no longer ours (cancelled, or re-claimed after
# our lease expired). The worker must stop and write nothing.
HEARTBEAT = """
UPDATE jobs
SET lease_expires_at = now() + make_interval(secs => %(lease_seconds)s),
    updated_at       = now()
WHERE id = %(job_id)s AND attempt = %(attempt)s AND state = 'running'
"""

# Mark succeeded AND write the side effect in ONE statement (one atomic commit).
# If the fenced UPDATE matches nothing, the INSERT inserts nothing: no effect.
# The side_effects primary key is a second wall against a duplicate effect.
# The effect records the WRITER's attempt (not the row's): with the fence they are equal,
# and if the fence were ever broken, a stale write would show up as a mismatch
# (found by a negative control in tests/silent_failures.py).
COMPLETE = """
WITH done AS (
    UPDATE jobs
    SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
        finished_at = now(), updated_at = now()
    WHERE id = %(job_id)s AND attempt = %(attempt)s AND state = 'running'
    RETURNING id
)
INSERT INTO side_effects (job_id, attempt, worker_id)
SELECT id, %(attempt)s, %(worker_id)s FROM done
RETURNING job_id
"""

# Used only when the connection died during COMPLETE and we don't know if it committed.
# If a side effect exists with OUR attempt number, our commit landed.
CHECK_COMPLETED_BY = """
SELECT 1 FROM side_effects WHERE job_id = %(job_id)s AND attempt = %(attempt)s
"""

# The work raised an error: put the job back for another try, or give up.
FAIL = """
UPDATE jobs
SET state            = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'queued' END,
    lease_owner      = NULL,
    lease_expires_at = NULL,
    last_error       = %(error)s,
    finished_at      = CASE WHEN attempt >= max_attempts THEN now() END,
    updated_at       = now()
WHERE id = %(job_id)s AND attempt = %(attempt)s AND state = 'running'
RETURNING state
"""

# Graceful shutdown (SIGTERM): hand the job back right away instead of waiting for the lease.
# attempt is NOT decreased (it is the fencing token), so this costs one attempt.
RELEASE = """
UPDATE jobs
SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = %(job_id)s AND attempt = %(attempt)s AND state = 'running'
"""

# Jobs whose worker died on their LAST attempt: CLAIM won't touch them, so without this
# they would sit in 'running' forever. Safe for many workers to run at once.
REAP_EXHAUSTED = """
UPDATE jobs
SET state = 'failed', lease_owner = NULL, lease_expires_at = NULL,
    last_error = 'max attempts reached (lease expired)',
    finished_at = now(), updated_at = now()
WHERE state = 'running' AND lease_expires_at < now() AND attempt >= max_attempts
RETURNING id
"""


# ------------------------------------------------------------------ API

JOB_COLUMNS = """id, idempotency_key, state, attempt, max_attempts, payload,
    lease_owner, lease_expires_at, last_error, created_at, updated_at, finished_at"""

# Create, or do nothing if the key already exists. The UNIQUE constraint decides, so two
# simultaneous requests with the same key can't both insert: the second one waits for the
# first to commit, then does nothing. No row returned => the key existed (see GET_BY_KEY).
CREATE_JOB = f"""
INSERT INTO jobs (idempotency_key, payload, payload_hash, max_attempts)
VALUES (%(key)s, %(payload)s, %(payload_hash)s, %(max_attempts)s)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING {JOB_COLUMNS}
"""

GET_BY_KEY = f"""
SELECT {JOB_COLUMNS}, payload_hash FROM jobs WHERE idempotency_key = %(key)s
"""

GET_JOB = f"""
SELECT {JOB_COLUMNS} FROM jobs WHERE id = %(job_id)s
"""

# Cursor pagination: "the next page starts after id X". Stable even while jobs are added,
# unlike OFFSET. Fetch limit+1 rows to know whether another page exists.
LIST_JOBS = f"""
SELECT {JOB_COLUMNS} FROM jobs
WHERE id > %(cursor)s
  AND (%(state)s::text IS NULL OR state = %(state)s::text)
ORDER BY id
LIMIT %(limit)s
"""

# Cancel = one guarded UPDATE. Only queued/running jobs can be cancelled.
# A running job's worker finds out through its fenced heartbeat/complete (0 rows),
# so whichever of cancel and complete commits first wins; the other changes nothing.
CANCEL = f"""
UPDATE jobs
SET state = 'cancelled', lease_owner = NULL, lease_expires_at = NULL,
    finished_at = now(), updated_at = now()
WHERE id = %(job_id)s AND state IN ('queued', 'running')
RETURNING {JOB_COLUMNS}
"""

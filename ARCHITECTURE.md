# Job queue: architecture

Status: **worker + supervisor done and smoke-tested.** API, chaos harness, design note: next.

## 1. The big picture

```
 chaos harness ──HTTP──▶ API (FastAPI) ──SQL──▶ Postgres ◀──SQL── supervisor ─▶ worker × N
        └────────────── kill -9 a random worker every ~2 s ──────────────────────▲
```

* **Postgres is the only thing that coordinates.** Workers never talk to each other,
  to the API, or to the supervisor. Every "who owns this job" decision is one SQL statement.
* The worker holds nothing important in memory. Kill it at any line and the database
  still has the truth; the job's lease simply expires and another worker takes it.
* Guarantee: **at-least-once execution, exactly-once effect.** A job may *start* more than
  once (after a crash), but only one run can *commit* its side effect.

## 2. Data model (`db/schema.sql`)

| table | purpose |
|---|---|
| `jobs` | one row per job: `state`, `attempt` (fencing token), `lease_owner`, `lease_expires_at`, `idempotency_key` (unique) |
| `executions` | one row every time a worker **starts** a job → may be > 1 per job (at-least-once) |
| `side_effects` | the "real" effect. `job_id` is the PRIMARY KEY → the DB refuses a 2nd effect |

States: `queued → running → succeeded | failed | cancelled`. `queued ← running` on retry.
Finished states are never changed again (every UPDATE requires a non-final state).

## 3. Four rules every SQL statement follows (`jobqueue/queries.py`)

1. **One statement per decision.** Never "SELECT, decide in Python, UPDATE".
2. **Every worker write is fenced:** `WHERE id = ? AND attempt = ? AND state = 'running'`.
   Re-claimed (attempt changed) or cancelled (state changed) → 0 rows → nothing written.
3. **Time comes only from the database** (`now()`). Worker clocks are never compared → no clock skew.
4. **`attempt` only goes up.** It is the proof of ownership.

## 4. The worker (`jobqueue/worker.py`)

### Identity
`worker_id = hostname-pid-random`. New id on every start, so a restarted process never
inherits its previous life's claims.

### In-memory state (all of it)
| field | meaning |
|---|---|
| `id` | worker name |
| current `Job(id, attempt, payload, lost)` | the claim; `attempt` = proof of ownership |
| `job.lost` (Event) | set by the heartbeat thread when the job is no longer ours |
| `stopping` (Event) | set by SIGTERM / Ctrl-C |

### Lifecycle
```
IDLE ─claim()─▶ RUNNING ─execute()─▶ complete() ─▶ IDLE
                  │  ├─ WorkError ─▶ fail()     (requeue, or 'failed' after max_attempts)
                  │  ├─ LostLease ─▶ write nothing (cancelled / presumed dead)
                  │  └─ SIGTERM  ─▶ release()  (hand the job back immediately)
                  └─ heartbeat thread renews the lease every second
```

### Main loop
```python
while not stopping:
    maybe_reap()                      # every 2 s: fail jobs that ran out of attempts
    job = claim()
    if job is None: sleep(backoff); continue    # 50 ms → 1 s, with jitter
    heartbeat.track(job)
    try:   execute(job); complete(job)
    except WorkError:    fail(job)
    except LostLease:    pass         # someone else owns it now
    except ShuttingDown: release(job)
    finally: heartbeat.track(None)
```

### Functions
| function | SQL | what it does |
|---|---|---|
| `claim()` | `CLAIM` | atomically takes the oldest queued job **or** a running job with an expired lease; `FOR UPDATE SKIP LOCKED`; `attempt += 1`; lease = now + 5 s |
| `execute()` | `RECORD_EXECUTION` | logs the start, then fake work: 100–500 ms in 10 steps, checking `lost`/`stopping` between steps; 2% random `WorkError` |
| heartbeat thread | `HEARTBEAT` | own DB connection; every 1 s extends the lease, fenced. 0 rows → sets `job.lost` |
| `complete()` | `COMPLETE` | **one statement** (CTE): mark succeeded + insert side effect. Fence fails → nothing written, logs `completion_rejected` |
| | `CHECK_COMPLETED_BY` | if the connection dies mid-commit: "is there a side effect with MY attempt?" → yes = it landed; no = retry (safe, fenced) |
| `fail()` | `FAIL` | fenced: back to `queued`, or `failed` if `attempt >= max_attempts` |
| `release()` | `RELEASE` | SIGTERM only: back to `queued` now (costs one attempt; attempt never decreases) |
| `maybe_reap()` | `REAP_EXHAUSTED` | expired lease + no attempts left → `failed` (CLAIM skips these, so without this they would stay `running` forever) |

### Choosing the lease length
* `HEARTBEAT_SECONDS = 1`, `LEASE_SECONDS = 5` → tolerates ~4 missed beats.
* Lower bound: lease must exceed the longest gap between a *healthy* worker's heartbeats.
  Measured in the smoke test: heartbeat latency p50 2 ms, max 12.8 ms → 5 s is very safe.
* Upper bound: how long a killed worker's job may sit idle before it is retried.
* A too-short lease costs **wasted work, never correctness** (the fence rejects the loser).

## 5. The supervisor (`jobqueue/supervisor.py`)

* Starts `WORKERS` processes (`python -m jobqueue.worker`), 4 by default, 8 for chaos.
* When one exits (e.g. `kill -9` → exit code -9) it logs it and starts a replacement after 0.5 s.
* Writes live PIDs to `PID_FILE` (atomic write) so the harness can pick one to kill.
* SIGTERM → forwards SIGTERM to all workers (they release their jobs), waits up to 10 s.
* Workers are **separate OS processes**, so `kill -9` kills exactly one worker.

## 6. How N workers work together
They don't coordinate at all. They are identical and only meet in the database:
`SKIP LOCKED` keeps them off each other's rows during a claim, leases hand back dead
workers' jobs, and the attempt fence rejects anyone who lost ownership. 4 → 8 workers
is a config change.

## 7. Files
```
db/schema.sql            tables + indexes
jobqueue/queries.py      every SQL statement (read this first)
jobqueue/worker.py       the worker
jobqueue/supervisor.py   keeps N workers alive
jobqueue/config.py       settings from env vars
jobqueue/logs.py         JSON-lines logging (harness evidence)
scripts/seed_jobs.py     insert jobs directly (until the API exists)
scripts/check.py         state counts + core invariants
Dockerfile, docker-compose.yml, requirements.txt
```

## 8. Run it
```bash
docker compose up --build -d                       # postgres + 4 workers
docker compose run --rm workers python -m scripts.seed_jobs 1000
docker compose run --rm workers python -m scripts.check
docker compose logs -f workers                     # watch JSON events
docker compose down -v                             # stop + wipe the database
WORKERS=8 docker compose up --build -d             # 8 workers
```

## 9. Smoke test (not the chaos harness yet)
1000 jobs, 8 workers, a random worker `kill -9`'d every 2 s (12 kills), one worker frozen
with `SIGSTOP` for ~8 s and then resumed.

```
state counts:   succeeded 1000
     0  succeeded jobs WITHOUT a side effect
     0  side effects on jobs NOT succeeded
     0  side effect from a stale attempt
     0  running with expired lease
    23  jobs executed more than once (allowed: at-least-once)
events: claimed 1023, completed 1000, completion_rejected 1, requeued 10, worker_exited 12
```

The zombie, caught in the act (job 50):
```
claimed              worker 388  attempt 1     <- then frozen (SIGSTOP)
claimed              worker 392  attempt 2     <- lease expired, re-claimed
completed            worker 392  attempt 2
completion_rejected  worker 388  attempt 1     <- woke up 3 s later; fence said no
```

## 10. Next
- [ ] API: create (idempotency key), get, list (cursor pagination), cancel
- [ ] chaos harness: 10k jobs, 8 workers, kill -9 every 2 s, duplicate POSTs, cancels
- [ ] negative controls: break the fence on purpose, confirm the harness catches it
- [ ] silent-failure tests, load test at 100 rps, DESIGN.md, RESULTS.md, AI_USAGE.md

## Known gaps (to discuss honestly)
* A worker that is alive but stuck forever keeps heartbeating → holds its job forever
  (heartbeat proves the *process* is alive, not that the job progresses).
* Exactly-once effect holds only because the effect is a row in the same database.
* Postgres is a single point of failure (correct when down, but not available).

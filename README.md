# Job queue: will it survive production?

A training-job queue service (API + Postgres + 8 workers), a chaos harness that kills
workers with `kill -9` while duplicate requests and cancels run in parallel, silent-failure
tests with negative controls, and a load test. Built for the Soup backend take-home.

**Video walkthrough:** _add link here_

## Run it

Needs Docker.

```bash
docker compose up --build -d                                              # postgres + api + 8 workers
docker compose run --build --rm harness                                   # chaos test: 10k jobs, kill -9 every ~2 s (~19 min)
docker compose run --build --rm harness python -m tests.silent_failures   # 9 silent-failure tests (~1 min)
docker compose run --build --rm -e LOAD_SECONDS=30 harness python -m harness.load   # load test
```

Always use `run --build` for the harness: `up --build` does not rebuild it (see `results/INCIDENT-002`).

The API is on `http://localhost:8000`:

| endpoint | what it does |
|---|---|
| `POST /jobs` `{"idempotency_key": "...", "payload": {...}}` | create; the same key returns the same job (`201` new, `200` existing, `409` same key with a different body) |
| `GET /jobs/{id}` | status |
| `GET /jobs?state=queued&cursor=0&limit=50` | list, cursor-paginated |
| `POST /jobs/{id}/cancel` | cancel a queued/running job; `409` if it already finished |

## How it works (short version)

Postgres is the only coordinator; workers never talk to each other. Full explanation:
**[ARCHITECTURE.md](ARCHITECTURE.md)**.

* **Claim:** one `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`
  statement, so two workers can't take the same job. Every claim increases `attempt`, the job's
  ticket number.
* **Dead workers:** a 5 s lease, renewed by a heartbeat every 1 s. When the lease expires,
  the job can be claimed again.
* **Zombies, and cancel vs. finish:** every worker write is fenced with
  `WHERE attempt = <my ticket> AND state = 'running'`. A stale or cancelled write matches 0 rows.
* **Duplicates:** `idempotency_key` is UNIQUE; create is `INSERT ... ON CONFLICT DO NOTHING`.
* **Guarantee:** at-least-once execution, **exactly-once effect**. The side effect is a row
  written in the same statement that marks the job succeeded, and its primary key allows only one.
* **Why Postgres, not SQLite:** row locks with `SKIP LOCKED` let 8 workers claim at the same time
  without blocking; SQLite allows one writer at a time. One database clock (`now()`) for every
  lease decision also means worker clocks never matter.

## Results

Everything is in [`results/`](results/), **including failed and invalid runs**.
`results/summary.txt` has one line per chaos run.

### Chaos harness (10,000 jobs, 8 workers, kill -9 every ~2 s): 13 runs

| runs | result |
|---|---|
| 7 full runs | **PASS**, ~550 kills each; the last (`run-20261111T173703Z`) is on the exact submitted code (`code=e23a5755e095`) |
| 3 small runs (1,000 jobs) | PASS |
| 2 full runs | **FAIL**: the containers ran mismatched code, so every POST returned 500 ([INCIDENT-001](results/INCIDENT-001-first-chaos-runs.md)) |
| 1 full run | **invalid**: said PASS with 0 kills, because a stale harness image was used ([INCIDENT-002](results/INCIDENT-002-harness-image-not-rebuilt.md)) |

Per passing run: 0 jobs lost, 0 double side effects, 0 stuck, every duplicate POST got the
same job, every cancel answer matched the final state, every kill was seen by the supervisor.
About 680 jobs per run executed more than once, each with exactly one effect.

**What the harness does NOT catch:** Postgres crashing or failing over; network partitions between
workers and the DB; the API crashing mid-request; long freezes (tested separately, below); clock
skew (there is one DB clock in this setup); rare timing windows that random kills never hit;
the "all attempts used up" path (it never happened in chaos runs; tested separately); and the
client-timeout retry, which barely triggered in Docker (tested deterministically instead).

### Silent failures: 9/9 OK ([results/silent-failures-*.txt](results/))

Each test runs the real code **and a broken copy with one safety check removed**, proving the
test can catch its bug:

| test | real code | broken copy |
|---|---|---|
| double claim (16 threads, 2,000 jobs) | 0 claimed twice | SELECT-then-UPDATE: 1,960 claimed twice |
| isolation level | correct at READ COMMITTED and REPEATABLE READ | naive code: silent doubles at READ COMMITTED |
| zombie (deterministic, 200x) | 0 zombie writes | no ticket check: zombie wins 200/200 |
| zombie (real processes, SIGSTOP) | frozen worker's result rejected | n/a |
| cancel vs. complete at the same instant (500x) | always consistent | no state check: 247 "cancelled" jobs succeeded |
| duplicate key, 20 concurrent requests (100x) | 1 job per key | no UNIQUE: 1,230 jobs for 100 keys |
| lost response then retry (real HTTP) | retry returns the same job | n/a |
| clock skew (worker -10 s / +1 h) | lease always exactly 5 s | worker-clock lease: job stolen / stuck 1 h |
| all attempts used up | job marked `failed`, nothing stuck | no reaper: stuck in `running` forever |

### Load test

| rate | achieved | errors | p50 | p99 | jobs created/s | jobs finished/s |
|---|---|---|---|---|---|---|
| 100 req/s | 100 | 0 | 4.2 ms | 10.8 ms | 60 | ~13 |
| 300 req/s | 300 | 0 | 3.0 ms | 8.5 ms | 180 | ~22 |
| 1000 req/s | jammed: the run hung and was stopped by hand ([load-hung-1000.txt](results/load-hung-1000.txt)) | | | | | |

## Verdict: DON'T SHIP at 100 requests/second

Correctness under failure is solid. But at production load three things break:

1. **Claims get slower as finished jobs pile up:** 0.45 ms with an empty history, **35 ms after
   150k finished jobs**, because the `queued OR expired` filter makes Postgres walk past every old
   row. At 100 req/s that's ~200k jobs an hour. **Fix first:** split it into two indexed lookups
   (tested: back to 0.41 ms, and it still passes all silent-failure tests; not applied here so the
   submitted results match the submitted code).
2. **Capacity:** ~60 new jobs/s arrive and 8 workers finish ~13/s, so the queue grows forever.
   Enough workers (~60, at 2 DB connections each) would exceed Postgres's default 100 connections.
   Needs a connection pooler (PgBouncer) and fewer connections per worker.
3. **No back-pressure:** under overload the API accepts everything and jams (1000 req/s run)
   instead of rejecting early with `429`/`503`.

Then: archive finished jobs, and run several API processes.

## Files

```
db/schema.sql              3 tables: jobs, executions (every start), side_effects (one per job)
jobqueue/queries.py        every SQL statement (start here)
jobqueue/worker.py         worker loop: claim, heartbeat, execute, complete / fail
jobqueue/supervisor.py     keeps N workers alive, restarts killed ones
jobqueue/api.py            FastAPI: create / get / list / cancel
harness/chaos.py           chaos test + invariant checks + pre-flight check
harness/load.py            load test (fixed rate, latency from when each request was due)
tests/silent_failures.py   9 silent-failure tests with negative controls
scripts/                   small dev helpers (seed jobs, quick check)
results/                   raw output of every run, incident reports
ARCHITECTURE.md            design note
aihelp.md                  how AI was used, and where it was wrong
```

## What surprised me / what still concerns me

The thing that surprised me most was how easily a test can pass while proving nothing. My
harness reported PASS on an empty database and on a run that killed nothing; both were caught
only by questioning the numbers behind the green line. The logs also always undercounted the
database by 2–3 completions per run, because workers die between committing and logging, so
the database, not the logs, has to be the source of truth. What still concerns me: a worker that
is alive but stuck forever keeps renewing its lease; the "exactly-once" effect only holds because
the effect lives in the same database; and Postgres is a single point of failure.

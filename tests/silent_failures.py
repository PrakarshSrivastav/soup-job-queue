"""Silent-failure tests.  Run with:  docker compose run --rm harness python -m tests.silent_failures

Each test runs the REAL code path, and a deliberately BROKEN variant (a "negative control").
A test is good evidence only if BOTH hold:
    real code   -> safe
    broken code -> the bug actually shows up (so the test is able to catch it)
Verdicts: OK (both hold), WEAK (real safe, but the control did not trigger), FAIL (real unsafe).

Tests run in their own Postgres schema ("silent"), so they never touch the real queue's jobs.
Most tests force the dangerous timing on purpose instead of hoping random chaos hits it.
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.conninfo import make_conninfo

from jobqueue import config, queries
from jobqueue.version import CODE_VERSION

SCHEMA = "silent"
DB = make_conninfo(config.DATABASE_URL, options=f"-c search_path={SCHEMA}")
API_URL = os.environ.get("API_URL", "http://localhost:8000")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")
SCHEMA_SQL = Path(__file__).resolve().parents[1] / "db" / "schema.sql"

# ---------------------------------------------------------------- broken variants (controls)
# Each is the real statement with ONE safety condition removed.

NAIVE_CLAIM_SELECT = "SELECT id FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1"
NAIVE_CLAIM_UPDATE = """UPDATE jobs SET state = 'running', attempt = attempt + 1, lease_owner = %(w)s
                        WHERE id = %(id)s RETURNING attempt"""

COMPLETE_WITHOUT_ATTEMPT_FENCE = queries.COMPLETE.replace("AND attempt = %(attempt)s ", "")
COMPLETE_WITHOUT_STATE_GUARD = queries.COMPLETE.replace(" AND state = 'running'", "")
assert COMPLETE_WITHOUT_ATTEMPT_FENCE != queries.COMPLETE
assert COMPLETE_WITHOUT_STATE_GUARD != queries.COMPLETE

# Lease end computed from the WORKER's clock instead of the database's.
CLAIM_WITH_WORKER_CLOCK = queries.CLAIM.replace(
    "lease_expires_at = now() + make_interval",
    "lease_expires_at = %(worker_now)s::timestamptz + make_interval")
assert CLAIM_WITH_WORKER_CLOCK != queries.CLAIM


# ---------------------------------------------------------------- helpers
def connect(isolation: str | None = None) -> psycopg.Connection:
    c = psycopg.connect(DB, autocommit=True)
    if isolation:
        c.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {isolation}")
    return c


def setup_schema() -> None:
    with psycopg.connect(config.DATABASE_URL, autocommit=True) as c:
        c.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        c.execute(f"CREATE SCHEMA {SCHEMA}")
    with connect() as c:
        c.execute(SCHEMA_SQL.read_text())
        c.execute("CREATE TABLE jobs_no_unique (id BIGSERIAL PRIMARY KEY, idempotency_key TEXT NOT NULL)")


def reset(c) -> None:
    c.execute("TRUNCATE side_effects, executions, jobs, jobs_no_unique RESTART IDENTITY CASCADE")


def add_jobs(c, n: int, max_attempts: int = 5) -> list[int]:
    rows = c.execute(
        """INSERT INTO jobs (idempotency_key, payload_hash, max_attempts)
           SELECT 'k' || g || '-' || clock_timestamp(), 'x', %s FROM generate_series(1, %s) g
           RETURNING id""", (max_attempts, n)).fetchall()
    return [r[0] for r in rows]


def claim(c, worker: str, lease: float = 30):
    row = c.execute(queries.CLAIM, {"worker_id": worker, "lease_seconds": lease}).fetchone()
    return (row[0], row[1]) if row else None


def complete(c, job_id, attempt, worker, sql=queries.COMPLETE) -> bool:
    return c.execute(sql, {"job_id": job_id, "attempt": attempt, "worker_id": worker}).fetchone() is not None


def expire_lease(c, job_id) -> None:
    """Simulates 'the lease ran out' without waiting: move the expiry into the past."""
    c.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s", (job_id,))


class Report:
    def __init__(self):
        self.lines: list[str] = []
        self.rows: list[tuple[str, str]] = []

    def say(self, msg: str = "") -> None:
        print(msg, flush=True)
        self.lines.append(msg)

    def test(self, name: str, how: str, real_ok: bool, real: str, caught: bool | None, control: str):
        verdict = "FAIL" if not real_ok else ("OK" if caught or caught is None else "WEAK")
        self.rows.append((name, verdict))
        self.say(f"\n## {name}   [{verdict}]")
        self.say(f"how:     {how}")
        self.say(f"real:    {'safe' if real_ok else 'UNSAFE'}  ({real})")
        if caught is not None:
            self.say(f"broken:  {'bug shown' if caught else 'bug NOT shown'}  ({control})")


R = Report()


# ---------------------------------------------------------------- 1. double claim + isolation
def run_claimers(claim_fn, isolation: str, jobs: int = 2000, threads: int = 16) -> dict:
    with connect() as c:
        reset(c)
        add_jobs(c, jobs)
    claims, errors, lock = [], Counter(), threading.Lock()

    def loop(t):
        with connect(isolation) as c:
            while True:
                try:
                    got = claim_fn(c, f"t{t}")
                except psycopg.errors.SerializationFailure:
                    with lock:
                        errors["serialization_failure"] += 1
                    continue
                if got is None:
                    return
                with lock:
                    claims.append(got)

    ts = [threading.Thread(target=loop, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    per_job = Counter(job for job, _ in claims)
    return {"claims": len(claims), "jobs": len(per_job),
            "claimed_twice": sum(1 for v in per_job.values() if v > 1), "errors": dict(errors)}


def naive_claim(c, worker):
    with c.transaction():
        row = c.execute(NAIVE_CLAIM_SELECT).fetchone()
        if row is None:
            return None
        attempt = c.execute(NAIVE_CLAIM_UPDATE, {"w": worker, "id": row[0]}).fetchone()[0]
        return (row[0], attempt)


def test_double_claim():
    real = run_claimers(claim, "READ COMMITTED")
    broken = run_claimers(naive_claim, "READ COMMITTED")
    R.test("double claim",
           "16 threads claim 2000 jobs as fast as they can; count jobs handed to 2+ claimers",
           real["claimed_twice"] == 0 and real["jobs"] == 2000, str(real),
           broken["claimed_twice"] > 0, f"SELECT-then-UPDATE in two steps: {broken}")
    return real, broken


def test_isolation():
    real_rr = run_claimers(claim, "REPEATABLE READ")
    naive_ser = run_claimers(naive_claim, "SERIALIZABLE")
    R.test("isolation level",
           "same claim test at stricter levels. Real code at READ COMMITTED: see 'double claim'. "
           "Postgres default is READ COMMITTED; our safety comes from single conditional "
           "statements + row locks, not from the isolation level",
           real_rr["claimed_twice"] == 0, f"real claim at REPEATABLE READ: {real_rr}",
           None, "")
    R.say(f"note:    naive SELECT-then-UPDATE at SERIALIZABLE: {naive_ser}")
    R.say("         -> at READ COMMITTED the naive code double-claims SILENTLY; at SERIALIZABLE the same")
    R.say("            bug turns into LOUD serialization errors. Our real claim is correct at READ COMMITTED,")
    R.say("            but at REPEATABLE READ it throws serialization errors under contention, which the")
    R.say("            worker does not retry (it would crash and be restarted). So: we depend on READ")
    R.say("            COMMITTED for availability, not for correctness.")


# ---------------------------------------------------------------- 2. zombie (deterministic)
def zombie_trials(complete_sql, trials: int = 200) -> dict:
    out = Counter()
    with connect() as c:
        reset(c)
        add_jobs(c, trials)
        for _ in range(trials):
            job, a1 = claim(c, "A")                  # A claims ...
            expire_lease(c, job)                     # ... freezes; its lease runs out
            job2, a2 = claim(c, "B")                 # B reclaims the same job
            assert job2 == job and a2 == a1 + 1
            out["zombie_write_accepted"] += complete(c, job, a1, "A", complete_sql)
            out["fresh_write_accepted"] += complete(c, job, a2, "B", complete_sql)
        out["stale_effects"] = c.execute(
            "SELECT count(*) FROM side_effects s JOIN jobs j ON j.id = s.job_id "
            "WHERE s.attempt <> j.attempt").fetchone()[0]
        out["effects"] = c.execute("SELECT count(*) FROM side_effects").fetchone()[0]
    return dict(out)


def test_zombie():
    real = zombie_trials(queries.COMPLETE)
    broken = zombie_trials(COMPLETE_WITHOUT_ATTEMPT_FENCE)
    R.test("zombie worker (deterministic)",
           "200 times: A claims, A's lease expires, B reclaims, then A (the zombie) tries to "
           "complete before B does",
           real.get("zombie_write_accepted", 0) == 0 and real["stale_effects"] == 0
           and real["effects"] == 200, str(real),
           broken.get("zombie_write_accepted", 0) > 0 and broken.get("stale_effects", 0) > 0,
           f"complete without 'attempt =': {broken} -> the zombie's stale result wins and the "
           "fresh worker's result is thrown away")


def wait_state(job_id, states, timeout=15.0):
    deadline = time.monotonic() + timeout
    with connect() as c:
        while time.monotonic() < deadline:
            s = c.execute("SELECT state FROM jobs WHERE id = %s", (job_id,)).fetchone()[0]
            if s in states:
                return s
            time.sleep(0.1)
    return s


def test_zombie_real_process():
    with connect() as c:
        reset(c)
        job = add_jobs(c, 1)[0]
    events = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl").name
    env = {**os.environ, "DATABASE_URL": DB, "EVENT_LOG": events, "LEASE_SECONDS": "2",
           "HEARTBEAT_SECONDS": "0.5", "FAIL_RATE": "0", "WORK_STEPS": "40"}
    a = subprocess.Popen([sys.executable, "-m", "jobqueue.worker"], stdout=subprocess.DEVNULL,
                         env={**env, "WORK_MIN_MS": "4000", "WORK_MAX_MS": "4000"})
    b = None
    try:
        wait_state(job, {"running"})
        time.sleep(0.3)
        os.kill(a.pid, signal.SIGSTOP)               # freeze A mid-job
        time.sleep(3)                                # longer than the 2 s lease
        b = subprocess.Popen([sys.executable, "-m", "jobqueue.worker"], stdout=subprocess.DEVNULL,
                             env={**env, "WORK_MIN_MS": "300", "WORK_MAX_MS": "300"})
        final = wait_state(job, {"succeeded"})
        os.kill(a.pid, signal.SIGCONT)               # A wakes up, still believing it owns the job
        time.sleep(3)
    finally:
        for p in (a, b):
            if p:
                p.send_signal(signal.SIGCONT)
                p.terminate()
                p.wait(10)
    with connect() as c:
        eff = c.execute("SELECT attempt, worker_id FROM side_effects WHERE job_id = %s", (job,)).fetchall()
        execs = c.execute("SELECT count(*) FROM executions WHERE job_id = %s", (job,)).fetchone()[0]
    ev = [json.loads(line) for line in open(events) if line.strip()]
    a_events = [e["event"] for e in ev if str(e.get("pid", "")) == str(a.pid)
                or f"-{a.pid}-" in str(e.get("worker", ""))]
    a_after = [x for x in a_events if x in ("heartbeat_lost", "lost_job", "completion_rejected", "completed")]
    ok = final == "succeeded" and len(eff) == 1 and eff[0][0] == 2 and "completed" not in a_after
    R.test("zombie worker (real processes, SIGSTOP)",
           "real worker A frozen with SIGSTOP mid-job for 3 s (lease 2 s); worker B takes over "
           "and finishes; A resumed with SIGCONT",
           ok, f"final={final} effects={eff} executions={execs} A_after_resume={a_after}",
           None, "")


# ---------------------------------------------------------------- 3. cancel / complete race
def race_trials(complete_sql, trials: int = 500) -> dict:
    out = Counter()
    with connect() as c:
        reset(c)
        jobs = add_jobs(c, trials)
    start, done = threading.Barrier(3), threading.Barrier(3)
    current = {}

    def canceller():
        with connect() as c:
            for _ in range(trials):
                start.wait()
                current["cancel"] = c.execute(queries.CANCEL, {"job_id": current["job"]}).fetchone() is not None
                done.wait()

    def completer():
        with connect() as c:
            for _ in range(trials):
                start.wait()
                current["complete"] = complete(c, current["job"], current["attempt"], "W", complete_sql)
                done.wait()

    ts = [threading.Thread(target=canceller), threading.Thread(target=completer)]
    for t in ts:
        t.start()
    with connect() as c:
        for job in jobs:
            got = claim(c, "W")
            current.update(job=got[0], attempt=got[1])
            start.wait()                               # release both at the same instant
            done.wait()
            state = c.execute("SELECT state FROM jobs WHERE id = %s", (got[0],)).fetchone()[0]
            effects = c.execute("SELECT count(*) FROM side_effects WHERE job_id = %s", (got[0],)).fetchone()[0]
            if current["cancel"] and not current["complete"] and state == "cancelled" and effects == 0:
                out["cancel_won"] += 1
            elif current["complete"] and not current["cancel"] and state == "succeeded" and effects == 1:
                out["complete_won"] += 1
            else:
                out["INCONSISTENT"] += 1
                if current["cancel"] and state != "cancelled":
                    out["cancel_said_ok_but_job_" + state] += 1
    for t in ts:
        t.join()
    return dict(out)


def test_cancel_race():
    real = race_trials(queries.COMPLETE)
    broken = race_trials(COMPLETE_WITHOUT_STATE_GUARD)
    R.test("cancel/complete race",
           "500 times: a running job; cancel and complete released at the same instant "
           "(thread barrier); check exactly one wins and the final state matches the answers",
           real.get("INCONSISTENT", 0) == 0 and real.get("cancel_won", 0) > 0
           and real.get("complete_won", 0) > 0, str(real),
           broken.get("INCONSISTENT", 0) > 0, f"complete without \"state = 'running'\": {broken}")


# ---------------------------------------------------------------- 4. duplicates on retry
def create_real(c, key):
    row = c.execute(queries.CREATE_JOB, {"key": key, "payload": "{}", "payload_hash": "h",
                                         "max_attempts": 5}).fetchone()
    return row[0] if row else c.execute(queries.GET_BY_KEY, {"key": key}).fetchone()[0]


def create_naive(c, key):
    row = c.execute("SELECT id FROM jobs_no_unique WHERE idempotency_key = %s", (key,)).fetchone()
    if row:
        return row[0]
    return c.execute("INSERT INTO jobs_no_unique (idempotency_key) VALUES (%s) RETURNING id",
                     (key,)).fetchone()[0]


def same_key_rounds(create_fn, table, rounds: int = 100, threads: int = 20) -> dict:
    with connect() as c:
        reset(c)
    barrier = threading.Barrier(threads)
    answers = Counter()                     # key -> distinct ids seen
    seen: dict[str, set] = {}
    lock = threading.Lock()

    def loop():
        with connect() as c:
            for r in range(rounds):
                key = f"dup-{r}"
                barrier.wait()
                job = create_fn(c, key)
                with lock:
                    seen.setdefault(key, set()).add(job)

    ts = [threading.Thread(target=loop) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    with connect() as c:
        rows = dict(c.execute(f"SELECT idempotency_key, count(*) FROM {table} GROUP BY 1").fetchall())
    answers["keys_with_2+_rows"] = sum(1 for v in rows.values() if v > 1)
    answers["keys_where_callers_got_different_ids"] = sum(1 for v in seen.values() if len(v) > 1)
    answers["rows"] = sum(rows.values())
    return dict(answers)


def test_duplicate_concurrent():
    real = same_key_rounds(create_real, "jobs")
    broken = same_key_rounds(create_naive, "jobs_no_unique")
    R.test("duplicate on retry (concurrent)",
           "100 rounds x 20 threads sending the same key at the same instant (the API's create logic)",
           real["keys_with_2+_rows"] == 0 and real["keys_where_callers_got_different_ids"] == 0
           and real["rows"] == 100, str(real),
           broken["keys_with_2+_rows"] > 0, f"SELECT-then-INSERT, no UNIQUE constraint: {broken}")


def test_duplicate_lost_response(trials: int = 50):
    """Over real HTTP: the client sends a POST and hangs up before reading the answer
    (as if the response was lost), then retries. The server committed the first one."""
    try:
        httpx.get(f"{API_URL}/health", timeout=3).raise_for_status()
    except Exception as e:  # noqa: BLE001
        R.say(f"\n## duplicate on retry (lost response)   [SKIPPED: API not reachable: {e!r}]")
        return
    u = urlparse(API_URL)
    out = Counter()
    run = int(time.time())
    for i in range(trials):
        key = f"silent-lost-{run}-{i}"
        body = json.dumps({"idempotency_key": key})
        s = socket.create_connection((u.hostname, u.port or 80))
        s.sendall((f"POST /jobs HTTP/1.1\r\nHost: {u.hostname}\r\nContent-Type: application/json\r\n"
                   f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}").encode())
        s.close()                                   # hang up without reading the response
        time.sleep(0.3)
        r = httpx.post(f"{API_URL}/jobs", json={"idempotency_key": key}, timeout=10)
        out[f"retry_answered_{r.status_code}"] += 1
        listing = httpx.get(f"{API_URL}/jobs/{r.json()['id']}", timeout=10).json()
        out["key_matches"] += listing["idempotency_key"] == key
    R.test("duplicate on retry (lost response, real HTTP)",
           f"{trials} times: send POST over a raw socket and hang up before the answer; retry the "
           "same key. 200 on retry = the first request had committed and was recognised",
           out.get("retry_answered_200", 0) + out.get("retry_answered_201", 0) == trials
           and out["key_matches"] == trials, str(dict(out)), None, "")


# ---------------------------------------------------------------- 5. clock skew
def lease_check(skew_s: float, broken: bool) -> dict:
    with connect() as c:
        reset(c)
        add_jobs(c, 1)
        worker_now = datetime.now(timezone.utc) + timedelta(seconds=skew_s)   # A's wrong clock
        if broken:
            row = c.execute(CLAIM_WITH_WORKER_CLOCK, {"worker_id": "A", "lease_seconds": 5,
                                                      "worker_now": worker_now}).fetchone()
        else:
            row = c.execute(queries.CLAIM, {"worker_id": "A", "lease_seconds": 5}).fetchone()
        lease_left = c.execute("SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs "
                               "WHERE id = %s", (row[0],)).fetchone()[0]
        stolen_at_once = claim(c, "B") is not None    # can B take it while A is still working?
    return {"lease_left_s": round(float(lease_left), 1), "stolen_immediately": stolen_at_once}


def test_clock_skew():
    real = {s: lease_check(s, False) for s in (-10, 0, 3600)}
    broken = {s: lease_check(s, True) for s in (-10, 0, 3600)}
    real_ok = all(4 <= v["lease_left_s"] <= 5.1 and not v["stolen_immediately"] for v in real.values())
    caught = broken[-10]["stolen_immediately"] and broken[3600]["lease_left_s"] > 3000
    R.test("clock skew",
           "worker clock set -10 s / 0 / +1 h. Real code: lease end = database now() + 5 s, so the "
           "worker clock never enters. Control: lease end computed from the worker's clock",
           real_ok, f"by worker skew: {real}", caught,
           f"by worker skew: {broken} -> -10 s: job stolen while A still works (wasted work; the "
           "attempt fence still blocks a double effect); +1 h: a dead worker's job is stuck ~1 h")
    R.say("note:    the worker uses its own clock only for log timestamps and time.monotonic() for")
    R.say("         intervals (heartbeat, backoff), which is immune to wall-clock jumps. Remaining risk:")
    R.say("         the DATABASE clock jumping (e.g. failover to a replica with a different clock).")


# ---------------------------------------------------------------- 6. attempts exhausted
def test_attempts_exhausted():
    out = {}
    with connect() as c:
        reset(c)
        # Job 1: its worker dies on every attempt (lease expires each time).
        j1 = add_jobs(c, 1, max_attempts=2)[0]
        for _ in range(2):
            got = claim(c, "A")
            assert got[0] == j1
            expire_lease(c, j1)
        out["claim_after_last_attempt"] = claim(c, "A")          # must be None
        out["j1_before_reap"] = c.execute("SELECT state FROM jobs WHERE id = %s", (j1,)).fetchone()[0]
        out["reaped"] = [r[0] for r in c.execute(queries.REAP_EXHAUSTED).fetchall()]
        out["j1_after_reap"] = c.execute("SELECT state FROM jobs WHERE id = %s", (j1,)).fetchone()[0]
        # Job 2: the work itself fails on every attempt.
        j2 = add_jobs(c, 1, max_attempts=2)[0]
        states = []
        for _ in range(2):
            got = claim(c, "A")
            assert got[0] == j2
            states.append(c.execute(queries.FAIL, {"job_id": got[0], "attempt": got[1],
                                                   "error": "boom"}).fetchone()[0])
        out["j2_state_after_each_fail"] = states
    ok = (out["claim_after_last_attempt"] is None and out["j1_after_reap"] == "failed"
          and out["reaped"] == [j1] and out["j2_state_after_each_fail"] == ["queued", "failed"])
    caught = out["j1_before_reap"] == "running"
    R.test("attempts exhausted (never happened in the chaos runs)",
           "max_attempts=2. Job 1: worker dies on both attempts. Job 2: work fails on both attempts",
           ok, str(out), caught,
           f"without the reaper, job 1 stays '{out['j1_before_reap']}' forever: no claim will take it")


# ---------------------------------------------------------------- main
def main() -> None:
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    R.say(f"# silent-failure tests  {started}  code={CODE_VERSION}")
    setup_schema()
    for t in (test_double_claim, test_isolation, test_zombie, test_zombie_real_process,
              test_cancel_race, test_duplicate_concurrent, test_duplicate_lost_response,
              test_clock_skew, test_attempts_exhausted):
        try:
            t()
        except Exception as e:  # noqa: BLE001  (a crashing test is a result too)
            R.rows.append((t.__name__, "ERROR"))
            R.say(f"\n## {t.__name__}   [ERROR] {e!r}")
    R.say("\n# summary")
    for name, verdict in R.rows:
        R.say(f"  {verdict:<6} {name}")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"silent-failures-{started}.txt")
    Path(path).write_text("\n".join(R.lines) + "\n")
    print(f"\nsaved {path}")
    raise SystemExit(0 if all(v in ("OK", "WEAK") for _, v in R.rows) else 1)


if __name__ == "__main__":
    main()

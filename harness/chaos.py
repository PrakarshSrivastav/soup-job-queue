"""Chaos harness.  Run with:  docker compose run --rm harness

One run:
  1. reset the tables and the event log
  2. killer: kill -9 a random worker every ~2 s, until all jobs are finished
  3. submit JOBS jobs through the API; ~30% of keys sent 2-4 times (at once or seconds
     later); ~5% first sent with a tiny timeout so the client gives up and retries
  4. cancel in parallel: some at random moments, some aimed at jobs that are running now
  5. wait until nothing is queued/running (or give up and report what is stuck)
  6. check the invariants, write results/run-<time>.txt (+ .json, + events .jsonl.gz)

Exit code 1 if any check fails. RUNS=5 repeats the whole thing 5 times.
"""
import asyncio
import gzip
import json
import os
import random
import shutil
import signal
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import httpx
import psycopg

from jobqueue.version import CODE_VERSION


def env(name, default):
    return type(default)(os.environ.get(name, default))


API_URL = env("API_URL", "http://localhost:8000")
DATABASE_URL = env("DATABASE_URL", "postgresql://queue:queue@localhost:5432/queue")
PID_FILE = env("PID_FILE", "/shared/workers.json")
EVENT_LOG = env("EVENT_LOG", "/shared/events.jsonl")
RESULTS_DIR = env("RESULTS_DIR", "results")

RUNS = env("RUNS", 1)
JOBS = env("JOBS", 10000)
EXPECTED_WORKERS = env("WORKERS", 8)
KILL_EVERY = env("KILL_EVERY", 2.0)           # seconds, +-25% random
KILLS_ENABLED = env("KILLS", 1) == 1          # KILLS=0 for a no-chaos baseline
DUP_FRACTION = env("DUP_FRACTION", 0.30)      # keys sent more than once
TIMEOUT_FRACTION = env("TIMEOUT_FRACTION", 0.05)  # first send gives up almost at once
CANCEL_RANDOM = env("CANCEL_RANDOM", 500)     # cancels at random moments
CANCEL_RUNNING = env("CANCEL_RUNNING", 500)   # cancels aimed at running jobs
CONCURRENCY = env("CONCURRENCY", 50)          # parallel HTTP requests
LEASE_SECONDS = env("LEASE_SECONDS", 5.0)
MAX_RUN_SECONDS = env("MAX_RUN_SECONDS", 3600)
DRAIN_SECONDS = env("DRAIN_SECONDS", 120)     # extra wait after MAX_RUN before "stuck"


class Run:
    def __init__(self, number: int):
        self.number = number
        self.id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.lines: list[str] = []
        self.responses: dict[str, list[tuple[int, int]]] = defaultdict(list)  # key -> [(status, id)]
        self.accepted: dict[str, int] = {}                                       # key -> job id
        self.planned_keys: set[str] = set()
        self.request_errors = Counter()
        self.cancels: list[tuple[int, str, int, str]] = []                     # (id, kind, status, state)
        self.kills: list[tuple[float, int]] = []
        self.kill_misses = 0
        self.submitted = asyncio.Event()
        self.finished = asyncio.Event()
        self.started = time.monotonic()

    def say(self, msg: str) -> None:
        line = f"[{time.monotonic() - self.started:7.1f}s] {msg}"
        print(line, flush=True)
        self.lines.append(line)


# ------------------------------------------------------------------ HTTP side
async def post_job(run: Run, client: httpx.AsyncClient, sem, key: str, n: int,
                   delay: float, tiny_timeout: bool) -> None:
    await asyncio.sleep(delay)
    timeout = 0.001 if tiny_timeout else 30.0
    for attempt in range(10):
        async with sem:
            try:
                r = await client.post("/jobs", json={"idempotency_key": key, "payload": {"n": n}},
                                      timeout=timeout)
            except httpx.TimeoutException:
                run.request_errors["client_timeout_then_retry"] += 1
                timeout = 30.0
                continue
            except httpx.TransportError as e:
                run.request_errors[f"transport:{type(e).__name__}"] += 1
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
        if r.status_code in (200, 201):
            job_id = r.json()["id"]
            run.responses[key].append((r.status_code, job_id))
            run.accepted.setdefault(key, job_id)
            return
        run.request_errors[f"http_{r.status_code}"] += 1
        if r.status_code < 500:
            return  # a 4xx is a real answer; the checks will flag it
        await asyncio.sleep(0.2 * (attempt + 1))
    run.request_errors["gave_up"] += 1


async def submit_all(run: Run, client, sem) -> None:
    tasks = []
    for n in range(JOBS):
        key = f"r{run.id}-{n}"
        run.planned_keys.add(key)
        sends = random.randint(2, 4) if random.random() < DUP_FRACTION else 1
        for s in range(sends):
            if s == 0:
                tasks.append(post_job(run, client, sem, key, n, 0, random.random() < TIMEOUT_FRACTION))
            else:  # duplicate: half fire right away (concurrent), half seconds later (retry)
                delay = 0 if random.random() < 0.5 else random.uniform(1, 10)
                tasks.append(post_job(run, client, sem, key, n, delay, False))
    await asyncio.gather(*tasks)
    run.say(f"submission done: {len(tasks)} POSTs for {JOBS} keys")
    run.submitted.set()


async def cancel(run: Run, client, sem, job_id: int, kind: str) -> None:
    for attempt in range(10):
        async with sem:
            try:
                r = await client.post(f"/jobs/{job_id}/cancel", timeout=30)
            except httpx.TransportError as e:
                run.request_errors[f"cancel_transport:{type(e).__name__}"] += 1
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
        if r.status_code >= 500:
            run.request_errors[f"cancel_http_{r.status_code}"] += 1
            await asyncio.sleep(0.2 * (attempt + 1))
            continue
        state = r.json().get("state", "?") if r.status_code in (200, 409) else "?"
        run.cancels.append((job_id, kind, r.status_code, state))
        return


async def cancel_random(run: Run, client, sem) -> None:
    async def one(key):
        while key not in run.accepted:           # wait until the job exists
            if run.submitted.is_set() and key not in run.accepted:
                return
            await asyncio.sleep(0.5)
        await asyncio.sleep(random.uniform(0, 60))
        await cancel(run, client, sem, run.accepted[key], "random")

    keys = random.sample(sorted(run.planned_keys), min(CANCEL_RANDOM, JOBS))
    await asyncio.gather(*[one(k) for k in keys])


async def cancel_running(run: Run, client, sem) -> None:
    """Aim cancels at jobs that are running right now: the cancel/complete race."""
    left = CANCEL_RUNNING
    pending = []
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True) as db:
        while left > 0 and not run.finished.is_set():
            rows = await (await db.execute(
                "SELECT id FROM jobs WHERE state = 'running' ORDER BY random() LIMIT 2")).fetchall()
            for (job_id,) in rows[:left]:
                left -= 1

                async def fire(j=job_id):
                    await asyncio.sleep(random.uniform(0, 0.3))
                    await cancel(run, client, sem, j, "running")
                pending.append(asyncio.create_task(fire()))
            await asyncio.sleep(0.25)
    await asyncio.gather(*pending)


# ------------------------------------------------------------------ the killer
def read_pid_file() -> dict:
    try:
        with open(PID_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def live_worker_pids() -> list[int]:
    return list(read_pid_file().get("pids", {}).values())


def is_worker(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return b"jobqueue.worker" in f.read()
    except OSError:
        return False


async def killer(run: Run) -> None:
    while not run.finished.is_set():
        await asyncio.sleep(random.uniform(KILL_EVERY * 0.75, KILL_EVERY * 1.25))
        if run.finished.is_set():
            break
        pids = [p for p in live_worker_pids() if is_worker(p)]
        if not pids:
            run.kill_misses += 1
            continue
        pid = random.choice(pids)
        try:
            os.kill(pid, signal.SIGKILL)
            run.kills.append((time.time(), pid))
        except ProcessLookupError:
            run.kill_misses += 1


# ------------------------------------------------------------------ progress + drain
async def counts(db) -> dict[str, int]:
    rows = await (await db.execute("SELECT state, count(*) FROM jobs GROUP BY state")).fetchall()
    return {s: c for s, c in rows}


async def monitor(run: Run) -> None:
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True) as db:
        last_print = 0.0
        while True:
            c = await counts(db)
            remaining = c.get("queued", 0) + c.get("running", 0)
            elapsed = time.monotonic() - run.started
            if elapsed - last_print >= 15:
                run.say(f"progress {dict(sorted(c.items()))} kills={len(run.kills)}")
                last_print = elapsed
            if run.submitted.is_set() and remaining == 0:
                run.say("all jobs finished; killer stops")
                break
            if elapsed > MAX_RUN_SECONDS:
                run.say(f"MAX_RUN_SECONDS reached with {remaining} unfinished; killer stops")
                break
            await asyncio.sleep(1)
        run.finished.set()

        # Drain: after the killer stops, nothing may stay queued/running for long.
        deadline = time.monotonic() + max(DRAIN_SECONDS, LEASE_SECONDS * 3)
        while time.monotonic() < deadline:
            c = await counts(db)
            if c.get("queued", 0) + c.get("running", 0) == 0:
                break
            await asyncio.sleep(1)
        # Stay finished for a full lease: a late zombie write would show up here.
        await asyncio.sleep(LEASE_SECONDS + 1)
        run.say(f"final {dict(sorted((await counts(db)).items()))}")


# ------------------------------------------------------------------ checks
def read_events() -> list[dict]:
    events = []
    try:
        with open(EVENT_LOG) as f:
            for line in f:
                try:
                    events.append(json.loads(line))
                except ValueError:
                    pass  # a line cut off by kill -9
    except OSError:
        pass
    return events


def check(run: Run) -> tuple[list[tuple[bool, str, str]], dict]:
    results = []

    def add(ok: bool, name: str, detail: str):
        results.append((ok, name, detail))

    with psycopg.connect(DATABASE_URL) as db:
        q = lambda sql, *a: db.execute(sql, a).fetchall()  # noqa: E731
        db_jobs = {k: (i, s, a) for i, k, s, a in q("SELECT id, idempotency_key, state, attempt FROM jobs")}
        effects = {j: (a, w) for j, a, w in q("SELECT job_id, attempt, worker_id FROM side_effects")}
        execs = dict(q("SELECT job_id, count(*) FROM executions GROUP BY job_id"))

    # 0. enough evidence: a check that passes on an empty database proves nothing (INCIDENT-001)
    accepted = set(run.accepted)
    enough = (len(accepted) >= 0.99 * JOBS and len(effects) > 0
              and (len(run.kills) > 0 or not KILLS_ENABLED))
    add(enough, "enough evidence for the checks below",
        f"accepted={len(accepted)}/{JOBS} side_effects={len(effects)} kills={len(run.kills)}")

    # 1. no job lost
    missing = accepted - set(db_jobs)
    extra = set(db_jobs) - run.planned_keys
    never_accepted = run.planned_keys - accepted
    add(not missing and not extra and not never_accepted, "no job lost",
        f"accepted={len(accepted)} in_db={len(db_jobs)} missing={len(missing)} "
        f"unknown_extra={len(extra)} never_accepted={len(never_accepted)}")

    # 2. idempotency: every 2xx for a key carried the same id, and it is the DB's id
    split = [k for k, rs in run.responses.items() if len({i for _, i in rs}) > 1]
    wrong = [k for k, i in run.accepted.items() if k in db_jobs and db_jobs[k][0] != i]
    creates = Counter(s for rs in run.responses.values() for s, _ in rs)
    add(not split and not wrong, "duplicate POSTs return the same job",
        f"keys_with_2+_ids={len(split)} id_mismatch_vs_db={len(wrong)} "
        f"responses: 201={creates[201]} 200={creates[200]}")

    # 3. at most one side effect, from the job's final attempt; no crash from the backstop
    by_id = {i: (k, s, a) for k, (i, s, a) in db_jobs.items()}
    stale = [j for j, (a, _) in effects.items() if j in by_id and by_id[j][2] != a]
    events = read_events()
    crashes = [e for e in events if e.get("event") == "worker_exited"
               and e.get("code") not in (-signal.SIGKILL, 0)]
    add(not stale and not crashes, "no job has more than one side effect",
        f"effects={len(effects)} (PK allows max 1) stale_attempt={len(stale)} "
        f"unexpected_worker_crashes={len(crashes)}")

    # 4. none stuck
    stuck = Counter(s for _, s, _ in by_id.values() if s in ("queued", "running"))
    add(not stuck, "none stuck in queued/running", f"{dict(stuck) or 0}")

    # 5. consistent final states
    succ_no_eff = [j for j, (_, s, _) in by_id.items() if s == "succeeded" and j not in effects]
    eff_not_succ = [j for j in effects if by_id.get(j, (0, "?"))[1] != "succeeded"]
    cancel_lies = [(j, c, st) for j, _, c, st in run.cancels
                   if (c == 200 and by_id.get(j, (0, "?"))[1] != "cancelled")
                   or (c == 409 and by_id.get(j, (0, "?"))[1] not in ("succeeded", "failed"))
                   or c not in (200, 409)]
    add(not succ_no_eff and not eff_not_succ and not cancel_lies, "final states consistent",
        f"succeeded_without_effect={len(succ_no_eff)} effect_on_non_succeeded={len(eff_not_succ)} "
        f"cancel_answer_contradicts_final_state={len(cancel_lies)}")

    # 6. kills accounted for (every kill was seen by the supervisor)
    sigkills = [e for e in events if e.get("event") == "worker_exited"
                and e.get("code") == -signal.SIGKILL]
    add(len(sigkills) == len(run.kills), "every kill seen by supervisor",
        f"kills_sent={len(run.kills)} sigkill_exits_seen={len(sigkills)}")

    # stats
    ev = Counter(e.get("event") for e in events)
    beats = sorted(e["ms"] for e in events if e.get("event") == "heartbeat")
    cancelled_ids = {j for j, (_, s, _) in by_id.items() if s == "cancelled"}
    multi = [c for c in execs.values() if c > 1]
    stats = {
        "jobs": len(db_jobs),
        "final_states": dict(Counter(s for _, s, _ in by_id.values())),
        "duration_s": round(time.monotonic() - run.started, 1),
        "kills": len(run.kills), "kill_misses": run.kill_misses,
        "post_requests": sum(len(r) for r in run.responses.values()),
        "request_errors": dict(run.request_errors),
        "cancel_requests": dict(Counter(f"{k}:{c}:{st}" for _, k, c, st in run.cancels)),
        "cancelled_while_running": len(cancelled_ids & set(execs)),
        "jobs_executed_more_than_once": len(multi),
        "max_executions_of_one_job": max(execs.values(), default=0),
        "executions_total": sum(execs.values()),
        "events": {k: ev[k] for k in ("claimed", "completed", "completion_rejected",
                                       "heartbeat_lost", "lost_job", "requeued", "failed",
                                       "reaped", "fail_rejected", "complete_outcome_unknown")},
        "heartbeat_ms": ({"n": len(beats), "p50": statistics.median(beats),
                          "p99": beats[int(len(beats) * 0.99) - 1], "max": beats[-1]} if beats else {}),
    }
    return results, stats


# ------------------------------------------------------------------ one run
def reset() -> None:
    with psycopg.connect(DATABASE_URL, autocommit=True) as db:
        db.execute("TRUNCATE side_effects, executions, jobs RESTART IDENTITY CASCADE")
    open(EVENT_LOG, "w").close()


async def wait_for_workers(run: Run) -> None:
    for _ in range(60):
        if len([p for p in live_worker_pids() if is_worker(p)]) >= EXPECTED_WORKERS:
            return
        await asyncio.sleep(1)
    run.say(f"WARNING: fewer than {EXPECTED_WORKERS} workers visible")


async def preflight(client: httpx.AsyncClient) -> str | None:
    """Refuse to start unless the whole system is healthy and runs the same code.
    Returns None if all good, else the reason. Added after INCIDENT-001."""
    try:
        health = (await client.get("/health", timeout=5)).json()
    except Exception as e:  # noqa: BLE001
        return f"API /health failed: {e!r}"
    if health.get("code") != CODE_VERSION:
        return f"API runs code {health.get('code')}, harness runs {CODE_VERSION}: rebuild images"
    workers_code = read_pid_file().get("code")
    if workers_code != CODE_VERSION:
        return f"workers run code {workers_code}, harness runs {CODE_VERSION}: rebuild images"

    # One probe job must go all the way through, and show up in the event log.
    key = f"preflight-{time.time()}"
    try:
        r = await client.post("/jobs", json={"idempotency_key": key}, timeout=10)
    except Exception as e:  # noqa: BLE001
        return f"probe POST failed: {e!r}"
    if r.status_code != 201:
        return f"probe POST returned {r.status_code}: {r.text[:300]}"
    job_id = r.json()["id"]
    state = "?"
    for _ in range(30):
        state = (await client.get(f"/jobs/{job_id}", timeout=10)).json()["state"]
        if state == "succeeded":
            break
        await asyncio.sleep(0.5)
    if state != "succeeded":
        return f"probe job {job_id} not processed within 15 s (state={state}): are workers running?"
    if not any(e.get("event") == "completed" and e.get("job") == job_id for e in read_events()):
        return "probe job succeeded but the event log has no 'completed' event: is EVENT_LOG shared?"
    return None


async def run_once(number: int) -> bool:
    reset()
    run = Run(number)
    run.say(f"run {number} id={run.id} code={CODE_VERSION} jobs={JOBS} workers={EXPECTED_WORKERS} "
            f"kill_every={KILL_EVERY}s kills={'on' if KILLS_ENABLED else 'OFF'} "
            f"dup={DUP_FRACTION} cancels={CANCEL_RANDOM}+{CANCEL_RUNNING} lease={LEASE_SECONDS}s")
    await wait_for_workers(run)
    sem = asyncio.Semaphore(CONCURRENCY)
    limits = httpx.Limits(max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(base_url=API_URL, limits=limits) as client:
        problem = await preflight(client)
        if problem:
            run.say(f"PREFLIGHT FAILED: {problem}")
            run.say("RESULT: FAIL (run not started)")
            save(run, False, [(False, "preflight", problem)], {})
            return False
        run.say("preflight ok: API, workers and harness run the same code; probe job processed")
        reset()  # remove the probe job
        tasks = [submit_all(run, client, sem), cancel_random(run, client, sem),
                 cancel_running(run, client, sem), monitor(run)]
        if KILLS_ENABLED:
            tasks.append(killer(run))
        await asyncio.gather(*tasks)

    results, stats = check(run)
    ok = all(r[0] for r in results)
    run.say("CHECKS")
    for passed, name, detail in results:
        run.say(f"  {'PASS' if passed else 'FAIL'}  {name}: {detail}")
    run.say("STATS")
    for k, v in stats.items():
        run.say(f"  {k}: {v}")
    run.say(f"RESULT: {'PASS' if ok else 'FAIL'}")
    save(run, ok, results, stats)
    return ok


def save(run: Run, ok: bool, results: list, stats: dict) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    base = os.path.join(RESULTS_DIR, f"run-{run.id}")
    with open(base + ".txt", "w") as f:
        f.write("\n".join(run.lines) + "\n")
    with open(base + ".json", "w") as f:
        json.dump({"run": run.id, "ok": ok, "checks": results, "stats": stats}, f, indent=2, default=str)
    with open(EVENT_LOG, "rb") as src, gzip.open(base + "-events.jsonl.gz", "wb") as dst:
        shutil.copyfileobj(src, dst)
    with open(os.path.join(RESULTS_DIR, "summary.txt"), "a") as f:
        f.write(f"{run.id}  {'PASS' if ok else 'FAIL'}  code={CODE_VERSION} jobs={JOBS} "
                f"kills={stats.get('kills', 0)} duration={stats.get('duration_s', 0)}s  "
                f"failed_checks={[n for p, n, _ in results if not p]}\n")


async def main() -> None:
    oks = []
    for i in range(1, RUNS + 1):
        oks.append(await run_once(i))
    print(f"\n{sum(oks)}/{len(oks)} runs passed", flush=True)
    raise SystemExit(0 if all(oks) else 1)


if __name__ == "__main__":
    asyncio.run(main())

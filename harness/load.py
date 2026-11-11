"""Load test for the SHIP / DON'T SHIP verdict.

Run with:  docker compose run --build --rm harness python -m harness.load

Sends a realistic request mix at a FIXED rate (open loop: request i is due at start + i/rate,
whether or not earlier requests have answered), so a slow server can't hide its slowness by
making the client wait ("coordinated omission"). Latency is measured from when each request
was DUE, not from when it was finally sent.

Steps through several rates (default 100, 300, 1000 req/s) to find where things break first.
Mix: 60% create new job, 10% create with a repeated key, 20% get status, 5% list, 5% cancel.
"""
import asyncio
import os
import random
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import httpx
import psycopg

from jobqueue.version import CODE_VERSION

API_URL = os.environ.get("API_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://queue:queue@localhost:5432/queue")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")
RATES = [int(r) for r in os.environ.get("LOAD_RATES", "100,300,1000").split(",")]
SECONDS = float(os.environ.get("LOAD_SECONDS", 60))
TIMEOUT = float(os.environ.get("LOAD_TIMEOUT", 10))

MIX = [("create", 0.60), ("create_dup", 0.10), ("get", 0.20), ("list", 0.05), ("cancel", 0.05)]

lines: list[str] = []


def say(msg: str = "") -> None:
    print(msg, flush=True)
    lines.append(msg)


def pick() -> str:
    r, acc = random.random(), 0.0
    for kind, share in MIX:
        acc += share
        if r < acc:
            return kind
    return MIX[-1][0]


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


async def db_counts(db) -> dict:
    rows = await (await db.execute("SELECT state, count(*) FROM jobs GROUP BY state")).fetchall()
    return dict(rows)


async def step(client: httpx.AsyncClient, db, rate: int, tag: str) -> dict:
    keys: list[str] = []
    ids: list[int] = []
    lat = defaultdict(list)          # kind -> latency (ms) measured from the due time
    status = Counter()
    late_start = []                  # how far behind schedule requests were sent (client health)
    n = int(rate * SECONDS)
    before = await db_counts(db)
    finished_before = (await (await db.execute(
        "SELECT count(*) FROM jobs WHERE finished_at IS NOT NULL")).fetchone())[0]

    async def one(i: int, due: float):
        await asyncio.sleep(max(0.0, due - time.monotonic()))
        late_start.append((time.monotonic() - due) * 1000)
        kind = pick()
        if kind in ("get", "cancel") and not ids:
            kind = "create"
        if kind == "create_dup" and not keys:
            kind = "create"
        try:
            if kind == "create":
                key = f"load-{tag}-{i}"
                r = await client.post("/jobs", json={"idempotency_key": key}, timeout=TIMEOUT)
                if r.status_code == 201:
                    keys.append(key)
                    ids.append(r.json()["id"])
            elif kind == "create_dup":
                r = await client.post("/jobs", json={"idempotency_key": random.choice(keys)}, timeout=TIMEOUT)
            elif kind == "get":
                r = await client.get(f"/jobs/{random.choice(ids)}", timeout=TIMEOUT)
            elif kind == "list":
                r = await client.get("/jobs", params={"state": "queued", "limit": 50}, timeout=TIMEOUT)
            else:
                r = await client.post(f"/jobs/{random.choice(ids)}/cancel", timeout=TIMEOUT)
            status[f"{kind}:{r.status_code}"] += 1
        except httpx.HTTPError as e:
            status[f"{kind}:{type(e).__name__}"] += 1
        lat[kind].append((time.monotonic() - due) * 1000)

    start = time.monotonic() + 0.5
    await asyncio.gather(*[one(i, start + i / rate) for i in range(n)])
    elapsed = time.monotonic() - start

    after = await db_counts(db)
    finished_after = (await (await db.execute(
        "SELECT count(*) FROM jobs WHERE finished_at IS NOT NULL")).fetchone())[0]
    every = [x for xs in lat.values() for x in xs]
    errors = sum(v for k, v in status.items() if not k.split(":")[1].startswith(("2", "404", "409")))
    return {
        "rate": rate, "requests": n, "seconds": round(elapsed, 1),
        "achieved_rps": round(n / elapsed, 1),
        "errors": errors,
        "p50_ms": round(pct(every, 0.50), 1), "p95_ms": round(pct(every, 0.95), 1),
        "p99_ms": round(pct(every, 0.99), 1), "max_ms": round(max(every), 1),
        "by_kind_p99_ms": {k: round(pct(v, 0.99), 1) for k, v in lat.items()},
        "client_send_delay_p99_ms": round(pct(late_start, 0.99), 1),
        "status": dict(status),
        "jobs_created_per_s": round(len(ids) / elapsed, 1),
        "jobs_finished_per_s": round((finished_after - finished_before) / elapsed, 1),
        "queue_depth_before": before.get("queued", 0), "queue_depth_after": after.get("queued", 0),
    }


async def main() -> None:
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    say(f"# load test {started} code={CODE_VERSION} rates={RATES} seconds_per_rate={SECONDS}")
    say(f"# mix: {MIX}")
    limits = httpx.Limits(max_connections=2000, max_keepalive_connections=500)
    async with httpx.AsyncClient(base_url=API_URL, limits=limits) as client, \
            await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True) as db:
        health = (await client.get("/health", timeout=5)).json()
        say(f"# api health: {health}")
        results = []
        for rate in RATES:
            say(f"\n## {rate} req/s for {SECONDS:.0f} s")
            r = await step(client, db, rate, f"{started}-{rate}")
            results.append(r)
            for k, v in r.items():
                say(f"  {k}: {v}")
            await asyncio.sleep(5)
    say("\n# summary  (latency measured from when each request was due)")
    say(f"  {'rate':>6} {'achieved':>9} {'errors':>7} {'p50':>7} {'p95':>7} {'p99':>8} {'max':>8} "
        f"{'created/s':>10} {'finished/s':>11} {'queue after':>12}")
    for r in results:
        say(f"  {r['rate']:>6} {r['achieved_rps']:>9} {r['errors']:>7} {r['p50_ms']:>7} {r['p95_ms']:>7} "
            f"{r['p99_ms']:>8} {r['max_ms']:>8} {r['jobs_created_per_s']:>10} "
            f"{r['jobs_finished_per_s']:>11} {r['queue_depth_after']:>12}")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"load-{started}.txt")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\nsaved {path}")


if __name__ == "__main__":
    asyncio.run(main())

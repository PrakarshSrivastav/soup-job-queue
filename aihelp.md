# How I used AI on this project

I used Claude (Anthropic) throughout: to understand the task, design the system, write
most of the code, and build the testing framework. I made the decisions, ran every
official test on my own machine in Docker, and reviewed code before it went into the repo.
This file says where the AI helped, which prompts changed the project, and where it was wrong.

## Starting point: my earlier Java project

I had built a similar job engine before in Java / Spring Boot
([JobEngine](https://github.com/PrakarshSrivastav/JobEngine)): Postgres, `FOR UPDATE SKIP LOCKED`
claiming, a lease reaper, heartbeats, fenced complete/fail. So the core ideas weren't new to me.

I gave the AI that repo and asked what would carry over. It reviewed the code and found
three things that **would fail this task's tests**, which became design decisions here:

| In JobEngine | Problem | What I did here |
|---|---|---|
| completion fenced by `worker_id` (one id per node, shared by threads) | a frozen thread's job can be re-claimed by another thread **on the same node**; the zombie's write then passes the check (ABA problem) | fence on an `attempt` counter that goes up on every claim |
| heartbeat used the DB clock, but claim time and the reaper's cutoff used the app clock | clock skew between app and DB breaks leases | only the database clock (`now()`) is ever used |
| heartbeat per node, for all its jobs | a stuck thread keeps its job's lease alive forever | heartbeat per job, with a "lost" flag the worker checks while working (stuck-but-alive is still a known gap) |

Carried over as-is: partial indexes, running the work outside any transaction, and the
idea of a lease reaper.

## The testing framework

All of this was built with the AI. I ran every official result on my machine.

1. **Chaos harness** (`harness/chaos.py`): 10,000 jobs, 8 workers, `kill -9` on a random
   worker every ~2 s, ~30% duplicate POSTs (some at the same instant, some seconds later),
   1,000 cancels (half aimed at jobs that are running at that moment). It checks the PDF's
   invariants **against the database, not the logs**, and saves every run, failed ones included.
2. **Pre-flight check + code fingerprint:** before a run, the harness checks that the API,
   workers and harness all run the same code, and that one probe job goes all the way through.
   Added after my first Docker runs failed (see `results/INCIDENT-001`).
3. **"Enough evidence" check:** a run with no jobs or no kills FAILS instead of passing
   (added after `results/INCIDENT-002`).
4. **Silent-failure tests** (`tests/silent_failures.py`): 9 tests. Each runs the real code
   **and a deliberately broken version** (the real SQL with one safety condition removed),
   so every test proves it can actually catch its bug.
5. **Load test** (`harness/load.py`): a fixed-rate request mix at 100 / 300 / 1000 req/s,
   latency measured from when each request was due.

## Prompts that changed the project

| My prompt (paraphrased) | What it changed |
|---|---|
| "Explain the task in simple English" | Broke the PDF into 4 parts and showed the scoring: the live discussion is 35%, so understanding mattered more than code volume |
| "What's the best architecture?" then "Which solution solves which problem?" | Postgres as the only coordinator; 5 mechanisms (atomic claim, leases, fencing token, effect + state in one transaction, unique key), each mapped to a PDF question |
| "How do we tell slow from dead?" / "How do we choose X (the timeout)?" | Learned you **can't** tell them apart, so the design makes a wrong guess harmless (fencing). The lease is set from a **measured** heartbeat latency, and it's a performance setting, not a correctness one |
| "Workers are like holes and jobs are marbles rolling into them?" | Corrected my mental model: workers *pull* jobs; a job never leaves the table; ownership is a tag with an expiry |
| "If we pick Python, what's the stack? Here's my Java project" | Python + FastAPI + raw SQL (no ORM, so every guarantee is a visible SQL statement), plus the JobEngine review above |
| "Show me the code step by step, I'll paste it" | I typed the first steps myself (schema, settings, claim query, worker loop) to understand them. **After that, I made the rule: show me the code first, I approve, then write files** |
| "The first run failed, what went wrong? Create a file" | Root cause found from logs (files on disk didn't match what was built); incident report; pre-flight check and code fingerprint added |
| "3/3 runs passed, check it" | AI read the stats and found gaps the PASS hid: client timeout-retries barely happened in Docker, and "job used up all attempts" never happened. Both became deterministic tests |
| "Add silent-failure tests" (with broken versions) | 9/9 OK, and the broken versions found a **blind spot in my own harness** (see below) |
| "Run the load test" | Found that claims get slower as finished jobs pile up, and that 8 workers can't keep up with 100 req/s. Basis of the DON'T SHIP verdict |
| "Make ARCHITECTURE.md simpler, something I'd write" | Rewrote the design doc in plain words, which became my outline for the video |

## Where the AI was wrong or unreliable (and what I did)

* **It wrote files into my folder before I asked**, early on. I pushed back and from then on
  reviewed code in chat before anything was written.
* **Its harness passed on an empty database.** In my first Docker runs every POST failed (500),
  yet 4 of the 6 checks said PASS, because "no double effects" is trivially true with zero jobs.
  Fixed with the evidence check and pre-flight check.
* **Its Docker instructions were incomplete.** `docker compose up --build` doesn't rebuild the
  harness (it's an on-demand service), so one run used the old harness, killed nothing, and still
  said PASS (`kills=0`). I noticed the result looked wrong; fixed with `run --build` (INCIDENT-002).
* **Its "stale attempt" check could never fire.** The side-effect row copied the attempt number
  from the job, not from the worker that wrote it, so a zombie write would look normal. Found by
  the broken-version test; fixed by recording the writer's attempt.
* **Two of its own tests had bugs** the first time (a WEAK control and a crashing test). Both
  fixed and re-run.
* **It assumed a 1 ms client timeout would trigger retries.** In Docker it almost never did
  (3–6 times per run vs ~180 in its sandbox). Replaced with a deterministic lost-response test.
* **Its first claim query had a performance flaw**, found only by the load test: 0.45 ms →
  35 ms after 150k finished jobs. A fix was tested (0.41 ms), but I chose not to apply it before
  the deadline, because that would need all tests re-run. It's the first item in the verdict.
* **Its numbers came from its own small sandbox** (2 CPUs). The official results in `results/`
  are from my machine.

## What I verified myself

* Ran every official test on my machine in Docker: the chaos runs, the silent-failure tests and
  the load test. The raw outputs are in `results/`, including the failed runs.
* Read the run outputs and questioned them (for example, a PASS with `kills=0` isn't a pass).
* Made the design decisions: job length 200–1500 ms, freeze tests kept separate from the main
  chaos run, finishing without the claim fix.
* Reviewed the code shown in chat before it went into the repo, from the API onwards.

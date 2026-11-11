# Incident 001: the first two Docker chaos runs failed (every POST returned 500)

**Date:** 2026-11-11, runs started 14:03 and 14:08 (UTC+1)
**Result:** 2 runs FAIL, a 3rd run aborted by hand (Ctrl+C) once the cause was suspected.
**Status:** fixed at about 14:40; three 1,000-job runs then passed (14:39–14:43).

These runs are kept on purpose. The task asks for the raw output of every run,
including failed ones.

## Raw output

| Run id | Result | Failed checks | Kills | POSTs answered `500` |
|---|---|---|---|---|
| `20261111T130310Z` | FAIL | no job lost; every kill seen by supervisor | 160 | 159,192 |
| `20261111T130843Z` | FAIL | no job lost; every kill seen by supervisor | 163 | 160,150 |
| (run 3) | aborted | none (no report written) | none | none |

Files: `run-20261111T130310Z.txt/.json`, `run-20261111T130843Z.txt/.json`,
`failed-run-api.log`, `failed-run-workers.log`, `failed-run-schema.txt`.

## What we saw

* The job table stayed empty for the whole run: every progress line was `progress {}`.
* `accepted=0 ... never_accepted=10000`. Not one of 10,000 keys got a 2xx answer.
  Each POST was retried up to 10 times, giving ~160,000 requests that all ended in `500`.
* `kills_sent=160 sigkill_exits_seen=0`. The harness killed workers, but found no
  record of the supervisor noticing.
* The other four checks reported PASS, which was **meaningless**: with zero jobs, "no
  double side effects" and "nothing stuck" are trivially true.

## Root cause

**1. The API was running code that didn't match.** The API log showed the same error
on every request:

```
File "/app/jobqueue/api.py", line 61, in create_job
    row = conn.execute(queries.CREATE_JOB, {
AttributeError: module 'jobqueue.queries' has no attribute 'CREATE_JOB'
```

`api.py` was the new version, but `queries.py` on disk had been put back to its
older version, which has no API queries. The Docker images were built from that
mixed state.

**2. The workers ran an old `logs.py`.** The new `logs.py` also writes every event to a
shared file that the harness reads. On disk it had likewise been put back to the
older version that only prints to stdout. That is why the harness saw zero events and
reported `sigkill_exits_seen=0`, even though the supervisor's own stdout
(`failed-run-workers.log`) shows it saw every kill:

```
{"event": "worker_exited", "slot": 7, "pid": 815, "code": -9, "killed_by_sigkill": true}
```

**3. How the files got reverted (most likely, not proven).** The updated versions of
both files were written to disk (by the AI assistant, see AI_USAGE) while the files
were open in VS Code. The file timestamps match moments right after each write. The
editor then saved its stale copy over them. We did not reproduce this, so it is the
most likely explanation rather than a confirmed one.

**The database was fine:** `failed-run-schema.txt` shows the full, current schema.

## What we did

1. Captured the API log, worker log and table schema (the files above).
2. Read the API traceback, which pointed at a missing `queries.CREATE_JOB`.
3. Compared every source file on disk against the tested versions: only `queries.py`
   and `logs.py` differed.
4. Restored both files, checked every file matched, closed the editor tabs, rebuilt
   the images (`docker compose up --build -d`).
5. Re-ran with 1,000 jobs, three times: all PASS.

## What we learned

1. **The harness failed loudly, which is what it should do.** The kill-accounting
   check was the useful cross-check: it caught that the workers were running the wrong
   code, even though that problem had nothing to do with kills.
2. **A PASS with no evidence is not a PASS.** Four checks passed against an empty
   database. Fix: a pre-flight step before every run (the API's `/health` answers, one
   probe job is created and processed, the event log receives events) that aborts at
   once. Each check should also need a minimum amount of evidence (e.g. at least one
   accepted job) before it can report PASS.
3. **Fail fast.** Each broken run wasted about 5.5 minutes producing nothing.
   The pre-flight step turns that into an immediate, specific error.
4. **Record which code ran.** Nothing in the report proved which version of each file
   was inside the containers. Fix: print a hash of the source files at the top of
   every run report.
5. **Retrying on `500` multiplies the load.** Our client retried every failing POST
   up to 10 times, turning 16k requests into ~160k against a broken server. Real
   clients should retry `500`s only a few times, with growing delays, or a broken
   server gets overloaded by its own clients (a "retry storm").

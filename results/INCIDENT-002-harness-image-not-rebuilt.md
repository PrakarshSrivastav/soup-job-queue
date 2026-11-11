# Incident 002: a chaos run "passed" with zero kills

**Date:** 2026-11-11, run `20261111T170906Z` (UTC)
**Result:** reported PASS, but it is **not valid evidence**: `kills=0`.
**Status:** fixed; the next run (`20261111T173703Z`, code `e23a5755e095`) passed with 553 kills.

Kept on purpose: the task asks for every run, and this one shows how a PASS can lie.

## What happened

After the pre-flight check and code fingerprint were added (the fixes from INCIDENT-001),
the images were rebuilt with `docker compose up --build -d` and the harness was run with
`docker compose run --rm harness`.

* `docker compose up --build` only rebuilds the services it starts. The harness only runs
  on demand (compose profile `chaos`), so its image was **not** rebuilt and still had the
  old `chaos.py`.
* The workers did get the new code, which writes the PID file in a new format:
  `{"code": ..., "pids": {...}}`.
* The old harness read that file the old way, found no worker PIDs, and so killed nothing.
  Every kill attempt counted as a "miss". The run itself went normally, so all checks passed.
* The old harness had neither the pre-flight check nor the "enough evidence" check, so
  nothing flagged it. The only visible signs were `kills=0` in the summary, no `code=` field
  and no `preflight ok` line.
* The silent-failure tests didn't run at that point either: the old harness image had no
  `tests` folder.

## What we did

* Ran with `docker compose run --build --rm harness` so the harness image is rebuilt every time.
* Re-ran: the silent-failure tests (9/9 OK) and one full chaos run (`20261111T173703Z`: pre-flight
  ok, code `e23a5755e095`, 553 kills, all 7 checks PASS).

## What we learned

1. **A green result without the chaos is worthless,** and here it looked identical to a real one
   except for one number. The new "enough evidence" check (at least 99% of jobs accepted, side
   effects > 0, kills > 0) turns exactly this case into a FAIL; it just wasn't in the stale image.
2. **The checker has to be checked too.** The pre-flight check compares the code fingerprint of
   the API and workers against the harness's own, but a stale harness can't detect that it is
   itself stale. Rebuilding on every run (`run --build`) is the guard for that.
3. Two incidents, same root cause: **the code that ran was not the code we thought.**
   Every report now prints the code fingerprint, so each result can be tied to an exact version.

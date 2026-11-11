"""One worker process.  Run with:  python -m jobqueue.worker

Loop:  claim -> record execution -> fake work (heartbeat in background) -> complete / fail.

The worker keeps nothing important in memory. If it is kill -9'd at any line, the
database still holds the truth and the job's lease simply expires.
"""
import os
import random
import secrets
import signal
import socket
import threading
import time

import psycopg

from . import config, queries
from .logs import log
from .version import CODE_VERSION


class LostLease(Exception):
    """The job is no longer ours: it was cancelled or re-claimed after our lease expired."""


class WorkError(Exception):
    """The (fake) work failed."""


class ShuttingDown(Exception):
    """SIGTERM received while working."""


def connect() -> psycopg.Connection:
    # autocommit: every statement in queries.py is a complete transaction on its own.
    return psycopg.connect(config.DATABASE_URL, autocommit=True)


class Job:
    """What the worker holds for one claimed job. attempt is its proof of ownership."""

    def __init__(self, job_id: int, attempt: int, payload: dict):
        self.id = job_id
        self.attempt = attempt
        self.payload = payload
        self.lost = threading.Event()  # set by the heartbeat thread

    def params(self, **extra) -> dict:
        return {"job_id": self.id, "attempt": self.attempt, **extra}


class Heartbeat(threading.Thread):
    """Renews the lease of the current job every HEARTBEAT_SECONDS.

    Runs on its OWN connection, so slow work or a busy main connection can't delay it.
    Lives as long as the worker; the main thread tells it which job is current.
    """

    def __init__(self, worker_id: str):
        super().__init__(daemon=True, name="heartbeat")
        self.worker_id = worker_id
        self._lock = threading.Lock()
        self._job: Job | None = None
        self._conn: psycopg.Connection | None = None

    def track(self, job: Job | None) -> None:
        with self._lock:
            self._job = job

    def run(self) -> None:
        while True:
            time.sleep(config.HEARTBEAT_SECONDS)
            with self._lock:
                job = self._job
            if job is None or job.lost.is_set():
                continue
            started = time.monotonic()
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = connect()
                rows = self._conn.execute(
                    queries.HEARTBEAT, job.params(lease_seconds=config.LEASE_SECONDS)
                ).rowcount
            except psycopg.Error as e:
                # Can't reach the DB. Don't give up the job here: if the lease really
                # expires, the fenced COMPLETE will be rejected anyway.
                log("heartbeat_error", worker=self.worker_id, job=job.id, error=str(e))
                self._conn = None
                continue
            ms = round((time.monotonic() - started) * 1000, 1)
            with self._lock:
                still_current = self._job is job
            if rows == 0 and still_current:
                job.lost.set()
                log("heartbeat_lost", worker=self.worker_id, job=job.id, attempt=job.attempt, ms=ms)
            elif rows == 1:
                log("heartbeat", worker=self.worker_id, job=job.id, attempt=job.attempt, ms=ms)


class Worker:
    def __init__(self):
        # New id on every start: a restarted process never inherits old claims.
        self.id = f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"
        self.conn = connect()
        self.stopping = threading.Event()
        self.heartbeat = Heartbeat(self.id)
        self._last_reap = 0.0

    # ---------------------------------------------------------------- main loop
    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stopping.set())
        signal.signal(signal.SIGINT, lambda *_: self.stopping.set())
        self.heartbeat.start()
        log("worker_started", worker=self.id, pid=os.getpid(), code=CODE_VERSION)

        poll = config.POLL_MIN_SECONDS
        while not self.stopping.is_set():
            try:
                self.maybe_reap()
                job = self.claim()
                if job is None:
                    # Empty queue: back off (with jitter so workers don't poll in lockstep).
                    time.sleep(poll * random.uniform(0.5, 1.5))
                    poll = min(poll * 2, config.POLL_MAX_SECONDS)
                    continue
                poll = config.POLL_MIN_SECONDS
                self.process(job)
            except psycopg.OperationalError as e:
                # DB unreachable. Whatever job we held will come back via its lease.
                log("db_error", worker=self.id, error=str(e))
                time.sleep(1)
                self.reconnect()

        log("worker_stopped", worker=self.id)

    def process(self, job: Job) -> None:
        self.heartbeat.track(job)
        try:
            self.execute(job)
            self.complete(job)
        except WorkError as e:
            self.fail(job, str(e))
        except LostLease:
            log("lost_job", worker=self.id, job=job.id, attempt=job.attempt)
        except ShuttingDown:
            self.release(job)
        finally:
            self.heartbeat.track(None)

    # ---------------------------------------------------------------- the steps
    def claim(self) -> Job | None:
        row = self.conn.execute(
            queries.CLAIM, {"worker_id": self.id, "lease_seconds": config.LEASE_SECONDS}
        ).fetchone()
        if row is None:
            return None
        job = Job(*row)
        log("claimed", worker=self.id, job=job.id, attempt=job.attempt)
        return job

    def execute(self, job: Job) -> None:
        self.conn.execute(queries.RECORD_EXECUTION, job.params(worker_id=self.id))
        duration = random.uniform(config.WORK_MIN_MS, config.WORK_MAX_MS) / 1000
        for _ in range(config.WORK_STEPS):
            if job.lost.is_set():
                raise LostLease()
            if self.stopping.is_set():
                raise ShuttingDown()
            time.sleep(duration / config.WORK_STEPS)
        if random.random() < config.FAIL_RATE:
            raise WorkError("simulated work failure")

    def complete(self, job: Job) -> None:
        for _ in range(3):
            try:
                row = self.conn.execute(queries.COMPLETE, job.params(worker_id=self.id)).fetchone()
                if row:
                    log("completed", worker=self.id, job=job.id, attempt=job.attempt)
                else:
                    # Fence said no: we're a zombie, or the job was cancelled meanwhile.
                    log("completion_rejected", worker=self.id, job=job.id, attempt=job.attempt)
                return
            except psycopg.OperationalError as e:
                # The connection died mid-COMMIT: did it land or not? Ask the database.
                log("complete_outcome_unknown", worker=self.id, job=job.id, error=str(e))
                time.sleep(0.5)
                self.reconnect()
                if self.conn.execute(queries.CHECK_COMPLETED_BY, job.params()).fetchone():
                    log("completed", worker=self.id, job=job.id, attempt=job.attempt, recovered=True)
                    return
                # Not committed. Retrying is safe: the fence makes COMPLETE idempotent.
        log("complete_gave_up", worker=self.id, job=job.id, attempt=job.attempt)

    def fail(self, job: Job, error: str) -> None:
        row = self.conn.execute(queries.FAIL, job.params(error=error)).fetchone()
        if row:
            log("failed" if row[0] == "failed" else "requeued",
                worker=self.id, job=job.id, attempt=job.attempt, error=error)
        else:
            log("fail_rejected", worker=self.id, job=job.id, attempt=job.attempt)

    def release(self, job: Job) -> None:
        self.conn.execute(queries.RELEASE, job.params())
        log("released", worker=self.id, job=job.id, attempt=job.attempt)

    def maybe_reap(self) -> None:
        now = time.monotonic()
        if now - self._last_reap < config.REAP_EVERY_SECONDS:
            return
        self._last_reap = now
        for (job_id,) in self.conn.execute(queries.REAP_EXHAUSTED).fetchall():
            log("reaped", worker=self.id, job=job_id)

    def reconnect(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
        while not self.stopping.is_set():
            try:
                self.conn = connect()
                return
            except psycopg.OperationalError:
                time.sleep(1)


if __name__ == "__main__":
    Worker().run()

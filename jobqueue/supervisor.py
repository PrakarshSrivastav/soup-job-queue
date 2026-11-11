"""Starts WORKERS worker processes and restarts any that die.

Run with:  python -m jobqueue.supervisor

Workers never talk to each other or to the supervisor; it only keeps N of them alive.
It writes the live PIDs to PID_FILE so the chaos harness can pick one to kill -9.
"""
import json
import os
import signal
import subprocess
import sys
import time

from . import config
from .logs import log
from .version import CODE_VERSION


def spawn(slot: int) -> subprocess.Popen:
    proc = subprocess.Popen([sys.executable, "-m", "jobqueue.worker"],
                            env={**os.environ, "WORKER_SLOT": str(slot)})
    log("worker_spawned", slot=slot, pid=proc.pid)
    return proc


def write_pid_file(children: dict[int, subprocess.Popen]) -> None:
    tmp = config.PID_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"code": CODE_VERSION,
                   "pids": {str(slot): p.pid for slot, p in children.items() if p.poll() is None}}, f)
    os.replace(tmp, config.PID_FILE)  # atomic: the harness never reads a half-written file


def main() -> None:
    stopping = False

    def on_signal(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    children = {slot: spawn(slot) for slot in range(config.WORKERS)}
    restart_at: dict[int, float] = {}
    log("supervisor_started", workers=config.WORKERS)

    while not stopping:
        now = time.monotonic()
        for slot, proc in list(children.items()):
            if slot in restart_at:
                if now >= restart_at[slot]:
                    del restart_at[slot]
                    children[slot] = spawn(slot)
                continue
            code = proc.poll()  # also reaps the dead child (no zombie processes)
            if code is not None:
                log("worker_exited", slot=slot, pid=proc.pid, code=code,
                    killed_by_sigkill=(code == -signal.SIGKILL))
                restart_at[slot] = now + config.RESTART_DELAY_SECONDS
        write_pid_file(children)
        time.sleep(0.1)

    # Graceful stop: ask every worker to release its job, then wait.
    for proc in children.values():
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 10
    for proc in children.values():
        try:
            proc.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            proc.kill()
    log("supervisor_stopped")


if __name__ == "__main__":
    main()

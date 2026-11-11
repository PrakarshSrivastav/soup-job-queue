"""Settings, read from environment variables so docker-compose can change them."""
import os


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://queue:queue@localhost:5432/queue")

# Leases (see ARCHITECTURE.md, "Choosing the lease length").
LEASE_SECONDS = _f("LEASE_SECONDS", 5)          # silent this long => presumed dead
HEARTBEAT_SECONDS = _f("HEARTBEAT_SECONDS", 1)  # how often a worker renews its lease

# Fake work.
WORK_MIN_MS = _f("WORK_MIN_MS", 100)
WORK_MAX_MS = _f("WORK_MAX_MS", 500)
WORK_STEPS = int(_f("WORK_STEPS", 10))          # work is split so a cancel is noticed early
FAIL_RATE = _f("FAIL_RATE", 0.02)               # chance the fake work raises an error

# Polling when the queue is empty.
POLL_MIN_SECONDS = _f("POLL_MIN_SECONDS", 0.05)
POLL_MAX_SECONDS = _f("POLL_MAX_SECONDS", 1.0)

REAP_EVERY_SECONDS = _f("REAP_EVERY_SECONDS", 2)

# API.
API_POOL_SIZE = int(_f("API_POOL_SIZE", 20))    # max DB connections the API holds

# Supervisor.
WORKERS = int(_f("WORKERS", 4))
RESTART_DELAY_SECONDS = _f("RESTART_DELAY_SECONDS", 0.5)
PID_FILE = os.environ.get("PID_FILE", "/tmp/workers.json")

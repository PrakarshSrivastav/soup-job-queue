"""Insert N jobs straight into the database (temporary, until the API exists).

Usage:  python -m scripts.seed_jobs 1000
"""
import sys
import time

import psycopg

from jobqueue import config

n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
run = int(time.time())
with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
    conn.execute(
        """
        INSERT INTO jobs (idempotency_key, payload, payload_hash)
        SELECT 'seed-' || %(run)s || '-' || g, jsonb_build_object('n', g), 'seed'
        FROM generate_series(1, %(n)s) AS g
        """,
        {"run": run, "n": n},
    )
print(f"inserted {n} jobs")

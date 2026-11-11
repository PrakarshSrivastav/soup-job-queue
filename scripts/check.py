"""Quick look at the queue and the core invariants (the full harness comes later).

Usage:  python -m scripts.check
"""
import psycopg

from jobqueue import config

CHECKS = {
    "succeeded jobs WITHOUT a side effect (must be 0)": """
        SELECT count(*) FROM jobs j
        WHERE j.state = 'succeeded'
          AND NOT EXISTS (SELECT 1 FROM side_effects s WHERE s.job_id = j.id)""",
    "side effects on jobs NOT succeeded (must be 0)": """
        SELECT count(*) FROM side_effects s JOIN jobs j ON j.id = s.job_id
        WHERE j.state <> 'succeeded'""",
    "side effect from a stale attempt (must be 0)": """
        SELECT count(*) FROM side_effects s JOIN jobs j ON j.id = s.job_id
        WHERE s.attempt <> j.attempt""",
    "running with expired lease (should be 0 once workers caught up)": """
        SELECT count(*) FROM jobs WHERE state = 'running' AND lease_expires_at < now()""",
    "jobs executed more than once (allowed: at-least-once)": """
        SELECT count(*) FROM (SELECT job_id FROM executions
                              GROUP BY job_id HAVING count(*) > 1) x""",
}

with psycopg.connect(config.DATABASE_URL) as conn:
    print("state counts:")
    for state, count in conn.execute("SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state"):
        print(f"  {state:<10} {count}")
    print("checks:")
    for name, sql in CHECKS.items():
        print(f"  {conn.execute(sql).fetchone()[0]:>6}  {name}")

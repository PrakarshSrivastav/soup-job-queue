"""A short fingerprint of the code in this folder.

Every component reports it (workers in their "worker_started" event, the API in /health),
and the harness refuses to run if they differ: see results/INCIDENT-001.
"""
import hashlib
from pathlib import Path

_here = Path(__file__).parent
CODE_VERSION = hashlib.sha256(
    b"".join(p.read_bytes() for p in sorted(_here.glob("*.py")))
).hexdigest()[:12]

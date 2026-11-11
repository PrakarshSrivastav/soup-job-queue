"""One JSON object per line, on stdout and (if EVENT_LOG is set) appended to a shared file.

The chaos harness reads the shared file as evidence: kills seen by the supervisor,
rejected completions, unexpected worker crashes, heartbeat latency.
"""
import json
import os
import sys
import time

_path = os.environ.get("EVENT_LOG")
# Append mode: every write goes to the current end of the file, even after the
# harness truncates it between runs. Line-buffered: one line = one write.
_file = open(_path, "a", buffering=1) if _path else None


def log(event: str, **fields) -> None:
    line = json.dumps({"ts": round(time.time(), 3), "event": event, **fields}, default=str)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    if _file:
        _file.write(line + "\n")

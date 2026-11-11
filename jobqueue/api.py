"""HTTP API.  Run with:  uvicorn jobqueue.api:app --host 0.0.0.0 --port 8000

POST /jobs               create (idempotent on idempotency_key)
GET  /jobs/{id}          status of one job
GET  /jobs               list, cursor-paginated, optional ?state=
POST /jobs/{id}/cancel   cancel a queued or running job
"""
import hashlib
import json
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from . import config, queries
from .version import CODE_VERSION

State = Literal["queued", "running", "succeeded", "failed", "cancelled"]

pool = ConnectionPool(
    config.DATABASE_URL,
    min_size=2,
    max_size=config.API_POOL_SIZE,
    kwargs={"autocommit": True, "row_factory": dict_row},
    open=False,
)


@asynccontextmanager
async def lifespan(_app):
    pool.open(wait=True)
    yield
    pool.close()


app = FastAPI(title="job queue", lifespan=lifespan)


class CreateJob(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=200)
    payload: dict = Field(default_factory=dict)
    max_attempts: int = Field(default=5, ge=1, le=20)


def request_hash(body: CreateJob) -> str:
    """Fingerprint of everything in the request except the key, so we can tell
    "same request retried" (fine) from "same key, different request" (client bug)."""
    canonical = json.dumps({"payload": body.payload, "max_attempts": body.max_attempts},
                           sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@app.post("/jobs")
def create_job(body: CreateJob):
    h = request_hash(body)
    with pool.connection() as conn:
        row = conn.execute(queries.CREATE_JOB, {
            "key": body.idempotency_key, "payload": Jsonb(body.payload),
            "payload_hash": h, "max_attempts": body.max_attempts,
        }).fetchone()
        if row:
            return JSONResponse(jsonable(row), status_code=201)        # new job

        existing = conn.execute(queries.GET_BY_KEY, {"key": body.idempotency_key}).fetchone()
    if existing["payload_hash"] != h:
        raise HTTPException(409, "idempotency_key already used with a different request")
    del existing["payload_hash"]
    return JSONResponse(jsonable(existing), status_code=200)          # same job as before


@app.get("/jobs/{job_id}")
def get_job(job_id: int):
    with pool.connection() as conn:
        row = conn.execute(queries.GET_JOB, {"job_id": job_id}).fetchone()
    if row is None:
        raise HTTPException(404, "job not found")
    return jsonable(row)


@app.get("/jobs")
def list_jobs(state: State | None = None,
              cursor: int = Query(0, ge=0),
              limit: int = Query(50, ge=1, le=500)):
    with pool.connection() as conn:
        rows = conn.execute(queries.LIST_JOBS,
                            {"state": state, "cursor": cursor, "limit": limit + 1}).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    return {"items": [jsonable(r) for r in rows],
            "next_cursor": rows[-1]["id"] if more else None}


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: int):
    with pool.connection() as conn:
        row = conn.execute(queries.CANCEL, {"job_id": job_id}).fetchone()
        if row:
            return jsonable(row)                                    # we cancelled it
        current = conn.execute(queries.GET_JOB, {"job_id": job_id}).fetchone()
    if current is None:
        raise HTTPException(404, "job not found")
    if current["state"] == "cancelled":
        return jsonable(current)                                    # already cancelled: same answer
    # succeeded or failed: too late, and we say so instead of pretending.
    return JSONResponse(jsonable(current), status_code=409)


@app.get("/health")
def health():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"ok": True, "code": CODE_VERSION}


def jsonable(row: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}

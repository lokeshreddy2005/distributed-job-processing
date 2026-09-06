from __future__ import annotations

import time
import uuid as uuid_mod
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from common.auth import ApiClient
from common.config import settings
from common.db import init_db, session_scope
from common.metrics import (
    HTTP_LATENCY,
    HTTP_REQUESTS,
    JOBS_SUBMITTED,
    QUEUE_DELAYED,
    QUEUE_DEPTH,
    QUEUE_PENDING,
)
from common.models import Job, JobStatus
from common.queue import delayed_count, enqueue, ensure_group, get_redis, pending_count
from common.ratelimit import enforce_rate_limit
from common.schemas import JobDetailOut, JobListOut, JobOut, JobSubmitRequest

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    r = get_redis()
    ensure_group(r, settings.stream_key, settings.consumer_group)
    yield


app = FastAPI(title="Distributed Job Processing API", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - t0
    path = request.url.path
    HTTP_REQUESTS.labels(request.method, path, response.status_code).inc()
    HTTP_LATENCY.labels(request.method, path).observe(duration)
    return response


@app.get("/health")
def health():
    checks = {"db": False, "redis": False}
    try:
        with session_scope() as db:
            db.execute(select(1))
        checks["db"] = True
    except Exception:
        pass
    try:
        get_redis().ping()
        checks["redis"] = True
    except Exception:
        pass
    ok = all(checks.values())
    return Response(
        content='{"status": "%s", "checks": %s}' % ("ok" if ok else "degraded", checks),
        media_type="application/json",
        status_code=200 if ok else 503,
    )


@app.get("/metrics")
def metrics():
    r = get_redis()
    # queue_depth is sourced from Postgres (count of status=queued), not
    # Redis XLEN: XLEN counts every entry ever appended to the stream,
    # acked or not (Redis 5.0.14 has no MINID trimming or XINFO GROUPS
    # `lag` to get a true unread count), so it only ever grows and does not
    # reflect the actual backlog. XPENDING-based queue_pending (in-flight,
    # delivered-but-unacked) IS accurate as-is and is left on Redis.
    with session_scope() as db:
        backlog = db.execute(
            select(func.count()).select_from(Job).where(Job.status == JobStatus.QUEUED)
        ).scalar_one()
    QUEUE_DEPTH.set(backlog)
    QUEUE_PENDING.set(pending_count(r))
    QUEUE_DELAYED.set(delayed_count(r))
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/jobs", response_model=JobOut, status_code=status.HTTP_201_CREATED)
def submit_job(req: JobSubmitRequest, client: ApiClient = Depends(enforce_rate_limit)):
    max_retries = req.max_retries if req.max_retries is not None else settings.default_max_retries

    with session_scope() as db:
        if req.idempotency_key:
            existing = db.execute(
                select(Job).where(
                    Job.client_name == client.client_name,
                    Job.idempotency_key == req.idempotency_key,
                )
            ).scalar_one_or_none()
            if existing is not None:
                return JobOut.model_validate(existing)

        job = Job(
            job_type=req.job_type,
            payload=req.payload,
            max_retries=max_retries,
            client_name=client.client_name,
            idempotency_key=req.idempotency_key,
            status=JobStatus.QUEUED,
        )
        db.add(job)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            existing = db.execute(
                select(Job).where(
                    Job.client_name == client.client_name,
                    Job.idempotency_key == req.idempotency_key,
                )
            ).scalar_one()
            return JobOut.model_validate(existing)

        job_id = str(job.id)
        job_type = job.job_type
        out = JobOut.model_validate(job)

    r = get_redis()
    enqueue(r, job_id, job_type)
    JOBS_SUBMITTED.labels(job_type, client.client_name).inc()
    return out


@app.get("/v1/jobs/{job_id}", response_model=JobDetailOut)
def get_job(job_id: uuid_mod.UUID, client: ApiClient = Depends(enforce_rate_limit)):
    with session_scope() as db:
        job = db.get(Job, job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        if not client.is_admin and job.client_name != client.client_name:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        return JobDetailOut.model_validate(job)


@app.get("/v1/jobs", response_model=JobListOut)
def list_jobs(
    status_filter: str | None = Query(default=None, alias="status"),
    job_type: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    client: ApiClient = Depends(enforce_rate_limit),
):
    with session_scope() as db:
        stmt = select(Job)
        count_stmt = select(func.count()).select_from(Job)
        if not client.is_admin:
            stmt = stmt.where(Job.client_name == client.client_name)
            count_stmt = count_stmt.where(Job.client_name == client.client_name)
        if status_filter:
            stmt = stmt.where(Job.status == status_filter)
            count_stmt = count_stmt.where(Job.status == status_filter)
        if job_type:
            stmt = stmt.where(Job.job_type == job_type)
            count_stmt = count_stmt.where(Job.job_type == job_type)

        total = db.execute(count_stmt).scalar_one()
        rows = db.execute(
            stmt.order_by(Job.created_at.desc()).limit(limit).offset(offset)
        ).scalars().all()
        return JobListOut(
            items=[JobOut.model_validate(j) for j in rows], total=total, limit=limit, offset=offset
        )

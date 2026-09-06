"""Integration test harness. Runs against the *real* portable Redis/Postgres
started by scripts/start_infra.ps1 — not mocks — because the thing under
test (Streams consumer-group crash recovery, Postgres row locking for
idempotency) is exactly the behavior a mock would fake away. Tests use a
separate Postgres database (jobsdb_test) and a separate Redis stream/group
namespace (test:*) so they never touch dev data.
"""
from __future__ import annotations

import os
import uuid

os.environ.setdefault("POSTGRES_DB", "jobsdb_test")
os.environ.setdefault("STREAM_KEY", "test:jobs:stream")
os.environ.setdefault("DLQ_STREAM_KEY", "test:jobs:dlq")
os.environ.setdefault("CONSUMER_GROUP", "test-workers")
os.environ.setdefault("DELAYED_ZSET_KEY", "test:jobs:delayed")
os.environ.setdefault("RECLAIM_IDLE_MS", "200")  # fast reclaim for tests

import pytest  # noqa: E402
from sqlalchemy import text  # noqa: E402

from common.config import settings  # noqa: E402
from common.db import SessionLocal, engine, init_db  # noqa: E402
from common.models import Base, Job, JobStatus  # noqa: E402
from common.queue import ensure_group, get_redis  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _init_schema():
    init_db()
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(autouse=True)
def _clean_state():
    """Truncate tables and flush test-namespaced Redis keys between tests so
    they can't leak state into each other."""
    yield
    with SessionLocal() as db:
        db.execute(text("TRUNCATE side_effects, job_state_transitions, jobs RESTART IDENTITY CASCADE"))
        db.commit()
    r = get_redis()
    for pattern in ("test:*", "ratelimit:*"):
        for key in r.keys(pattern):
            r.delete(key)
    ensure_group(r, settings.stream_key, settings.consumer_group)


@pytest.fixture
def db_session():
    with SessionLocal() as db:
        yield db


@pytest.fixture
def redis_client():
    return get_redis()


def make_job(db, job_type="prime_calc", payload=None, max_retries=4, client_name="test-client") -> Job:
    job = Job(
        id=uuid.uuid4(),
        job_type=job_type,
        payload=payload or {"limit": 200},
        max_retries=max_retries,
        client_name=client_name,
        status=JobStatus.QUEUED,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job

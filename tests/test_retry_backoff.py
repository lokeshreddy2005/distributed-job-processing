"""Exponential backoff: a failing job is retried, not immediately, and the
delay grows with each attempt (capped)."""
from __future__ import annotations

import time

from common.models import JobStateTransition, JobStatus
from common.queue import delayed_count, enqueue, read_new
from tests.conftest import make_job
from worker.main import _requeue_due_delayed, backoff_seconds, process_message


def test_backoff_seconds_grows_and_is_capped():
    d1 = backoff_seconds(1)
    d2 = backoff_seconds(2)
    d3 = backoff_seconds(3)
    d_high = backoff_seconds(20)
    assert d1 < d2 < d3
    assert d_high <= 60 * 1.2 + 0.01  # retry_max_delay_seconds + jitter headroom


def test_failed_job_is_scheduled_for_retry_then_recovers(db_session, redis_client, monkeypatch):
    calls = {"n": 0}

    def flaky_handler(job_id, payload, attempt=1):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated transient failure")
        return {"ok": True}, f"/tmp/{job_id}", "deadbeef"

    import worker.handlers as handlers_mod
    monkeypatch.setitem(handlers_mod.HANDLERS, "prime_calc", flaky_handler)
    import worker.main as worker_main
    monkeypatch.setitem(worker_main.HANDLERS, "prime_calc", flaky_handler)

    job = make_job(db_session, job_type="prime_calc", max_retries=3)
    enqueue(redis_client, str(job.id), job.job_type)

    msg_id, fields = read_new(redis_client, "worker-a", count=1, block_ms=2000)[0]
    process_message(redis_client, msg_id, fields, "worker-a", reclaimed=False)

    db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.attempts == 1
    assert delayed_count(redis_client) == 1

    # force the backoff window to have elapsed and re-enqueue, using the
    # exact same helper the worker loop calls periodically
    real_time = time.time
    monkeypatch.setattr(worker_main.time, "time", lambda: real_time() + 3600)
    _requeue_due_delayed(redis_client)

    db_session.refresh(job)
    assert job.status == JobStatus.QUEUED

    msg_id2, fields2 = read_new(redis_client, "worker-a", count=1, block_ms=2000)[0]
    process_message(redis_client, msg_id2, fields2, "worker-a", reclaimed=False)

    db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED
    assert job.attempts == 1  # only the failure incremented attempts; the success didn't
    assert calls["n"] == 2

    transitions = (
        db_session.query(JobStateTransition).filter_by(job_id=job.id).order_by(JobStateTransition.timestamp).all()
    )
    path = [(t.from_status, t.to_status) for t in transitions]
    assert path == [
        ("queued", "processing"),
        ("processing", "retrying"),
        ("retrying", "queued"),
        ("queued", "processing"),
        ("processing", "succeeded"),
    ]

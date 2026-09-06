"""Dead-letter transition: a job that exhausts its retry budget must land in
DEAD_LETTERED (not loop forever) and be recorded on the DLQ stream for
inspection/replay."""
from __future__ import annotations

from common.config import settings
from common.models import JobStateTransition, JobStatus
from common.queue import enqueue, read_new
from tests.conftest import make_job
from worker.main import process_message


def test_job_exhausting_retries_is_dead_lettered(db_session, redis_client, monkeypatch):
    def always_fails(job_id, payload, attempt=1):
        raise RuntimeError("permanent failure for DLQ test")

    import worker.main as worker_main
    monkeypatch.setitem(worker_main.HANDLERS, "prime_calc", always_fails)
    # collapse backoff to keep the test fast
    monkeypatch.setattr(worker_main, "backoff_seconds", lambda attempt: 0)

    job = make_job(db_session, job_type="prime_calc", max_retries=2)
    enqueue(redis_client, str(job.id), job.job_type)

    for _ in range(job.max_retries + 1):
        msg_id, fields = read_new(redis_client, "worker-a", count=1, block_ms=2000)[0]
        process_message(redis_client, msg_id, fields, "worker-a", reclaimed=False)
        db_session.refresh(job)
        if job.status == JobStatus.DEAD_LETTERED:
            break
        assert job.status == JobStatus.RETRYING
        # requeue immediately (backoff collapsed to 0) instead of waiting on the delayed zset
        enqueue(redis_client, str(job.id), job.job_type)

    db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTERED
    assert job.attempts == job.max_retries + 1
    assert "permanent failure" in job.error

    transitions = (
        db_session.query(JobStateTransition).filter_by(job_id=job.id).order_by(JobStateTransition.timestamp).all()
    )
    assert transitions[-1].to_status == "dead_lettered"
    assert "exhausted retries" in transitions[-1].note

    dlq_entries = redis_client.xrange(settings.dlq_stream_key)
    matching = [e for e in dlq_entries if e[1]["job_id"] == str(job.id)]
    assert len(matching) == 1
    assert "permanent failure" in matching[0][1]["reason"]

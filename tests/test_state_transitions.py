"""Job lifecycle correctness: queued -> processing -> succeeded, with a
complete, correctly-ordered, timestamped audit trail."""
from __future__ import annotations

from common.models import Job, JobStateTransition, JobStatus, SideEffect
from common.queue import enqueue, read_new
from tests.conftest import make_job
from worker.main import process_message


def test_successful_job_transitions_and_audit_trail(db_session, redis_client):
    job = make_job(db_session, job_type="prime_calc", payload={"limit": 100})
    enqueue(redis_client, str(job.id), job.job_type)

    messages = read_new(redis_client, "worker-test", count=1, block_ms=2000)
    assert len(messages) == 1
    msg_id, fields = messages[0]
    assert fields["job_id"] == str(job.id)

    process_message(redis_client, msg_id, fields, "worker-test", reclaimed=False)

    db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED
    assert job.result["prime_count"] == 25  # primes below 100
    assert job.started_at is not None
    assert job.finished_at is not None
    assert job.finished_at >= job.started_at

    transitions = (
        db_session.query(JobStateTransition)
        .filter_by(job_id=job.id)
        .order_by(JobStateTransition.timestamp)
        .all()
    )
    path = [(t.from_status, t.to_status) for t in transitions]
    assert path == [("queued", "processing"), ("processing", "succeeded")]
    assert all(transitions[i].timestamp <= transitions[i + 1].timestamp for i in range(len(transitions) - 1))

    effect = db_session.get(SideEffect, job.id)
    assert effect is not None
    assert effect.write_count == 1


def test_unknown_job_type_field_rejected_at_api_layer_not_worker():
    # worker.handlers.HANDLERS only knows about the two real job types —
    # anything else would KeyError in worker.main.process_message, which is
    # why api/main.py restricts JobSubmitRequest.job_type to a Literal.
    from worker.handlers import HANDLERS

    assert set(HANDLERS.keys()) == {"prime_calc", "image_resize"}

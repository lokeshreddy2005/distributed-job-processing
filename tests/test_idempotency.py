"""Idempotency under simulated worker death: a job that crashes mid-process
and gets redelivered must not run its side effect twice or corrupt the
result, whether the crash happens (a) after it already succeeded and a
duplicate message shows up, or (b) while it's genuinely still `processing`
and gets reclaimed via XAUTOCLAIM."""
from __future__ import annotations

import pytest

from common.models import JobStatus, SideEffect
from common.queue import enqueue, read_new
from tests.conftest import make_job
from worker.main import _insert_side_effect, process_message


def test_duplicate_delivery_of_already_succeeded_job_is_a_noop(db_session, redis_client, monkeypatch):
    calls = {"n": 0}

    def counting_handler(job_id, payload, attempt=1):
        calls["n"] += 1
        return {"n": calls["n"]}, f"/tmp/{job_id}", "fixed-hash"

    import worker.main as worker_main
    monkeypatch.setitem(worker_main.HANDLERS, "prime_calc", counting_handler)

    job = make_job(db_session, job_type="prime_calc")
    enqueue(redis_client, str(job.id), job.job_type)
    msg_id, fields = read_new(redis_client, "worker-a", count=1, block_ms=2000)[0]
    process_message(redis_client, msg_id, fields, "worker-a", reclaimed=False)

    db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED
    assert calls["n"] == 1

    # Simulate the message being redelivered (e.g. a slow ACK race, or the
    # stream being replayed) — worker.main must detect the job is already
    # terminal and refuse to re-run the handler.
    process_message(redis_client, "9999999999999-0", fields, "worker-b", reclaimed=True)

    assert calls["n"] == 1, "handler must not run twice for an already-succeeded job"
    effect = db_session.get(SideEffect, job.id)
    assert effect.write_count == 1


def test_crash_mid_processing_then_reclaimed_reprocesses_exactly_once(db_session, redis_client, monkeypatch):
    """This is the DB-level analogue of what happens in the real
    failure-injection experiment: a worker claims the job (status=processing)
    and then dies (kill -9) before finishing. Another worker's XAUTOCLAIM
    picks the message back up; process_message(reclaimed=True) must bump
    attempts, redo the work, and still only produce one side-effect row."""
    calls = {"n": 0}

    def counting_handler(job_id, payload, attempt=1):
        calls["n"] += 1
        return {"n": calls["n"]}, f"/tmp/{job_id}", "stable-hash"

    import worker.main as worker_main
    monkeypatch.setitem(worker_main.HANDLERS, "prime_calc", counting_handler)

    job = make_job(db_session, job_type="prime_calc", max_retries=3)
    enqueue(redis_client, str(job.id), job.job_type)
    msg_id, fields = read_new(redis_client, "worker-crashed", count=1, block_ms=2000)[0]

    # Manually put the job into the state a real crash would leave it in:
    # claimed and marked processing, but never ack'd (message stays in PEL).
    job.status = JobStatus.PROCESSING
    db_session.add(job)
    db_session.commit()

    # A live worker reclaims the abandoned PEL entry.
    process_message(redis_client, msg_id, fields, "worker-rescuer", reclaimed=True)

    db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED
    assert job.attempts == 1, "the crash-triggered reprocess counts as one retry attempt"
    assert calls["n"] == 1, "handler ran exactly once despite the crash+reclaim"

    effect = db_session.get(SideEffect, job.id)
    assert effect is not None
    assert effect.write_count == 1


def test_side_effect_ledger_rejects_corrupted_reprocess(db_session):
    """Safety net: if a handler ever produced a *different* result on a
    reprocess (e.g. non-determinism, a bug), the idempotency ledger must
    raise instead of silently accepting a corrupted duplicate."""
    job = make_job(db_session, job_type="prime_calc")
    first = _insert_side_effect(db_session, job.id, "/tmp/x", "hash-A")
    db_session.commit()
    assert first is True

    with pytest.raises(RuntimeError, match="idempotency violation"):
        _insert_side_effect(db_session, job.id, "/tmp/x", "hash-B-DIFFERENT")

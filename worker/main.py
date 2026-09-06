"""Worker process. Run several of these (as separate OS processes — this is
the process-based worker pool substituting for Docker containers) pointed at
the same Redis stream + consumer group.

Lifecycle per job: queued -> processing -> succeeded | retrying -> ... ->
dead_lettered. Postgres is updated inside the same transaction as each state
transition so the transitions table is an exact audit log with timestamps.

Crash recovery: if this process is kill -9'd while `status=processing`, the
Streams message it was handling is never XACK'd, so it stays in the consumer
group's Pending Entries List. Every worker's main loop periodically scans
XPENDING for entries idle longer than RECLAIM_IDLE_MS and XCLAIMs +
reprocesses them — that's the whole recovery mechanism, no custom heartbeat
protocol needed.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
import uuid as uuid_mod

from prometheus_client import start_http_server
from sqlalchemy import select

from common.config import settings
from common.db import session_scope
from common.metrics import (
    JOB_PROCESSING_SECONDS,
    JOBS_PROCESSED,
    JOBS_RECLAIMED,
    JOBS_RETRIED,
    WORKER_UP,
)
from common.models import Job, JobStateTransition, JobStatus, SideEffect, utcnow
from common.queue import (
    ack,
    claim_stale,
    dlq_add,
    ensure_group,
    get_redis,
    pop_due_delayed,
    read_new,
    schedule_retry,
)
from worker.handlers import HANDLERS, JobFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

_running = True


def _handle_sigterm(signum, frame):
    global _running
    log.info("received signal %s, shutting down after current iteration", signum)
    _running = False


def backoff_seconds(attempt: int) -> float:
    delay = settings.retry_base_delay_seconds * (2 ** (attempt - 1))
    delay = min(delay, settings.retry_max_delay_seconds)
    jitter = delay * 0.2 * (os.urandom(1)[0] / 255.0)
    return delay + jitter


def _insert_side_effect(db, job_id: uuid_mod.UUID, effect_key: str, result_hash: str) -> bool:
    """Returns True if this call actually created the row (first time this job
    produced its side effect); False if a row already existed (duplicate
    reprocess after crash/redelivery — idempotent no-op)."""
    existing = db.get(SideEffect, job_id)
    if existing is not None:
        existing.write_count += 1
        if existing.result_hash != result_hash:
            raise RuntimeError(
                f"idempotency violation: job {job_id} produced a different result "
                f"on reprocess ({existing.result_hash} != {result_hash})"
            )
        return False
    db.add(SideEffect(job_id=job_id, effect_key=effect_key, result_hash=result_hash))
    return True


def _claim_for_processing(db, job_id: str, consumer_name: str, reclaimed: bool):
    """Locks the job row and transitions it to PROCESSING, or returns a
    sentinel if the job is already terminal (duplicate delivery) or has been
    sent to the DLQ because it exhausted retries while its previous attempt
    was in flight. Returns (job_or_None, should_run_handler)."""
    job = db.execute(
        select(Job).where(Job.id == uuid_mod.UUID(job_id)).with_for_update()
    ).scalar_one_or_none()
    if job is None:
        return None, False
    if job.status in (JobStatus.SUCCEEDED, JobStatus.DEAD_LETTERED):
        return job, False  # duplicate delivery of an already-terminal job — no-op

    prev_status = job.status
    note = None
    if reclaimed and job.status == JobStatus.PROCESSING:
        job.attempts += 1
        JOBS_RECLAIMED.labels(job.job_type).inc()
        if job.attempts > job.max_retries:
            job.status = JobStatus.DEAD_LETTERED
            job.error = "worker crashed mid-job and retry budget was already exhausted"
            job.finished_at = utcnow()
            db.add(
                JobStateTransition(
                    job_id=job.id,
                    from_status=prev_status.value,
                    to_status=JobStatus.DEAD_LETTERED.value,
                    worker_id=consumer_name,
                    note=f"reclaimed after presumed crash, attempt {job.attempts} > max_retries {job.max_retries}",
                )
            )
            return job, False
        note = f"reclaimed after presumed crash of previous worker (retry attempt {job.attempts})"
    elif reclaimed:
        note = "reclaimed stale PEL entry (job was not actually in-flight, resuming)"

    job.status = JobStatus.PROCESSING
    job.started_at = utcnow()
    db.add(
        JobStateTransition(
            job_id=job.id,
            from_status=prev_status.value,
            to_status=JobStatus.PROCESSING.value,
            worker_id=consumer_name,
            note=note,
        )
    )
    return job, True


def process_message(r, msg_id: str, fields: dict, consumer_name: str, reclaimed: bool) -> None:
    job_id = fields["job_id"]
    job_type = fields["job_type"]

    with session_scope() as db:
        job, should_run = _claim_for_processing(db, job_id, consumer_name, reclaimed)
        if job is None:
            log.warning("job %s not found in DB, acking orphan message", job_id)
            ack(r, msg_id)
            return
        if not should_run:
            log.info("job %s already terminal (%s), acking duplicate delivery", job_id, job.status.value)
            ack(r, msg_id)
            return
        max_retries = job.max_retries
        attempts = job.attempts
        payload = dict(job.payload)

    handler = HANDLERS[job_type]
    this_attempt = attempts + 1
    log.info("[%s] processing job %s (%s) attempt=%d", consumer_name, job_id, job_type, this_attempt)
    t0 = time.perf_counter()
    try:
        result, effect_key, result_hash = handler(job_id, payload, attempt=this_attempt)
        duration = time.perf_counter() - t0
        JOB_PROCESSING_SECONDS.labels(job_type).observe(duration)

        with session_scope() as db:
            job = db.get(Job, uuid_mod.UUID(job_id))
            first_write = _insert_side_effect(db, job.id, effect_key, result_hash)
            job.status = JobStatus.SUCCEEDED
            job.result = result
            job.finished_at = utcnow()
            db.add(
                JobStateTransition(
                    job_id=job.id,
                    from_status=JobStatus.PROCESSING.value,
                    to_status=JobStatus.SUCCEEDED.value,
                    worker_id=consumer_name,
                    note="side effect written" if first_write else "duplicate reprocess — idempotent, no new side effect (result_hash matched)",
                )
            )
        ack(r, msg_id)
        JOBS_PROCESSED.labels(job_type, "succeeded").inc()
        log.info("[%s] job %s succeeded in %.3fs", consumer_name, job_id, duration)

    except Exception as e:  # noqa: BLE001 — handler failures are data, not bugs
        with session_scope() as db:
            job = db.execute(
                select(Job).where(Job.id == uuid_mod.UUID(job_id)).with_for_update()
            ).scalar_one()
            job.attempts += 1
            job.error = str(e)
            if job.attempts > max_retries:
                job.status = JobStatus.DEAD_LETTERED
                job.finished_at = utcnow()
                db.add(
                    JobStateTransition(
                        job_id=job.id,
                        from_status=JobStatus.PROCESSING.value,
                        to_status=JobStatus.DEAD_LETTERED.value,
                        worker_id=consumer_name,
                        note=f"exhausted retries ({job.attempts}/{max_retries}): {e}",
                    )
                )
                dlq_add(r, job_id, job_type, str(e))
                JOBS_PROCESSED.labels(job_type, "dead_lettered").inc()
                log.warning("[%s] job %s DEAD-LETTERED after %d attempts: %s", consumer_name, job_id, job.attempts, e)
            else:
                delay = backoff_seconds(job.attempts)
                job.status = JobStatus.RETRYING
                job.next_retry_at = utcnow()
                db.add(
                    JobStateTransition(
                        job_id=job.id,
                        from_status=JobStatus.PROCESSING.value,
                        to_status=JobStatus.RETRYING.value,
                        worker_id=consumer_name,
                        note=f"attempt {job.attempts}/{max_retries} failed: {e}. backing off {delay:.1f}s",
                    )
                )
                schedule_retry(r, job_id, time.time() + delay)
                JOBS_RETRIED.labels(job_type, "handler_error").inc()
                log.info("[%s] job %s scheduled for retry in %.1fs (attempt %d/%d)", consumer_name, job_id, delay, job.attempts, max_retries)
        ack(r, msg_id)


def _requeue_due_delayed(r) -> None:
    due = pop_due_delayed(r, time.time())
    if not due:
        return
    with session_scope() as db:
        for job_id in due:
            job = db.get(Job, uuid_mod.UUID(job_id))
            if job is None or job.status != JobStatus.RETRYING:
                continue
            job.status = JobStatus.QUEUED
            db.add(
                JobStateTransition(
                    job_id=job.id,
                    from_status=JobStatus.RETRYING.value,
                    to_status=JobStatus.QUEUED.value,
                    worker_id=None,
                    note="backoff elapsed, re-enqueued",
                )
            )
            from common.queue import enqueue
            enqueue(r, str(job.id), job.job_type)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--metrics-port", type=int, default=settings.worker_metrics_base_port)
    args = parser.parse_args()

    worker_id = args.worker_id or f"pid{os.getpid()}"
    consumer_name = f"worker-{worker_id}"

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    start_http_server(args.metrics_port)
    log.info("worker %s: metrics on :%d, pid=%d", consumer_name, args.metrics_port, os.getpid())

    # Record our own OS pid regardless of how we were launched (start_all.ps1
    # already tracks this via Start-Process, but the failure-injection
    # experiment needs a launch-method-agnostic way to find "the real PID of
    # worker N" to kill -9 it).
    from common.config import REPO_ROOT
    run_dir = REPO_ROOT / "run"
    run_dir.mkdir(exist_ok=True)
    (run_dir / f"worker-{worker_id}.pid").write_text(str(os.getpid()))

    r = get_redis()
    ensure_group(r, settings.stream_key, settings.consumer_group)
    WORKER_UP.labels(worker_id).set(1)

    last_reclaim = 0.0
    last_delay_check = 0.0

    while _running:
        now = time.time()
        try:
            if now - last_delay_check > settings.delayed_poll_seconds:
                _requeue_due_delayed(r)
                last_delay_check = now

            if now - last_reclaim > settings.reclaim_poll_seconds:
                stale = claim_stale(r, consumer_name, settings.reclaim_idle_ms)
                for msg_id, fields in stale:
                    log.warning("[%s] reclaiming stale message %s (job %s) — presumed dead worker", consumer_name, msg_id, fields.get("job_id"))
                    process_message(r, msg_id, fields, consumer_name, reclaimed=True)
                last_reclaim = now

            messages = read_new(r, consumer_name, count=1, block_ms=1500)
            for msg_id, fields in messages:
                process_message(r, msg_id, fields, consumer_name, reclaimed=False)
        except Exception:
            log.exception("[%s] error in main loop, continuing", consumer_name)
            time.sleep(1)

    WORKER_UP.labels(worker_id).set(0)
    log.info("[%s] shut down cleanly", consumer_name)


if __name__ == "__main__":
    main()

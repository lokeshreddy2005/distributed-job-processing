"""Redis Streams queue transport.

Why Streams over plain lists/pubsub or Celery: consumer groups give us
per-message delivery tracking (the Pending Entries List) for free, and
XPENDING+XCLAIM (see claim_stale below) are exactly the primitives the
failure-injection experiment needs — when a worker is kill -9'd mid-job, its
in-flight message just sits in the PEL until another consumer reclaims it
after an idle timeout. That maps directly onto "detect a dead worker and
recover its job" without inventing a custom heartbeat protocol. Delivery is
at-least-once by construction (a message is only removed from the PEL by an
explicit XACK after the handler completes), so exactly-once semantics are
pushed up to the idempotency layer in common/models.py:SideEffect, where
they belong.
"""
from __future__ import annotations

import time
from typing import Any

import redis

from common.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        # protocol=2 (RESP2): the portable Redis 5.0.14 binary used in this
        # environment predates RESP3/HELLO, which redis-py 5+ speaks by default.
        _client = redis.Redis(
            host=settings.redis_host, port=settings.redis_port, decode_responses=True, protocol=2
        )
    return _client


def ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    try:
        r.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def enqueue(r: redis.Redis, job_id: str, job_type: str) -> str:
    return r.xadd(settings.stream_key, {"job_id": job_id, "job_type": job_type})


def read_new(r: redis.Redis, consumer: str, count: int = 1, block_ms: int = 2000):
    resp = r.xreadgroup(
        groupname=settings.consumer_group,
        consumername=consumer,
        streams={settings.stream_key: ">"},
        count=count,
        block=block_ms,
    )
    return _flatten(resp)


def claim_stale(r: redis.Redis, consumer: str, min_idle_ms: int, count: int = 10):
    """Steals PEL entries idle longer than min_idle_ms — this is how a live
    worker recovers jobs abandoned by a worker that was killed.

    Uses XPENDING (extended form) + XCLAIM rather than XAUTOCLAIM: the
    portable Redis 5.0.14 build used in this environment predates both
    XAUTOCLAIM and XPENDING's IDLE filter (added in Redis 6.2), so idle time
    is filtered client-side from each pending entry's time_since_delivered.
    XPENDING/XCLAIM themselves have been available since Streams shipped in
    5.0, and this is exactly what XAUTOCLAIM does internally anyway."""
    entries = r.xpending_range(
        name=settings.stream_key,
        groupname=settings.consumer_group,
        min="-",
        max="+",
        count=count,
    )
    stale = [e for e in entries if e["time_since_delivered"] >= min_idle_ms]
    if not stale:
        return []
    message_ids = [e["message_id"] for e in stale]
    claimed = r.xclaim(
        name=settings.stream_key,
        groupname=settings.consumer_group,
        consumername=consumer,
        min_idle_time=min_idle_ms,
        message_ids=message_ids,
    )
    return [(mid, fields) for mid, fields in claimed]


def ack(r: redis.Redis, msg_id: str) -> None:
    r.xack(settings.stream_key, settings.consumer_group, msg_id)


def dlq_add(r: redis.Redis, job_id: str, job_type: str, reason: str) -> None:
    r.xadd(
        settings.dlq_stream_key,
        {"job_id": job_id, "job_type": job_type, "reason": reason, "ts": time.time()},
    )


def schedule_retry(r: redis.Redis, job_id: str, ready_at_epoch: float) -> None:
    r.zadd(settings.delayed_zset_key, {job_id: ready_at_epoch})


def pop_due_delayed(r: redis.Redis, now_epoch: float, limit: int = 50) -> list[str]:
    due = r.zrangebyscore(settings.delayed_zset_key, min="-inf", max=now_epoch, start=0, num=limit)
    if due:
        r.zrem(settings.delayed_zset_key, *due)
    return due


def pending_count(r: redis.Redis) -> int:
    try:
        summary = r.xpending(settings.stream_key, settings.consumer_group)
        return summary["pending"] if summary else 0
    except redis.ResponseError:
        return 0


def delayed_count(r: redis.Redis) -> int:
    return r.zcard(settings.delayed_zset_key)


def _flatten(resp: list[Any]):
    out = []
    for _stream_name, messages in resp or []:
        out.extend(messages)
    return out

"""Fixed-window per-client rate limiting backed by Redis.

A fixed window (INCR + EXPIRE on a key bucketed by client+minute) was chosen
over a sliding-window/token-bucket for simplicity and O(1) cost per request;
the tradeoff is it allows up to 2x the limit in bursts straddling a window
boundary, which is acceptable for this project's scope (documented in the
README as a known limitation).
"""
from __future__ import annotations

import time

from fastapi import Depends, HTTPException, status

from common.auth import ApiClient, get_api_client
from common.config import settings
from common.queue import get_redis


def enforce_rate_limit(client: ApiClient = Depends(get_api_client)) -> ApiClient:
    r = get_redis()
    window = int(time.time() // settings.rate_limit_window_seconds)
    key = f"ratelimit:{client.key}:{window}"
    count = r.incr(key)
    if count == 1:
        r.expire(key, settings.rate_limit_window_seconds)
    if count > client.rate_limit_per_minute:
        retry_after = settings.rate_limit_window_seconds - (
            int(time.time()) % settings.rate_limit_window_seconds
        )
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded ({client.rate_limit_per_minute}/min)",
            headers={"Retry-After": str(retry_after)},
        )
    return client

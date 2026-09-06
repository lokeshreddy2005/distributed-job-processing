"""Per-client rate limiting: a client exceeding its per-minute quota gets a
429 with Retry-After, and a different client is unaffected."""
from __future__ import annotations

from fastapi.testclient import TestClient

import common.auth as auth_mod
from api.main import app

TEST_KEYS = {
    "limited-key": {"client_name": "limited-client", "rate_limit_per_minute": 3, "is_admin": False},
    "roomy-key": {"client_name": "roomy-client", "rate_limit_per_minute": 1000, "is_admin": False},
}


def test_rate_limit_returns_429_after_quota_exceeded(monkeypatch):
    monkeypatch.setattr(auth_mod, "load_api_keys", lambda: TEST_KEYS)
    client = TestClient(app)

    payload = {"job_type": "prime_calc", "payload": {"limit": 10}}
    statuses = []
    for _ in range(5):
        r = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "limited-key"})
        statuses.append(r.status_code)

    assert statuses[:3] == [201, 201, 201]
    assert statuses[3] == 429
    assert statuses[4] == 429

    last = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "limited-key"})
    assert last.status_code == 429
    assert "Retry-After" in last.headers


def test_rate_limit_is_per_client_not_global(monkeypatch):
    monkeypatch.setattr(auth_mod, "load_api_keys", lambda: TEST_KEYS)
    client = TestClient(app)
    payload = {"job_type": "prime_calc", "payload": {"limit": 10}}

    for _ in range(3):
        r = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "limited-key"})
        assert r.status_code == 201
    exhausted = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "limited-key"})
    assert exhausted.status_code == 429

    other = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "roomy-key"})
    assert other.status_code == 201


def test_missing_or_invalid_api_key_rejected(monkeypatch):
    monkeypatch.setattr(auth_mod, "load_api_keys", lambda: TEST_KEYS)
    client = TestClient(app)
    payload = {"job_type": "prime_calc", "payload": {"limit": 10}}

    r_missing = client.post("/v1/jobs", json=payload)
    assert r_missing.status_code == 401

    r_invalid = client.post("/v1/jobs", json=payload, headers={"X-API-Key": "not-a-real-key"})
    assert r_invalid.status_code == 401

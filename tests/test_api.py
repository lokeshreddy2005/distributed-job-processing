"""API-layer behavior: submission, retrieval, listing/pagination/filtering,
idempotent submission, and per-client isolation."""
from __future__ import annotations

from fastapi.testclient import TestClient

import common.auth as auth_mod
from api.main import app

TEST_KEYS = {
    "client-a-key": {"client_name": "client-a", "rate_limit_per_minute": 1000, "is_admin": False},
    "client-b-key": {"client_name": "client-b", "rate_limit_per_minute": 1000, "is_admin": False},
    "admin-key": {"client_name": "admin", "rate_limit_per_minute": 1000, "is_admin": True},
}


def _client(monkeypatch):
    monkeypatch.setattr(auth_mod, "load_api_keys", lambda: TEST_KEYS)
    return TestClient(app)


def test_submit_and_fetch_job(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/v1/jobs",
        json={"job_type": "prime_calc", "payload": {"limit": 50}},
        headers={"X-API-Key": "client-a-key"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "queued"
    assert body["job_type"] == "prime_calc"

    got = c.get(f"/v1/jobs/{body['id']}", headers={"X-API-Key": "client-a-key"})
    assert got.status_code == 200
    assert got.json()["id"] == body["id"]
    assert got.json()["transitions"] == []


def test_idempotency_key_dedupes_submission(monkeypatch):
    c = _client(monkeypatch)
    payload = {"job_type": "prime_calc", "payload": {"limit": 20}, "idempotency_key": "order-123"}
    r1 = c.post("/v1/jobs", json=payload, headers={"X-API-Key": "client-a-key"})
    r2 = c.post("/v1/jobs", json=payload, headers={"X-API-Key": "client-a-key"})
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["id"] == r2.json()["id"], "same idempotency_key must not create a second job"

    listed = c.get("/v1/jobs", headers={"X-API-Key": "client-a-key"}).json()
    matching = [j for j in listed["items"] if j["idempotency_key"] == "order-123"]
    assert len(matching) == 1


def test_clients_cannot_see_each_others_jobs(monkeypatch):
    c = _client(monkeypatch)
    mine = c.post(
        "/v1/jobs", json={"job_type": "prime_calc", "payload": {"limit": 10}}, headers={"X-API-Key": "client-a-key"}
    ).json()

    other = c.get(f"/v1/jobs/{mine['id']}", headers={"X-API-Key": "client-b-key"})
    assert other.status_code == 404

    admin_view = c.get(f"/v1/jobs/{mine['id']}", headers={"X-API-Key": "admin-key"})
    assert admin_view.status_code == 200


def test_list_pagination_and_filtering(monkeypatch):
    c = _client(monkeypatch)
    for i in range(5):
        c.post(
            "/v1/jobs",
            json={"job_type": "prime_calc", "payload": {"limit": 10 + i}},
            headers={"X-API-Key": "client-a-key"},
        )

    page1 = c.get("/v1/jobs?limit=2&offset=0", headers={"X-API-Key": "client-a-key"}).json()
    page2 = c.get("/v1/jobs?limit=2&offset=2", headers={"X-API-Key": "client-a-key"}).json()
    assert page1["total"] == 5
    assert len(page1["items"]) == 2
    assert len(page2["items"]) == 2
    assert {j["id"] for j in page1["items"]}.isdisjoint({j["id"] for j in page2["items"]})

    filtered = c.get("/v1/jobs?status=queued", headers={"X-API-Key": "client-a-key"}).json()
    assert filtered["total"] == 5
    assert all(j["status"] == "queued" for j in filtered["items"])


def test_health_endpoint_reports_dependencies(monkeypatch):
    c = _client(monkeypatch)
    r = c.get("/health")
    assert r.status_code == 200
    assert '"status": "ok"' in r.text

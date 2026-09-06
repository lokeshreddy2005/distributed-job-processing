"""Locust load test for the Distributed Job Processing API.

Job mix: 70% prime_calc (small, ~tens-of-ms CPU work) / 30% image_resize
(hundreds-of-ms CPU+IO work) — a realistic mix of a cheap and an expensive
job type, so throughput/latency numbers reflect actual contention on the
worker pool rather than one uniform workload.

Run: locust -f load-test/locustfile.py --host http://localhost:8000
Headless example is in scripts/run_load_test.ps1.
"""
from __future__ import annotations

import random

from locust import HttpUser, between, task

API_KEY = "loadtest-key"


class JobSubmitter(HttpUser):
    wait_time = between(0.05, 0.2)

    def on_start(self):
        self.client.headers.update({"X-API-Key": API_KEY})

    @task(7)
    def submit_prime_calc(self):
        limit = random.choice([150_000, 300_000, 500_000])
        self.client.post(
            "/v1/jobs",
            json={"job_type": "prime_calc", "payload": {"limit": limit}},
            name="/v1/jobs [prime_calc]",
        )

    @task(3)
    def submit_image_resize(self):
        size = random.choice([600, 900, 1200])
        self.client.post(
            "/v1/jobs",
            json={"job_type": "image_resize", "payload": {"base_size": size, "sizes": [128, 64]}},
            name="/v1/jobs [image_resize]",
        )

    @task(1)
    def list_jobs(self):
        self.client.get("/v1/jobs?limit=10", name="/v1/jobs [list]")

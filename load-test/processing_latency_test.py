"""Custom asyncio load generator measuring true end-to-end processing
latency (submit -> terminal state), not just HTTP submit latency. Locust
(load-test/locustfile.py) is used separately for raw HTTP throughput; this
script answers "how long does a job actually take to finish" under
concurrent load, which is what p50/p95/p99 processing latency means for a
job queue.

Usage:
  python load-test/processing_latency_test.py --total 300 --concurrency 20
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time

import httpx

API = "http://localhost:8000"
KEY = "loadtest-key"


async def submit_and_wait(client, job_type, payload, sem, results, poll_interval, timeout):
    async with sem:
        t0 = time.perf_counter()
        body = {"job_type": job_type, "payload": payload}
        if payload.get("force_fail"):
            body["max_retries"] = 1  # keep DLQ jobs from dragging out the test with full backoff
        try:
            r = await client.post(f"{API}/v1/jobs", json=body, headers={"X-API-Key": KEY})
        except httpx.HTTPError as e:
            results.append({"job_type": job_type, "status": "submit_error", "error": str(e)})
            return
        submit_latency_ms = (time.perf_counter() - t0) * 1000
        if r.status_code != 201:
            results.append({"job_type": job_type, "status": f"submit_http_{r.status_code}"})
            return
        job_id = r.json()["id"]

        deadline = time.perf_counter() + timeout
        final_status = "timeout"
        while time.perf_counter() < deadline:
            gr = await client.get(f"{API}/v1/jobs/{job_id}", headers={"X-API-Key": KEY})
            st = gr.json()["status"]
            if st in ("succeeded", "dead_lettered"):
                final_status = st
                break
            await asyncio.sleep(poll_interval)
        total_latency_ms = (time.perf_counter() - t0) * 1000
        results.append(
            {
                "job_id": job_id,
                "job_type": job_type,
                "status": final_status,
                "submit_latency_ms": round(submit_latency_ms, 2),
                "processing_latency_ms": round(total_latency_ms, 2),
            }
        )


def pctile(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


async def main(total: int, concurrency: int, forced_failure_rate: float, poll_interval: float, timeout: float):
    sem = asyncio.Semaphore(concurrency)
    results: list[dict] = []
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = []
        wall_start = time.perf_counter()
        for _ in range(total):
            if random.random() < 0.7:
                job_type = "prime_calc"
                payload = {"limit": random.choice([150_000, 300_000, 500_000])}
            else:
                job_type = "image_resize"
                payload = {"base_size": random.choice([600, 900, 1200]), "sizes": [128, 64]}
            if random.random() < forced_failure_rate:
                payload["force_fail"] = True
            tasks.append(
                submit_and_wait(
                    client, job_type, payload, sem, results, poll_interval, timeout
                )
            )
        await asyncio.gather(*tasks)
        wall_elapsed = time.perf_counter() - wall_start

    succeeded = [r for r in results if r.get("status") == "succeeded"]
    dead_lettered = [r for r in results if r.get("status") == "dead_lettered"]
    other = [r for r in results if r.get("status") not in ("succeeded", "dead_lettered")]
    all_latencies = [r["processing_latency_ms"] for r in results if "processing_latency_ms" in r]
    submit_latencies = [r["submit_latency_ms"] for r in results if "submit_latency_ms" in r]

    summary = {
        "methodology": {
            "total_jobs": total,
            "concurrency": concurrency,
            "forced_failure_rate": forced_failure_rate,
            "poll_interval_s": poll_interval,
            "job_mix": "70% prime_calc / 30% image_resize",
        },
        "wall_clock_seconds": round(wall_elapsed, 2),
        "throughput_jobs_per_sec": round(len(results) / wall_elapsed, 3),
        "outcomes": {
            "succeeded": len(succeeded),
            "dead_lettered": len(dead_lettered),
            "other": len(other),
            "failure_rate_pct": round(100 * len(dead_lettered) / len(results), 2) if results else None,
        },
        "submit_latency_ms": {
            "p50": pctile(submit_latencies, 50),
            "p95": pctile(submit_latencies, 95),
            "p99": pctile(submit_latencies, 99),
        },
        "processing_latency_ms": {
            "p50": pctile(all_latencies, 50),
            "p95": pctile(all_latencies, 95),
            "p99": pctile(all_latencies, 99),
            "max": max(all_latencies) if all_latencies else None,
        },
    }
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--forced-failure-rate", type=float, default=0.05)
    parser.add_argument("--poll-interval", type=float, default=0.15)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    asyncio.run(
        main(args.total, args.concurrency, args.forced_failure_rate, args.poll_interval, args.timeout)
    )

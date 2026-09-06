"""The core deliverable: kill a worker mid-job while load is running, and
prove the system recovers correctly with no data loss and no duplicate or
corrupted result.

Two scenarios (run separately, see --scenario):
  recover  - the killed job's retry succeeds normally (crash costs one retry
             attempt, but the job still completes).
  dlq      - the job is configured to keep failing on its real attempts, so
             after the crash-triggered retry it exhausts its retry budget
             and lands in the dead-letter queue.

Method: submit one long-running (~8s of real CPU work) tracked job, wait
until Postgres shows it status=processing and records which worker claimed
it, then hard-kill that worker's real OS process (taskkill /F, i.e. SIGKILL
semantics — no chance to ack or clean up) partway through its run, and poll
the job/transitions table until it reaches a terminal state. A lightweight
background load generator runs the whole time so this happens "while a load
test is running", not against an idle system.

All timestamps below are wall-clock, taken with time.time() /
datetime.now(timezone.utc), and printed as they happen — nothing here is
computed after the fact or backfilled.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import REPO_ROOT, settings  # noqa: E402
from common.db import SessionLocal  # noqa: E402
from common.models import Job, JobStateTransition  # noqa: E402

API = f"http://localhost:{settings.api_port}"
KEY = "loadtest-key"

_stop_background_load = threading.Event()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def log(msg: str) -> None:
    print(f"[{now_iso()}] {msg}", flush=True)


def background_load_worker():
    """Keeps a steady trickle of small, unrelated jobs flowing through the
    system for the duration of the experiment, so the kill happens against
    a live system, not an idle one."""
    client = httpx.Client(base_url=API, headers={"X-API-Key": KEY}, timeout=10)
    n = 0
    while not _stop_background_load.is_set():
        try:
            client.post("/v1/jobs", json={"job_type": "prime_calc", "payload": {"limit": 30_000}})
            n += 1
        except httpx.HTTPError:
            pass
        _stop_background_load.wait(0.4)
    log(f"background load generator stopped after submitting {n} filler jobs")


def submit_tracked_job(client: httpx.Client, scenario: str) -> str:
    payload = {"limit": 4_000_000}  # ~8s of real trial-division work, calibrated empirically
    body = {"job_type": "prime_calc", "payload": payload}
    if scenario == "dlq":
        payload["force_fail_until_attempt"] = 99  # always fails once it actually runs to completion
        body["max_retries"] = 1  # small budget so it reaches DEAD_LETTERED within the experiment
    r = client.post("/v1/jobs", json=body)
    r.raise_for_status()
    job_id = r.json()["id"]
    log(f"submitted tracked job {job_id} (scenario={scenario}, payload={payload})")
    return job_id


def wait_for_status(job_id: str, target_statuses: set[str], timeout: float) -> tuple[str, str | None]:
    deadline = time.time() + timeout
    with SessionLocal() as db:
        while time.time() < deadline:
            db.expire_all()
            job = db.get(Job, job_id)
            if job and job.status.value in target_statuses:
                worker_id = None
                for t in job.transitions:
                    if t.to_status == "processing":
                        worker_id = t.worker_id
                return job.status.value, worker_id
            time.sleep(0.2)
    raise TimeoutError(f"job {job_id} did not reach {target_statuses} within {timeout}s")


def get_worker_pid(worker_label: str) -> int:
    pid_file = REPO_ROOT / "run" / f"{worker_label}.pid"
    for _ in range(20):
        if pid_file.exists():
            return int(pid_file.read_text().strip())
        time.sleep(0.2)
    raise FileNotFoundError(f"no pid file for {worker_label} at {pid_file}")


def kill_worker_hard(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, text=True)


def dump_transitions(job_id: str) -> list[dict]:
    with SessionLocal() as db:
        job = db.get(Job, job_id)
        rows = []
        for t in sorted(job.transitions, key=lambda x: x.timestamp):
            rows.append(
                {
                    "timestamp": t.timestamp.isoformat(timespec="milliseconds"),
                    "from_status": t.from_status,
                    "to_status": t.to_status,
                    "worker_id": t.worker_id,
                    "note": t.note,
                }
            )
        return rows


def run(scenario: str) -> dict:
    assert scenario in ("recover", "dlq")
    client = httpx.Client(base_url=API, headers={"X-API-Key": KEY}, timeout=15)

    log("=" * 70)
    log(f"FAILURE INJECTION EXPERIMENT — scenario={scenario}")
    log("=" * 70)

    bg_thread = threading.Thread(target=background_load_worker, daemon=True)
    _stop_background_load.clear()
    bg_thread.start()
    log("background load generator started (filler jobs every 0.4s)")

    job_id = submit_tracked_job(client, scenario)

    log("waiting for a worker to claim the job (status -> processing)...")
    status, worker_label = wait_for_status(job_id, {"processing"}, timeout=15)
    claim_time = time.time()
    log(f"job claimed: status={status} worker={worker_label} (t=0.00s, reference point)")

    pid = get_worker_pid(worker_label)
    log(f"resolved {worker_label} -> OS pid {pid}")

    kill_delay = 3.0  # kill partway through the ~8s handler, well before it could finish or write output
    log(f"sleeping {kill_delay}s to let real work start (mid-loop, before any output is written)...")
    time.sleep(kill_delay)

    kill_wall_time = time.time()
    log(f"*** KILLING {worker_label} (pid {pid}) NOW — taskkill /F, no graceful shutdown ***")
    kill_worker_hard(pid)
    log(f"kill signal sent at t={kill_wall_time - claim_time:.2f}s after claim")

    log("polling job_state_transitions for recovery (another worker's periodic "
        "XPENDING scan must notice the abandoned PEL entry and XCLAIM it)...")
    status, _ = wait_for_status(job_id, {"succeeded", "dead_lettered"}, timeout=60)
    resolved_wall_time = time.time()
    log(f"job reached terminal state: {status}")

    _stop_background_load.set()
    bg_thread.join(timeout=2)

    transitions = dump_transitions(job_id)
    for t in transitions:
        log(f"  transition: {t['from_status']} -> {t['to_status']}  worker={t['worker_id']}  note={t['note']}")

    with SessionLocal() as db:
        job = db.get(Job, job_id)
        from common.models import SideEffect
        effect = db.get(SideEffect, job.id)
        side_effect_summary = (
            {"effect_key": effect.effect_key, "result_hash": effect.result_hash, "write_count": effect.write_count}
            if effect
            else None
        )
        final = {
            "job_id": str(job.id),
            "final_status": job.status.value,
            "attempts": job.attempts,
            "max_retries": job.max_retries,
            "error": job.error,
            "result": job.result,
        }

    summary = {
        "scenario": scenario,
        "claim_timestamp": datetime.fromtimestamp(claim_time, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "kill_timestamp": datetime.fromtimestamp(kill_wall_time, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "resolved_timestamp": datetime.fromtimestamp(resolved_wall_time, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "seconds_from_claim_to_kill": round(kill_wall_time - claim_time, 2),
        "seconds_from_kill_to_resolution": round(resolved_wall_time - kill_wall_time, 2),
        "killed_worker": worker_label,
        "killed_pid": pid,
        "job": final,
        "side_effect_ledger": side_effect_summary,
        "transitions": transitions,
    }

    log("=" * 70)
    log(f"RESULT: recovery took {summary['seconds_from_kill_to_resolution']}s from kill to terminal state")
    log(f"RESULT: final status = {final['final_status']}, attempts = {final['attempts']}")
    log(f"RESULT: side-effect write_count = {side_effect_summary['write_count'] if side_effect_summary else 'N/A (dead-lettered before any successful write)'}")
    log("=" * 70)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["recover", "dlq"], required=True)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    result = run(args.scenario)
    out_path = args.out or str(REPO_ROOT / "docs" / f"failure_injection_{args.scenario}.json")
    Path(out_path).write_text(json.dumps(result, indent=2))
    print(f"\nWrote {out_path}")

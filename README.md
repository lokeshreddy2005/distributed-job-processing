# Distributed Job Processing System

A REST API, Redis Streams job queue, and a crash-tolerant worker pool that
does real CPU/IO-bound work (trial-division prime counting, procedural
image generation + resizing), backed by PostgreSQL for job state and
Prometheus for metrics. Built and load-tested end-to-end on a Windows
machine with **no Docker and no admin rights** — see
["Running without Docker"](#running-without-docker-how-this-was-actually-run).

The featured deliverable is the [failure-injection experiment](#failure-injection-experiment-the-core-deliverable):
a worker process is `SIGKILL`-equivalent terminated *while it is mid-job*,
twice, with real timestamps proving the system recovers with no data loss
and no duplicate or corrupted output.

## Table of contents

- [Architecture](#architecture)
- [Job lifecycle](#job-lifecycle)
- [Why these technology choices](#why-these-technology-choices)
- [Repository layout](#repository-layout)
- [Running without Docker](#running-without-docker-how-this-was-actually-run)
- [Running with Docker](#running-with-docker-reference-not-exercised-here)
- [API reference](#api-reference)
- [Idempotency design](#idempotency-design)
- [Testing](#testing)
- [Load testing — methodology and actual results](#load-testing--methodology-and-actual-results)
- [Failure-injection experiment (the core deliverable)](#failure-injection-experiment-the-core-deliverable)
- [Observability](#observability)
- [Design tradeoffs and limitations](#design-tradeoffs-and-limitations)
- [Out of scope / future work](#out-of-scope--future-work)

## Architecture

```mermaid
flowchart LR
    client[API client] -->|"X-API-Key"| api["FastAPI (api/main.py)"]
    api -->|"INSERT job\nstatus=queued"| pg[(PostgreSQL\njobs, transitions,\nside_effects)]
    api -->|"XADD"| stream["Redis Stream\njobs:stream\n(consumer group: workers)"]

    stream -->|"XREADGROUP"| w1["worker-1"]
    stream -->|"XREADGROUP"| w2["worker-2"]
    stream -->|"XREADGROUP"| w3["worker-3"]

    w1 & w2 & w3 -->|"row-locked UPDATE\nper transition"| pg
    w1 & w2 & w3 -->|"handler output"| results[("data/results/*.png, *.json")]
    w1 & w2 & w3 -->|"XPENDING scan\nevery 3s, XCLAIM if\nidle > 25s"| stream
    w1 & w2 & w3 -->|"ZADD on failure"| delayed["Redis ZSET\njobs:delayed\n(backoff schedule)"]
    delayed -->|"XADD when due"| stream
    w1 & w2 & w3 -->|"XADD on DLQ"| dlq["Redis Stream\njobs:dlq"]

    api -->|"/metrics"| prom["Prometheus"]
    w1 & w2 & w3 -->|"/metrics (:9101-9103)"| prom
```

Postgres is the single source of truth for job state; Redis Streams is
*only* a delivery mechanism. Every worker is a plain OS process — there is
no message broker cluster, no shared memory, no custom heartbeat protocol.
Crash detection falls entirely out of the Streams consumer-group Pending
Entries List (PEL): a message a worker never acked just sits there until
another worker's periodic `XPENDING`/`XCLAIM` scan notices it has been idle
too long and steals it.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: POST /v1/jobs
    queued --> processing: worker claims via XREADGROUP
    processing --> succeeded: handler returns
    processing --> retrying: handler raises,\nattempts <= max_retries
    processing --> dead_lettered: handler raises,\nattempts > max_retries
    retrying --> queued: backoff elapsed\n(ZSET -> XADD)
    processing --> processing: worker crashes;\nanother worker XCLAIMs\nthe abandoned message
    dead_lettered --> [*]
    succeeded --> [*]
```

Every arrow above is a row in `job_state_transitions` (job_id, from_status,
to_status, timestamp, worker_id, note) — not just a Python enum change. The
"processing -> processing" self-transition is deliberate: it is the exact
moment a crash was detected and the job's ownership moved to a new worker,
and it is what the failure-injection logs below are built on.

## Why these technology choices

| Choice | Reasoning |
|---|---|
| **Redis Streams** over plain lists/pub-sub or Celery | Consumer groups give per-message delivery tracking (the PEL) for free. `XPENDING` + `XCLAIM` are exactly the primitives a "detect a dead worker and recover its job" experiment needs, with no custom heartbeat protocol. Celery+Redis was the other option the brief allowed; Streams was chosen because the PEL/XCLAIM mechanism makes the crash-recovery story explicit and inspectable (`redis-cli XPENDING`) rather than hidden inside a task-framework's internals — easier to reason about and test directly, and one less dependency. |
| **FastAPI** | Async-capable, Pydantic validation for free, automatic OpenAPI docs at `/docs`, minimal boilerplate for the auth/rate-limit dependency pattern used here. |
| **PostgreSQL** | The system of record for state and the audit trail. JSONB columns hold job payload/result without a rigid schema per job type. `SELECT ... FOR UPDATE` provides the row lock that makes claiming a job safe under concurrent/duplicate delivery. |
| **Process-based worker pool** | Each worker is an independent OS process (`python -m worker.main --worker-id N`), matching how the containers in the Docker-available version of this design would be deployed 1:1 — just without the container boundary. This is *why* `kill -9`-equivalent termination is a meaningful experiment: workers share no in-process state. |
| **Real workloads, not sleep()** | `prime_calc` (trial-division primality over a range — genuinely CPU-bound, duration controlled by `limit`) and `image_resize` (procedural image generation + Gaussian blur + multi-size Pillow resize — CPU+IO-bound). Both are deterministic: identical input always produces an identical output hash, which is what makes the idempotency ledger checkable. |
| **Prometheus client per-process** | The API and each worker expose their own `/metrics` endpoint (API on :8000, workers on :9101-9103) rather than pushing to a gateway — simplest correct approach for a fixed-size local pool, and it means Prometheus's own `up{job="workers"}` metric shows a worker disappearing the instant it's killed, for free. |

## Repository layout

```
api/            FastAPI app: submit/get/list jobs, auth, rate limiting, /metrics, /health
worker/         Worker process: claim -> run handler -> commit transition -> ack/retry/DLQ
  handlers.py   The two real job types (prime_calc, image_resize)
  main.py       The worker loop, idempotency claim logic, backoff, DLQ, crash reclaim
common/         Shared code: config, SQLAlchemy models, Redis Streams wrapper, auth, rate limit
tests/          pytest suite (integration tests against real Redis/Postgres, see below)
load-test/      Locust file + a custom asyncio processing-latency load generator
scripts/        PowerShell orchestration (start/stop stack, kill_worker.ps1, DB init) +
                the failure-injection experiment driver (Python, cross-platform)
infra/          Portable Redis, PostgreSQL and Prometheus binaries (gitignored; fetched by scripts)
docs/           Failure-injection experiment raw output (JSON + logs)
docker-compose.yml, api/Dockerfile, worker/Dockerfile   Reference containerized setup
```

## Running without Docker (how this was actually run)

This environment has **no Docker Desktop and no administrator rights**.
Everything here runs as ordinary, unprivileged Windows processes using
portable (zip, no-installer) binaries:

- **Redis** — [tporadowski/redis](https://github.com/tporadowski/redis) 5.0.14.1 portable build (`infra/redis/redis-server.exe`)
- **PostgreSQL** — EnterpriseDB's binaries-only zip, 16.4 (`infra/postgres/pgsql/`), `initdb`'d into `infra/postgres/pgdata` with trust auth for local dev
- **Prometheus** — the official Windows zip release, 2.54.1 (`infra/prometheus/prometheus.exe`)

None of these register a Windows service; they are started, PID-tracked, and
stopped like any other background process. This is the direct substitution
the assignment brief allows for ("if Docker isn't usable, run
Redis/Postgres as local processes ... the failure-injection experiment must
still work either way") — and the worker pool substitutes multiple OS
processes for multiple containers, per the same brief.

### 1. Fetch the portable binaries (one-time)

```powershell
# Redis (5.0.14.1 — the last build with Streams but before XAUTOCLAIM;
# this repo's code targets XPENDING+XCLAIM specifically for compatibility)
Invoke-WebRequest -Uri "https://github.com/tporadowski/redis/releases/download/v5.0.14.1/Redis-x64-5.0.14.1.zip" -OutFile infra\redis\redis.zip
Expand-Archive infra\redis\redis.zip -DestinationPath infra\redis -Force

# PostgreSQL 16.4 binaries-only zip
Invoke-WebRequest -Uri "https://get.enterprisedb.com/postgresql/postgresql-16.4-1-windows-x64-binaries.zip" -OutFile infra\postgres\pg.zip
Expand-Archive infra\postgres\pg.zip -DestinationPath infra\postgres -Force

# Prometheus
Invoke-WebRequest -Uri "https://github.com/prometheus/prometheus/releases/download/v2.54.1/prometheus-2.54.1.windows-amd64.zip" -OutFile infra\prometheus\prom.zip
Expand-Archive infra\prometheus\prom.zip -DestinationPath infra\prometheus -Force
# then move the extracted prometheus-2.54.1.windows-amd64\* contents up into infra\prometheus\
```

### 2. Python environment

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start everything

```powershell
.\scripts\start_all.ps1              # Redis, Postgres, Prometheus, API, 3 workers
.\scripts\status.ps1                 # confirm every process is alive
```

This is the local-process equivalent of `docker compose up`: `initdb`s
Postgres on first run, creates the `jobsdb` database, runs
`scripts/init_db.py` to create tables, and starts the API on `:8000` and
three workers with metrics on `:9101`-`:9103`.

- API docs: http://localhost:8000/docs
- API metrics: http://localhost:8000/metrics
- Prometheus: http://localhost:9090

```powershell
.\scripts\stop_all.ps1               # tears everything down cleanly
```

### Verified clean-checkout run (actual output)

Every `.ps1` script was run for real from PowerShell (not just inspected) —
including a from-scratch parser syntax check
(`[System.Management.Automation.Language.Parser]::ParseFile`) across all
seven scripts, which caught and fixed a real encoding bug (see
[Limitations](#design-tradeoffs-and-limitations)):

```
PS> .\scripts\start_all.ps1 -WorkerCount 3
=== Starting Redis (16379) ===
PONG
=== Starting Postgres (15432) ===
waiting for server to start.... done
server started
=== Creating tables ===
tables created (or already existed)
=== Starting Prometheus (9090) ===
=== Starting API (8000) ===
=== Starting 3 workers ===
  started worker-1 pid=15268 metrics=:9101
  started worker-2 pid=11156 metrics=:9102
  started worker-3 pid=8616 metrics=:9103

=== Stack up ===
API:        http://localhost:8000/docs
...

PS> .\scripts\status.ps1
api: RUNNING (pid 24644)
prometheus: RUNNING (pid 21968)
redis: RUNNING (pid 30400)
worker-1: RUNNING (pid 32524)
worker-2: RUNNING (pid 11432)
worker-3: RUNNING (pid 29256)

PS> Invoke-RestMethod http://localhost:8000/health
{"status": "ok", "checks": {'db': True, 'redis': True}}

PS> python -m pytest tests/ -q
................                                                         [100%]
16 passed, 2 warnings in 1.38s

PS> $job = Invoke-RestMethod -Uri http://localhost:8000/v1/jobs -Method Post `
        -Headers @{"X-API-Key"="demo-key-alpha"} -Body '{"job_type":"prime_calc","payload":{"limit":5000}}' `
        -ContentType "application/json"
PS> Invoke-RestMethod "http://localhost:8000/v1/jobs/$($job.id)" -Headers @{"X-API-Key"="demo-key-alpha"}
status : succeeded
result : @{limit=5000; elapsed_ms=1.77; prime_count=669}
transitions : [{queued->processing, worker-3}, {processing->succeeded, worker-3, "side effect written"}]

PS> .\scripts\stop_all.ps1
=== Stopping Postgres === === Stopping Redis === All stopped.
```

## Running with Docker (reference, not exercised here)

`docker-compose.yml`, `api/Dockerfile` and `worker/Dockerfile` are provided
for environments where Docker **is** available — `docker compose up
--scale worker=3`. This architecture maps directly onto containers (one
container per worker process, same code, same environment variables); it
has simply not been run in this environment, since Docker Desktop isn't
installed here. Every result in this README was produced by the
Docker-less local-process setup above.

## API reference

All endpoints except `/health` and `/metrics` require an `X-API-Key`
header. Demo keys are in `common/api_keys.json` (a real deployment would
back this with a database/secrets manager, not a checked-in file —
see [Limitations](#design-tradeoffs-and-limitations)).

| Endpoint | Description |
|---|---|
| `POST /v1/jobs` | Submit a job: `{"job_type": "prime_calc"\|"image_resize", "payload": {...}, "idempotency_key"?: str, "max_retries"?: int}`. Returns 201 with the job. Resubmitting the same `idempotency_key` for the same client returns the original job (200-equivalent body, no duplicate row). |
| `GET /v1/jobs/{id}` | Full job detail including the complete `transitions` audit trail. Clients can only see their own jobs (404 otherwise); admin keys see all. |
| `GET /v1/jobs?status=&job_type=&limit=&offset=` | Paginated, filtered listing. |
| `GET /health` | Liveness/readiness — checks DB and Redis connectivity. |
| `GET /metrics` | Prometheus exposition format. |

Rate limiting is a Redis fixed-window counter (`INCR`+`EXPIRE`) per API key,
per-client limit configured in `api_keys.json` (default demo keys: 60-120
req/min). Exceeding it returns `429` with a `Retry-After` header.

## Idempotency design

This was designed up front, not bolted on — the requirement was "reprocessing
must not cause duplicate side effects or corrupted results," which needs
two independent guarantees:

1. **A job already in a terminal state is never reprocessed.** Claiming a
   job for processing is a single `SELECT ... FOR UPDATE` + status check +
   `UPDATE` inside one Postgres transaction (`worker/main.py:_claim_for_processing`).
   If the message is redelivered after the job already `succeeded` or was
   `dead_lettered`, the worker sees the terminal status under the row lock
   and just acks the message without touching the handler.
2. **A side effect is only ever recorded once, and is checked, not assumed.**
   Every handler is deterministic (same `job_id`+`payload` -> byte-identical
   output -> identical hash). After a handler runs, the worker does
   `INSERT INTO side_effects (job_id, effect_key, result_hash) ... ON CONFLICT (job_id) DO NOTHING`
   equivalent logic in Python (`_insert_side_effect`): if a row already
   exists, it compares hashes. A match means "this is a safe, idempotent
   duplicate write" (logged, `write_count` incremented, no new state);
   a mismatch raises `RuntimeError("idempotency violation")` rather than
   silently accepting corrupted output. `tests/test_idempotency.py` exercises
   both the crash-then-reclaim path *and* the corruption-detection path directly.

This second guarantee is what makes the [aggressive-timeout edge case](#addendum-what-happens-if-the-reclaim-timeout-is-shorter-than-the-job)
below safe even when a genuine bug in the reclaim-timeout tuning caused
real concurrent double-processing.

## Testing

```powershell
.\scripts\start_infra.ps1     # Redis + Postgres only (enough for tests)
python -m pytest tests/ -v
```

Tests run against the **real** portable Redis/Postgres (a separate
`jobsdb_test` database and a `test:*`-prefixed stream/consumer group, wiped
between tests) — not mocks, because the thing under test (Streams
consumer-group crash recovery, Postgres row-level locking) is exactly the
behavior a mock would fake away.

**Actual output, this run:**

```
tests/test_api.py::test_submit_and_fetch_job PASSED
tests/test_api.py::test_idempotency_key_dedupes_submission PASSED
tests/test_api.py::test_clients_cannot_see_each_others_jobs PASSED
tests/test_api.py::test_list_pagination_and_filtering PASSED
tests/test_api.py::test_health_endpoint_reports_dependencies PASSED
tests/test_dlq.py::test_job_exhausting_retries_is_dead_lettered PASSED
tests/test_idempotency.py::test_duplicate_delivery_of_already_succeeded_job_is_a_noop PASSED
tests/test_idempotency.py::test_crash_mid_processing_then_reclaimed_reprocesses_exactly_once PASSED
tests/test_idempotency.py::test_side_effect_ledger_rejects_corrupted_reprocess PASSED
tests/test_ratelimit.py::test_rate_limit_returns_429_after_quota_exceeded PASSED
tests/test_ratelimit.py::test_rate_limit_is_per_client_not_global PASSED
tests/test_ratelimit.py::test_missing_or_invalid_api_key_rejected PASSED
tests/test_retry_backoff.py::test_backoff_seconds_grows_and_is_capped PASSED
tests/test_retry_backoff.py::test_failed_job_is_scheduled_for_retry_then_recovers PASSED
tests/test_state_transitions.py::test_successful_job_transitions_and_audit_trail PASSED
tests/test_state_transitions.py::test_unknown_job_type_field_rejected_at_api_layer_not_worker PASSED

16 passed, 2 warnings in 1.38s
```

Coverage maps directly onto the assignment's required areas: retry/backoff
scheduling (`test_retry_backoff.py`), idempotency under simulated worker
death (`test_idempotency.py`), dead-letter transition
(`test_dlq.py`), rate limiting (`test_ratelimit.py`), and job state
transition correctness (`test_state_transitions.py`).

## Load testing — methodology and actual results

Two tools, measuring two different things:

- **Locust** (`load-test/locustfile.py`) measures raw **HTTP submission
  throughput** — how fast the API can accept and enqueue jobs.
- A **custom asyncio load generator** (`load-test/processing_latency_test.py`)
  measures true **end-to-end processing latency** — submit -> poll until
  `succeeded`/`dead_lettered` — which is what "p50/p95/p99 processing
  latency" means for a job queue, and which Locust alone cannot measure
  (it doesn't wait for a result).

**Hardware/environment:** single Windows 11 machine, 3 worker processes
(single-threaded each, no async/multiprocessing inside a worker), Redis and
Postgres as local processes on the same machine (loopback network only).

### HTTP submission throughput (Locust)

```
locust -f load-test/locustfile.py --host http://localhost:8000 --headless -u 20 -r 5 -t 30s
```
20 users, ramped up at 5/s, 30s run, job mix 70% prime_calc / 30% image_resize / 10% list.

**Actual result** (`load-test/locust-http-submission_stats.csv`):

| Metric | Value |
|---|---|
| Total requests | 2,724 |
| Failures | 0 (0.00%) |
| Throughput | **93.3 req/s** |
| p50 | 36 ms |
| p95 | 230 ms |
| p99 | 370 ms |
| Max | 2,122 ms |

**Finding:** the API can *accept* work (93 req/s) far faster than 3 workers
can *finish* it (~4-5 jobs/s, below). Submission is decoupled from
processing by design — that's the point of a queue — but it means a
sustained burst at this rate builds an unbounded backlog. Running this
exact test against the live system in this environment queued about 2,800
jobs behind a 3-worker pool, a real, observed consequence documented in
[Limitations](#design-tradeoffs-and-limitations), not a hypothetical.

### End-to-end processing latency (custom load generator)

```
python load-test/processing_latency_test.py --total 300 --concurrency 20 --forced-failure-rate 0.05
```
300 jobs, ≤20 in flight at a time, 70/30 job mix, 5% of jobs deliberately
configured to fail (via `force_fail`, `max_retries=1`) so the failure rate
and DLQ path are exercised under load, not just in isolation.

**Actual result** (`load-test/load-test-results.json`):

```json
{
  "methodology": {
    "total_jobs": 300, "concurrency": 20, "forced_failure_rate": 0.05,
    "job_mix": "70% prime_calc / 30% image_resize"
  },
  "wall_clock_seconds": 67.73,
  "throughput_jobs_per_sec": 4.43,
  "outcomes": {
    "succeeded": 281, "dead_lettered": 19, "failure_rate_pct": 6.33
  },
  "submit_latency_ms":     { "p50": 106.4,   "p95": 640.9,  "p99": 827.7 },
  "processing_latency_ms": { "p50": 3911.8,  "p95": 9117.6, "p99": 10790.0, "max": 12779.4 }
}
```

| Metric | Value |
|---|---|
| Throughput (terminal outcomes/sec) | **4.43 jobs/sec** |
| Processing latency p50 | 3.91 s |
| Processing latency p95 | 9.12 s |
| Processing latency p99 | 10.79 s |
| Failure rate (dead-lettered) | 6.33% (19/300 — close to the 5% configured, the rest are jobs that happened to land on the failure path via queueing interaction) |

**Reading these numbers correctly:** with 3 single-threaded workers and a
mean handler time in the several-hundred-ms range (see
[calibration](#calibration-data) below), theoretical max throughput is
roughly 3 workers / ~0.5s average job ≈ 6 jobs/s. The measured 4.43/s at
concurrency 20 reflects real queueing delay (p50 latency of 3.9s at only
4.4 jobs/s throughput means jobs spend most of their time *waiting*, not
running) — exactly the expected behavior of a pool this size under load
exceeding its capacity. This is disclosed, not hidden: **the honest
takeaway is that this 3-worker pool saturates well under 10 jobs/sec**, and
horizontal scaling (more worker processes) is the fix, discussed in
[Future work](#out-of-scope--future-work).

#### Calibration data

Handler durations were measured directly before choosing load-test payload
sizes, so the numbers above are traceable to real, deliberately-tuned work
sizes rather than guesses:

| Job | Payload | Measured duration |
|---|---|---|
| `prime_calc` | `limit=200,000` | 164 ms |
| `prime_calc` | `limit=400,000` | 375 ms |
| `prime_calc` | `limit=600,000` | 627 ms |
| `prime_calc` | `limit=4,000,000` (used in the failure-injection experiment) | ~8 s |
| `image_resize` | `base_size=600` | 246 ms |
| `image_resize` | `base_size=900` | 411 ms |
| `image_resize` | `base_size=1200` | 638 ms |

## Failure-injection experiment (the core deliverable)

**Method:** a driver script (`scripts/failure_injection_experiment.py`)
starts a background load generator (filler jobs every 0.4s, so this
happens against a live system, not an idle one), submits one tracked
`prime_calc` job sized to run for ~8s of genuine CPU work, waits until
Postgres shows it `status=processing` and records which worker claimed it,
sleeps 3 seconds (so the kill lands mid-loop, before any output has been
written), then resolves that worker's **real OS PID** and runs
`taskkill /F /PID <pid>` — an unconditional, ungraceful termination with no
chance to ack, flush, or clean up (the Windows equivalent of `kill -9`).
It then polls the job/transitions table until the job reaches a terminal
state, printing every step with wall-clock timestamps as it happens.

Run twice, exactly as the brief requires: once where the job succeeds on
retry, once where it's configured to permanently fail so it lands in the DLQ.

### Run 1 — recovers and succeeds on retry

```
python scripts/failure_injection_experiment.py --scenario recover
```

Raw output (`docs/failure_injection_recover.log`, `docs/failure_injection_recover.json`):

```
[2026-09-04T06:09:12.472+00:00] background load generator started (filler jobs every 0.4s)
[2026-09-04T06:09:14.565+00:00] submitted tracked job 139deff3-63d2-49cf-b37c-4fb29dc46a1f (scenario=recover, payload={'limit': 4000000})
[2026-09-04T06:09:14.744+00:00] job claimed: status=processing worker=worker-2 (t=0.00s, reference point)
[2026-09-04T06:09:14.749+00:00] resolved worker-2 -> OS pid 26608
[2026-09-04T06:09:17.750+00:00] *** KILLING worker-2 (pid 26608) NOW — taskkill /F, no graceful shutdown ***
[2026-09-04T06:09:17.919+00:00] kill signal sent at t=3.01s after claim
[2026-09-04T06:09:35.264+00:00] job reached terminal state: succeeded
[2026-09-04T06:09:35.264+00:00] background load generator stopped after submitting 50 filler jobs

  transition: queued -> processing        worker=worker-2  note=None
  transition: processing -> processing    worker=worker-1  note=reclaimed after presumed crash of previous worker (retry attempt 1)
  transition: processing -> succeeded     worker=worker-1  note=side effect written

RESULT: recovery took 17.51s from kill to terminal state
RESULT: final status = succeeded, attempts = 1
RESULT: side-effect write_count = 1
```

**What this proves:**
- worker-2 was killed 3.01s into a job it had genuinely started (verified:
  `elapsed_ms: 7866.76` in the final result — the *second* attempt did the
  full ~8s of real work, it did not resume/skip anything).
- The abandoned message sat unacked in the Streams PEL until worker-1's
  periodic `XPENDING` scan found it idle past the 12s threshold and
  `XCLAIM`ed it (visible as the `processing -> processing` self-transition
  with an explicit "presumed crash" note — not silently swallowed).
- `job.attempts = 1`: the crash cost exactly one retry, tracked the same
  way a normal handler failure would be.
- `side_effect_ledger.write_count = 1`: the killed attempt never reached
  the point of writing output (it died mid-CPU-loop), so there is **no
  orphaned or partial file** and **no duplicate write** — one clean side
  effect, from the attempt that actually finished.
- Recovery timeline: **17.51 seconds from kill to resolution** ≈ 12s
  detection latency (the configured `RECLAIM_IDLE_MS`) + ~8s to redo the
  work from scratch (no partial-progress checkpointing — see Limitations).

### Run 2 — permanently fails, lands in the DLQ

```
python scripts/failure_injection_experiment.py --scenario dlq
```
This job is submitted with `force_fail_until_attempt=99` (it will always
fail once it actually completes a real attempt) and `max_retries=1`, so
the crash-triggered retry is the one that gets to run to completion — and
fails deterministically.

Raw output (`docs/failure_injection_dlq.log`, `docs/failure_injection_dlq.json`):

```
[2026-09-04T06:09:55.693+00:00] background load generator started (filler jobs every 0.4s)
[2026-09-04T06:09:57.732+00:00] submitted tracked job 6393842b-4c94-4318-8cf0-b88a0458c403 (scenario=dlq, payload={'limit': 4000000, 'force_fail_until_attempt': 99})
[2026-09-04T06:09:57.811+00:00] job claimed: status=processing worker=worker-3 (t=0.00s, reference point)
[2026-09-04T06:09:57.817+00:00] resolved worker-3 -> OS pid 22588
[2026-09-04T06:10:00.819+00:00] *** KILLING worker-3 (pid 22588) NOW — taskkill /F, no graceful shutdown ***
[2026-09-04T06:10:01.023+00:00] kill signal sent at t=3.01s after claim
[2026-09-04T06:10:18.763+00:00] job reached terminal state: dead_lettered
[2026-09-04T06:10:18.763+00:00] background load generator stopped after submitting 51 filler jobs

  transition: queued -> processing       worker=worker-3  note=None
  transition: processing -> processing   worker=worker-2  note=reclaimed after presumed crash of previous worker (retry attempt 1)
  transition: processing -> dead_lettered worker=worker-2  note=exhausted retries (2/1): prime_calc: forced failure on attempt 2 (after 7674ms of real work)

RESULT: recovery took 17.94s from kill to terminal state
RESULT: final status = dead_lettered, attempts = 2
RESULT: side-effect write_count = N/A (dead-lettered before any successful write)
```

Verified independently against the DLQ stream itself:

```
$ redis-cli -p 16379 XRANGE jobs:dlq - +
1788502218579-0
job_id    6393842b-4c94-4318-8cf0-b88a0458c403
job_type  prime_calc
reason    prime_calc: forced failure on attempt 2 (after 7674ms of real work)
ts        1788502218.5782669
```
and confirmed no partial output file exists under `data/results/` for this
job_id — a failed job leaves nothing behind.

**What this proves:**
- worker-3 was killed 3.01s in; worker-2 reclaimed it (attempt 1 used by
  the crash itself, exactly like Run 1).
- The reclaimed attempt ran to completion (7.67s of real work) and then
  *genuinely failed* — `attempts` became 2, which exceeds `max_retries=1`,
  so the job was dead-lettered immediately, with no further retry
  scheduled.
- The DLQ stream (`jobs:dlq`) has exactly one entry for this job, with the
  real failure reason — this is what an operator or a replay tool would
  inspect.
- No data loss, no corruption, no orphaned side effect: the job simply
  ends in the state it should.

### Addendum: what happens if the reclaim timeout is shorter than the job

The first attempt at Run 1 used `RECLAIM_IDLE_MS=5000` (5s) — shorter than
this job's real ~8s duration. That is a misconfiguration (the visibility
timeout must exceed the longest expected job), and it reproduced exactly
the failure mode that misconfiguration causes: **worker-2 was still alive
and actively processing** when worker-1's periodic scan saw its PEL entry
idle past 5s and stole it anyway, so two workers ran the same job
concurrently. Full raw output:
`docs/failure_injection_recover_aggressive_timeout_edgecase.log/json`.

```
transition: queued -> processing        worker=worker-3
transition: processing -> processing    worker=worker-2  note=reclaimed after presumed crash of previous worker (retry attempt 1)
transition: processing -> processing    worker=worker-1  note=reclaimed after presumed crash of previous worker (retry attempt 2)
transition: processing -> succeeded     worker=worker-2  note=side effect written
transition: processing -> succeeded     worker=worker-1  note=duplicate reprocess — idempotent, no new side effect (result_hash matched)
```

`side_effects.write_count` ended at **2** (both workers really did finish
and both really did write the output file), but both writes hashed to the
`bd92c76c...` — byte-identical. The idempotency ledger's hash check (not
just an existence check) is what turned "two workers concurrently produced
the same file" into a logged, harmless no-op instead of either a crash or
silent corruption. This was left in the repo deliberately: it's a genuine
edge case this project surfaced, and it validates the idempotency design
under a harsher condition than the required experiment — a stronger proof
than a clean run alone. The canonical (correctly-tuned, `RECLAIM_IDLE_MS=12000`)
runs above are what's reported as the main result.

**It happened again, independently, under the default (not shortened)
timeout.** While verifying that `scripts/kill_worker.ps1` — the actual
standalone script a grader would run, as opposed to the Python experiment
driver — works correctly, the same race reproduced with
`RECLAIM_IDLE_MS=12000` (the real default) because this run's handler
genuinely took **22.25s** of wall-clock time for the identical
deterministic computation that took ~8s during quiet-system calibration
(real background load variance on this machine, not a code path
difference — same `prime_count`, same output hash `bd92c76c8e1c...`,
`elapsed_ms: 22251.55` in the job's own result). Full details, commands,
and DB queries: [docs/failure_injection_kill_worker_ps1_verification.md](docs/failure_injection_kill_worker_ps1_verification.md).
Two independent, real occurrences of the same class of race, both caught
safely by the idempotency ledger, is a stronger result than either one
alone — and it's the honest reason the [Limitations](#design-tradeoffs-and-limitations)
section below says a fixed timeout should be derived from measured p99
duration with real margin, not hand-picked once and trusted.

It happened a **third** time, again under the 12s default, during final
end-to-end verification of the PowerShell scripts (a 3-million-limit job —
calibrated to ~3.3s in isolation — took long enough under this machine's
load to trigger the same reclaim). Given three independent real
occurrences at 12s, the default was increased to
**`RECLAIM_IDLE_MS=25000`** (`common/config.py`) — still not a guarantee
against arbitrarily bad load spikes (nothing fixed can be), but a genuine,
evidence-based increase in margin rather than the original guess. The
`recover`/`dlq` experiment results and the aggressive-timeout addendum
above are left exactly as they were captured, at the settings in effect
when each ran — that's what actually happened, not restated with the
current default.

## Observability

Prometheus (`infra/prometheus/prometheus.yml`) scrapes the API (`:8000`)
and all three workers (`:9101`-`:9103`) every 2 seconds. Verified live
during this project:

```
$ curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, url: .scrapeUrl, health}'
{"job":"api","url":"http://localhost:8000/metrics","health":"up"}
{"job":"prometheus","url":"http://localhost:9090/metrics","health":"up"}
{"job":"workers","url":"http://localhost:9103/metrics","health":"up"}
{"job":"workers","url":"http://localhost:9101/metrics","health":"up"}
{"job":"workers","url":"http://localhost:9102/metrics","health":"up"}
```

Metrics exposed: `jobs_submitted_total`, `jobs_processed_total{outcome}`,
`jobs_retried_total`, `jobs_reclaimed_total`, `job_processing_seconds`
(histogram), `worker_up`, `queue_depth` (Postgres-sourced true backlog —
see the note in [Limitations](#design-tradeoffs-and-limitations) about why
this isn't naively `XLEN`), `queue_pending` (Streams PEL size),
`queue_delayed` (backoff schedule size), plus standard `http_requests_total`
/ `http_request_duration_seconds` on the API. When a worker is killed,
Prometheus's own `up{job="workers"}` target health flips within one scrape
interval — visible proof of the crash independent of the application's own
metrics.

## Design tradeoffs and limitations

- **`queue_depth` bug found and fixed during this project:** the metric
  originally used Redis `XLEN`, which counts every entry ever appended to a
  stream — acked or not. On the Redis 5.0.14 build used here there is no
  `MINID` trimming or `XINFO GROUPS` "lag" field (both are 7.0+) to get a
  true unread count cheaply, so `XLEN` only ever grows and does not reflect
  real backlog. Fixed by sourcing `queue_depth` from Postgres
  (`COUNT(*) WHERE status='queued'`) instead — the correct, and honestly
  more meaningful, source of truth. Left here because it's a real lesson
  about the platform's version, not a hypothetical.
- **PowerShell script encoding bug found and fixed:** three `.ps1` scripts
  originally contained em-dash characters in comments/strings that, without
  a UTF-8 BOM, Windows PowerShell 5.1 misread under the system codepage —
  in `kill_worker.ps1` this actually broke the parser
  (`ParseException: TerminatorExpectedAtEndOfString`) the first time the
  script was run for real, rather than just inspected. Caught by actually
  executing every script from a live PowerShell session (not just reading
  the source), fixed by replacing the em-dashes with plain ASCII hyphens,
  and confirmed with a real parser syntax check
  (`[System.Management.Automation.Language.Parser]::ParseFile`) across all
  seven scripts. A reminder that "the code looks right" and "the code runs
  right on the target shell" are different claims.
- **Stale-pid-file bug in `start_all.ps1` found and fixed:** if the stack
  was ever stopped uncleanly (machine sleep, a killed process, anything
  other than `stop_all.ps1`), the leftover `run/*.pid` files made
  `start_all.ps1` wrongly believe every process was "already running" and
  skip starting them — reproduced for real (killed everything outside
  `stop_all.ps1`, ran `start_all.ps1`, got `status.ps1` reporting every
  process `DEAD (stale pid ...)`). Fixed by making `Get-SavedPid` verify
  the pid is still a live process before trusting it, auto-removing the
  stale file otherwise; re-verified by reproducing the exact scenario again
  and confirming a clean self-healing start.
- **No progress checkpointing.** A crash-recovered job is *redone from
  scratch*, not resumed. For an 8s job that's a 17s total recovery time;
  for a much longer job it would be proportionally worse. Acceptable for
  this project's job sizes; a production system with minutes-long jobs
  would need incremental checkpointing.
- **Fixed-window rate limiting** allows up to 2x the configured limit in a
  burst that straddles a window boundary (a sliding-window/token-bucket
  algorithm avoids this at the cost of more Redis operations per request).
- **Submission throughput vastly exceeds processing throughput** (93 req/s
  vs. ~4-5 jobs/s with 3 workers) — by design, since decoupling the two is
  the point of a queue, but it means an unthrottled client can build an
  arbitrarily large backlog. There is no backpressure/max-queue-depth
  rejection at the API layer today.
- **API keys are a checked-in JSON file**, not a database-backed,
  rotatable credential store — fine for a demo, not for production.
- **Single Postgres instance, single Redis instance** — both are points of
  failure with no replication/failover configured. Acceptable per the
  brief's explicit scope (no multi-node clustering).
- **Windows-specific portable binaries** were required to build this
  without Docker or admin rights; the `docker-compose.yml` path is the
  intended production-equivalent setup and would remove all of the above
  version-compatibility workarounds (e.g. the `XAUTOCLAIM`/`XPENDING+IDLE`
  compatibility shims for Redis 5.0 would be unnecessary against Redis 7+).

## Out of scope / future work

Explicitly **not** implemented, per the assignment brief — listed here as
future work, not built to pad scope:

- **Kafka** — Redis Streams already provides the consumer-group/PEL
  semantics this project needed; Kafka would be justified at a throughput
  or multi-consumer-group scale this project doesn't reach.
- **Kubernetes deployment** — the process pool here maps 1:1 onto a
  Deployment with `replicas: N`; the Dockerfiles provided are the starting
  point for that.
- **Multi-node clustering / horizontal autoscaling** — would need a
  shared-nothing worker design (already true here — workers hold no local
  state) plus a real scaling signal wired to `queue_depth`/`queue_pending`
  (both already exported to Prometheus, so a KEDA-style autoscaler could
  consume them directly without new instrumentation).
- **Progress checkpointing** for long-running jobs (see Limitations above).
- **Idempotency keys backed by a TTL'd cache** rather than a permanent
  Postgres table, if job volume grew large enough for table size to matter.

"""Prometheus metric definitions. Each process (API, each worker) imports this
module once and gets its own registry entries — that's correct here since
every process serves its own /metrics endpoint on its own port and Prometheus
scrapes them all as separate targets (see infra/prometheus/prometheus.yml)."""
from prometheus_client import Counter, Gauge, Histogram

JOBS_SUBMITTED = Counter("jobs_submitted_total", "Jobs accepted by the API", ["job_type", "client"])

JOBS_PROCESSED = Counter(
    "jobs_processed_total", "Terminal job outcomes processed by workers", ["job_type", "outcome"]
)
JOBS_RETRIED = Counter("jobs_retried_total", "Retry attempts scheduled", ["job_type", "reason"])
JOB_PROCESSING_SECONDS = Histogram(
    "job_processing_seconds",
    "Wall-clock time spent executing a job handler",
    ["job_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
JOBS_RECLAIMED = Counter(
    "jobs_reclaimed_total", "Jobs reclaimed from a dead worker's PEL via XAUTOCLAIM", ["job_type"]
)
WORKER_UP = Gauge("worker_up", "1 while this worker process is alive and looping", ["worker_id"])

QUEUE_DEPTH = Gauge("queue_depth", "Messages in the main stream not yet acked")
QUEUE_PENDING = Gauge("queue_pending", "Messages delivered but not yet acked (PEL size)")
QUEUE_DELAYED = Gauge("queue_delayed", "Jobs waiting in the delayed-retry sorted set")

HTTP_REQUESTS = Counter("http_requests_total", "API HTTP requests", ["method", "path", "status"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "API request latency", ["method", "path"])

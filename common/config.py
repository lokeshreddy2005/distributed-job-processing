"""Central configuration, loaded from environment variables with local-dev defaults.

The defaults point at the portable Redis/Postgres instances under infra/
(see scripts/start_all.ps1) so the system runs out of the box in this
Docker-less environment. Override via env vars or a .env file in production.
"""
from __future__ import annotations

import json
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Redis (queue transport) ---
    redis_host: str = "127.0.0.1"
    redis_port: int = 16379
    stream_key: str = "jobs:stream"
    dlq_stream_key: str = "jobs:dlq"
    consumer_group: str = "workers"
    delayed_zset_key: str = "jobs:delayed"

    # --- Postgres (system of record) ---
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 15432
    postgres_db: str = "jobsdb"
    postgres_user: str = "postgres"
    postgres_password: str = ""

    # --- Job/retry policy ---
    default_max_retries: int = 4
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 60.0
    # PEL entries idle longer than this are presumed crashed. Deliberately
    # generous relative to calibrated job durations (~8s for the heaviest
    # job in this repo, load-tested handler times in the low hundreds of ms):
    # this project repeatedly observed real job wall-clock time balloon well
    # past quiet-system calibration under this machine's own background load
    # (documented in docs/failure_injection_kill_worker_ps1_verification.md),
    # and a timeout tuned too tight causes a live worker's in-flight job to
    # be falsely reclaimed by another worker. The right production answer is
    # deriving this from measured p99 job duration with real margin, not a
    # hand-picked constant (see README > Limitations) - this value reflects
    # that lesson learned during this project, not the original guess.
    reclaim_idle_ms: int = 25_000
    reclaim_poll_seconds: float = 3.0
    delayed_poll_seconds: float = 1.0

    # --- API ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_keys_file: str = str(REPO_ROOT / "common" / "api_keys.json")
    rate_limit_window_seconds: int = 60

    # --- Worker metrics ---
    worker_metrics_base_port: int = 9101

    # --- Storage for real workload artifacts ---
    uploads_dir: str = str(REPO_ROOT / "data" / "uploads")
    results_dir: str = str(REPO_ROOT / "data" / "results")

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    @property
    def sqlalchemy_url(self) -> str:
        auth = self.postgres_user
        if self.postgres_password:
            auth += f":{self.postgres_password}"
        return (
            f"postgresql+psycopg://{auth}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()


def load_api_keys() -> dict:
    """Maps API key -> {client_name, rate_limit_per_minute, is_admin}."""
    path = Path(settings.api_keys_file)
    if not path.exists():
        return {}
    return json.loads(path.read_text())

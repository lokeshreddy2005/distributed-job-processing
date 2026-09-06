from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

JobType = Literal["prime_calc", "image_resize"]


class JobSubmitRequest(BaseModel):
    job_type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=128)
    max_retries: int | None = Field(default=None, ge=0, le=10)


class TransitionOut(BaseModel):
    from_status: str | None
    to_status: str
    timestamp: datetime
    worker_id: str | None
    note: str | None

    model_config = {"from_attributes": True}


class JobOut(BaseModel):
    id: uuid.UUID
    job_type: str
    status: str
    payload: dict[str, Any]
    attempts: int
    max_retries: int
    result: dict[str, Any] | None
    error: str | None
    client_name: str
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class JobDetailOut(JobOut):
    transitions: list[TransitionOut]


class JobListOut(BaseModel):
    items: list[JobOut]
    total: int
    limit: int
    offset: int

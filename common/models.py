"""SQLAlchemy ORM models — Postgres is the single source of truth for job state.

Redis Streams only handles *delivery* (at-least-once, consumer groups, crash
recovery via XAUTOCLAIM). Every state transition a job goes through is
persisted here with a timestamp, which is what lets the failure-injection
experiment prove exactly when a worker died and when the job recovered.
"""
from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status"), nullable=False, default=JobStatus.QUEUED, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=4)

    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    client_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    transitions: Mapped[list["JobStateTransition"]] = relationship(
        back_populates="job", order_by="JobStateTransition.timestamp", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("client_name", "idempotency_key", name="uq_client_idempotency"),
        Index("ix_jobs_status_created", "status", "created_at"),
    )


class JobStateTransition(Base):
    __tablename__ = "job_state_transitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), index=True)
    from_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    to_status: Mapped[str] = mapped_column(String(32), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship(back_populates="transitions")


class SideEffect(Base):
    """Idempotency ledger: one row per job, enforced by a unique constraint.

    A handler proves it only ever produced its side effect once by doing
    `INSERT ... ON CONFLICT (job_id) DO NOTHING` here and checking whether the
    row was actually inserted. Reprocessing the same job_id after a crash
    must hit the conflict branch, not create a second row.
    """

    __tablename__ = "side_effects"

    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id"), primary_key=True)
    effect_key: Mapped[str] = mapped_column(String(255), nullable=False)
    result_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    write_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

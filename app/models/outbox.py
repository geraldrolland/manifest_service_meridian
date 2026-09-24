"""SQLModel table and enums for the transactional outbox."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Column
from sqlmodel import SQLModel, Field


class OutboxStatus(str, Enum):
    """Outbox event state."""

    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class Outbox(SQLModel, table=True):
    """Transaction outbox for reliable event publishing."""

    __tablename__ = "outbox"

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
    )
    topic: str = Field(max_length=255)
    payload: dict = Field(default={}, sa_column=Column(JSON, nullable=True))
    status: str = Field(default=OutboxStatus.PENDING.value, max_length=32)
    retry_after: datetime | None = Field(default=None, nullable=True)
    num_of_retry: int = Field(default=0)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
"""SQLModel table and enums for manifest tasks."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Column
from sqlmodel import SQLModel, Field


class ManifestStatus(str, Enum):
    """Manifest processing state."""

    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ManifestTask(SQLModel, table=True):
    """SQLModel table for manifest tasks."""

    __tablename__ = "manifest_tasks"

    id: str = Field(
        default_factory=lambda: f"manifest:{uuid.uuid4().hex}",
        primary_key=True,
        max_length=256,
    )
    video_id: str = Field(max_length=255)
    job_id: str = Field(max_length=255)
    status: str = Field(default=ManifestStatus.PENDING.value, max_length=16)
    task_metadata: dict = Field(
        default={},
        sa_column=Column("metadata", JSON, nullable=True),
    )
    published: bool = Field(default=False)
    manifest_url: str | None = Field(default=None, max_length=1024, nullable=True)
    num_of_retries: int = Field(default=0)
    retry_after: datetime | None = Field(default=None, nullable=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
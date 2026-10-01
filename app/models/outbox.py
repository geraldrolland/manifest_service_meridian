"""SQLModel table and enums for the transactional outbox."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Column, ForeignKeyConstraint
from sqlmodel import SQLModel, Field


class OutboxStatus(str, Enum):
    """Outbox event state."""

    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class Outbox(SQLModel, table=True):
    """Transaction outbox for reliable event publishing."""

    __tablename__ = "outbox"

    # Ties outbox rows to their manifest so cleanup_manifest (video.deleted)
    # removes both the manifest row and its unpublishable events together.
    # Existing DBs:
    #   ALTER TABLE outbox ADD COLUMN IF NOT EXISTS manifest_id VARCHAR(256);
    #   ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_manifest_id_fkey;
    #   ALTER TABLE outbox ADD CONSTRAINT outbox_manifest_id_fkey
    #     FOREIGN KEY (manifest_id) REFERENCES manifest_tasks(id) ON DELETE CASCADE;
    __table_args__ = (
        ForeignKeyConstraint(
            ["manifest_id"],
            ["manifest_tasks.id"],
            name="outbox_manifest_id_fkey",
            ondelete="CASCADE",
        ),
    )

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
    )
    topic: str = Field(max_length=255)
    payload: dict = Field(default={}, sa_column=Column(JSON, nullable=True))
    status: str = Field(default=OutboxStatus.PENDING.value, max_length=32)
    manifest_id: str | None = Field(default=None, max_length=256, nullable=True)
    retry_after: datetime | None = Field(default=None, nullable=True)
    num_of_retry: int = Field(default=0)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
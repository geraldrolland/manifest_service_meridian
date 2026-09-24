"""Synchronous database engine and session management for Celery tasks.

Provides a synchronous SQLAlchemy engine and session factory used by
Celery workers that cannot use async drivers.
"""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.config import settings

logger = logging.getLogger(__name__)

sync_database_url = settings.database_url.replace("+asyncpg", "")

sync_engine = create_engine(
    sync_database_url,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
)

SyncSessionLocal = sessionmaker(bind=sync_engine)


def get_sync_session() -> Session:
    """Create a new synchronous database session for Celery tasks."""
    return SyncSessionLocal()
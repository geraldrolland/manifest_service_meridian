"""Async database engine and session management for the manifest service.

Provides a SQLAlchemy async engine, session factory, and FastAPI dependency
for request-scoped database sessions.
"""

import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlmodel import SQLModel

from app.config import settings

logger = logging.getLogger(__name__)

engine = create_async_engine(
    settings.db_dsn,
    echo=False,
    future=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Create all tables defined by SQLModel."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Database tables created")


async def close_db() -> None:
    """Dispose of the engine connection pool."""
    await engine.dispose()
    logger.info("Database connections disposed")


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an async session."""
    async with async_session_factory() as session:
        yield session
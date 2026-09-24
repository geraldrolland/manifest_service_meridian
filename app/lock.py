"""Redis distributed lock utility for the manifest service.

Provides a dual-lock mechanism for processing and committing events.
"""

import logging
from enum import Enum

import redis

from app.config import settings

logger = logging.getLogger(__name__)


class LockState(str, Enum):
    """Lock state prefix used in generating the lock key."""

    PROCESSING = "PROCESSING"
    COMMITTING = "COMMITTING"


redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)

LOCK_TTL = 120  # 2 minutes
LOCK_BLOCKING_TIMEOUT = 5  # seconds


def acquire_lock(state: LockState, event_id: str) -> redis.lock.Lock | None:
    """Acquire a distributed lock for an event.

    Args:
        state: The lock state (PROCESSING or COMMITTING) used as key prefix.
        event_id: The event ID from the Kafka message.

    Returns:
        The lock object if acquired, None if already locked by another worker.
    """
    lock = redis_client.lock(
        name=f"{state.value}:{event_id}",
        timeout=LOCK_TTL,
        blocking_timeout=LOCK_BLOCKING_TIMEOUT,
    )
    acquired = lock.acquire(blocking=True)
    if acquired:
        return lock
    return None


def release_lock(lock: redis.lock.Lock) -> None:
    """Release a distributed lock."""
    try:
        lock.release()
    except redis.exceptions.LockNotOwnedError:
        logger.warning("Lock already expired or released")
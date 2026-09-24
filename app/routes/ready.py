"""Readiness check route."""

import logging

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Response
from kombu import Connection
from sqlalchemy import text

from app.config import settings
from app.db_config import async_session_factory
from app.lock import redis_client
from app.minio_client import client as minio_client

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/ready")
async def ready(response: Response):
    """Check Redis, DB, MinIO, RabbitMQ, and Kafka connectivity.

    Returns 200 with status "ok" if all are reachable, 500 with status "not_ok" otherwise.
    """
    checks: dict[str, str] = {}

    try:
        redis_client.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        logger.warning("Readiness check: Redis unreachable: %s", exc)
        checks["redis"] = "not_ok"

    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:
        logger.warning("Readiness check: DB unreachable: %s", exc)
        checks["db"] = "not_ok"

    try:
        minio_client.bucket_exists("manifest")
        checks["minio"] = "ok"
    except Exception as exc:
        logger.warning("Readiness check: MinIO unreachable: %s", exc)
        checks["minio"] = "not_ok"

    try:
        with Connection(settings.celery_broker_url) as conn:
            conn.connect()
        checks["rabbitmq"] = "ok"
    except Exception as exc:
        logger.warning("Readiness check: RabbitMQ unreachable: %s", exc)
        checks["rabbitmq"] = "not_ok"

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await producer.start()
        checks["kafka"] = "ok"
    except Exception as exc:
        logger.warning("Readiness check: Kafka unreachable: %s", exc)
        checks["kafka"] = "not_ok"
    finally:
        try:
            await producer.stop()
        except Exception:
            pass

    all_ok = all(v == "ok" for v in checks.values())
    if not all_ok:
        response.status_code = 500
    return {"status": "ok" if all_ok else "not_ok", "checks": checks}

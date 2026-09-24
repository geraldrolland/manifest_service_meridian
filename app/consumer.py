"""Kafka consumer for the manifest service.

Subscribes to the job.completed topic and persists ManifestTask records
along with a manifest.generating outbox event.
"""

import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRebalanceListener
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db_config import async_session_factory
from app.models.manifest_task import ManifestTask
from app.models.outbox import Outbox

logger = logging.getLogger(__name__)


class AppRebalanceListener(ConsumerRebalanceListener):
    """Logs Kafka partition rebalance events for the manifest consumer group."""

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        logger.info("Partitions revoked: %s", revoked)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info("Partitions assigned: %s", assigned)


async def consume_messages(consumer: AIOKafkaConsumer) -> None:
    """Main consumer loop. Runs until cancelled."""
    try:
        async for msg in consumer:
            session = None
            try:
                raw_value: bytes = msg.value
                if raw_value is None:
                    logger.warning("Received empty message at offset %d", msg.offset)
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                event_dict: dict[str, Any] = json.loads(raw_value.decode("utf-8"))
                event_id = event_dict.get("event_id")
                video_id = event_dict.get("video_id")
                job_id = event_dict.get("job_id")
                thumbnail_url = event_dict.get("thumbnail_url")
                manifest_metadata = event_dict.get("manifest_metadata")
                origin_service = event_dict.get("origin_service")

                if not event_id or not video_id or not job_id:
                    logger.warning(
                        "Missing required fields (event_id, video_id, job_id), skipping"
                    )
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                manifest_task = ManifestTask(
                    id=f"manifest:{event_id}",
                    video_id=video_id,
                    job_id=job_id,
                    task_metadata=manifest_metadata or {},
                )

                outbox = Outbox(
                    topic="manifest.generating",
                    payload={
                        "origin_service": origin_service,
                        "video_id": video_id,
                        "thumbnail_url": thumbnail_url,
                    },
                )

                session = async_session_factory()
                session.add(manifest_task)
                session.add(outbox)
                await session.commit()
                await session.close()

                logger.info(
                    "ManifestTask committed id=manifest:%s video_id=%s job_id=%s",
                    event_id,
                    video_id,
                    job_id,
                )

                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except IntegrityError:
                logger.warning(
                    "IntegrityError for offset %d, rolling back and committing offset",
                    msg.offset,
                )
                if session is not None:
                    await session.rollback()
                    await session.close()
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except json.JSONDecodeError as e:
                logger.error("Failed to decode message JSON: %s", e)
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=True)
                if session is not None:
                    await session.rollback()
                    await session.close()
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    finally:
        await consumer.stop()
        logger.info("Consumer stopped")


async def start_consumer() -> None:
    """Create and start the Kafka consumer."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=False,
        session_timeout_ms=30000,
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=60000,
    )

    await consumer.start()
    consumer.subscribe([settings.kafka_topic], listener=AppRebalanceListener())
    logger.info(
        "Kafka consumer started — topic=%s group=%s",
        settings.kafka_topic,
        settings.kafka_consumer_group_id,
    )

    await consume_messages(consumer)
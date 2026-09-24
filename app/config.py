"""Centralized configuration for the manifest service.

All settings are loaded from environment variables via pydantic-settings.
Falls back to sensible defaults for local development.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings sourced from environment variables.

    Attributes:
        kafka_bootstrap_servers: Comma-separated Kafka broker addresses.
        kafka_topic: Topic for job completed events.
        kafka_consumer_group_id: Consumer group ID for the Kafka consumer.
        kafka_auto_offset_reset: Where to start reading when no committed offset exists.
        database_url: SQLAlchemy async database connection string.
        redis_host: Redis host for distributed locking.
        redis_port: Redis port.
        redis_db: Redis database number.
        minio_endpoint: MinIO (S3-compatible) endpoint address.
        minio_access_key: MinIO access key.
        minio_secret_key: MinIO secret key.
        minio_secure: Whether to use HTTPS for MinIO connections.
        celery_broker_url: RabbitMQ broker URL for Celery task dispatch.
        celery_result_backend: Redis URL for Celery task storage.
        log_level: Python logging level.
    """

    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "job.completed"
    kafka_consumer_group_id: str = "meridian-manifest-consumer-group"
    kafka_auto_offset_reset: str = "earliest"
    database_url: str = "postgresql+asyncpg://postgres:postgres@manifest-db:5432/manifest_db"
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 4
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    celery_broker_url: str = "amqp://guest:guest@rabbitmq:5672//"
    celery_result_backend: str = "redis://redis:6379/1"
    log_level: str = "info"

    @property
    def db_dsn(self) -> str:
        return self.database_url

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
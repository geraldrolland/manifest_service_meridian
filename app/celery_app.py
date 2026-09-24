"""Celery application for the manifest service.

Configured with RabbitMQ as the broker and Redis as the result backend.
"""

from celery import Celery

from app.config import settings

celery_app = Celery(
    "manifest_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    result_expires=3600,
    worker_prefetch_multiplier=4,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=False,
    task_routes={
        "app.tasks.process_outbox_task": {"queue": "manifest"},
        "app.tasks.process_manifest_task": {"queue": "manifest"},
        "app.tasks.process_failed_manifest_tasks": {"queue": "manifest"},
        "app.tasks.process_completed_manifest_task": {"queue": "manifest"},
    },
)

celery_app.conf.beat_schedule = {
    "process-outbox-every-10-seconds": {
        "task": "app.tasks.process_outbox_task",
        "schedule": 10.0,
        "options": {"queue": "manifest"},
    },
    "process-manifest-every-15-seconds": {
        "task": "app.tasks.process_manifest_task",
        "schedule": 15.0,
        "options": {"queue": "manifest"},
    },
    "process-failed-manifest-every-15-seconds": {
        "task": "app.tasks.process_failed_manifest_tasks",
        "schedule": 15.0,
        "options": {"queue": "manifest"},
    },
    "process-completed-manifest-every-15-seconds": {
        "task": "app.tasks.process_completed_manifest_task",
        "schedule": 15.0,
        "options": {"queue": "manifest"},
    },
}

celery_app.autodiscover_tasks(["app"])
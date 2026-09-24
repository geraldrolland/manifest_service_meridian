# Manifest Service — MERIDIAN

Production-grade DASH manifest generation microservice for the MERIDIAN video platform. Consumes `job.completed` events from Kafka, builds static MPEG-DASH (`.mpd`) manifests for multi-rendition CMAF segments, uploads them to MinIO, and publishes `manifest.completed` via the transactional outbox pattern so the Video Service can mark videos `COMPLETED` and expose `manifest_url`.

## Architecture

```
┌────────────────┐     ┌─────────┐     ┌──────────────────────┐
│  job.completed │────▶│  Kafka  │────▶│  FastAPI + Consumer  │
│     (topic)    │     │         │     │     (port 8002)      │
└────────────────┘     └─────────┘     └──────────┬───────────┘
                                                  │
                                      ┌───────────▼───────────┐
                                      │     Celery Worker      │
                                      │   (4 periodic tasks)   │
                                      └───────────┬───────────┘
                                                  │
                        ┌─────────────────────────┼─────────────────────────┐
                        │                         │                         │
                ┌───────▼───────┐         ┌───────▼───────┐         ┌───────▼───────┐
                │  PostgreSQL   │         │     Redis     │         │     MinIO     │
                │ (manifest_    │         │ (distributed  │         │  (manifest    │
                │  tasks, outbox)│         │    locks)     │         │   bucket)     │
                └───────────────┘         └───────────────┘         └───────────────┘
```

## Processing Pipeline

```
job.completed event (media_processing_service)
    │
    ▼
┌──────────────────────────────────────┐
│ 1. Kafka Consumer                    │
│    Persist ManifestTask (PENDING)    │
│    + Outbox manifest.generating      │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 2. process_manifest_task (every 15s) │
│    PROCESSING lock                   │
│    Generate DASH MPD (.mpd)          │
│    Upload to MinIO manifest bucket   │
│    COMMITTING lock → status=COMPLETED│
│    + manifest_url                    │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 3. process_completed_manifest_task   │
│    (every 15s)                       │
│    Cleanup temp files                │
│    Outbox manifest.completed event   │
│    published=true                    │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 4. process_outbox_task (every 10s)   │
│    Publish PENDING outbox → Kafka    │
└──────────────────────────────────────┘
```

Downstream, the Video Service `manifest.completed` consumer sets `videos.status = COMPLETED` and `videos.manifest_url`, then clients can fetch the MPD and segments for playback.

## Celery Beat Schedule

| Task | Schedule | Description |
|------|----------|-------------|
| `process_outbox_task` | Every 10s | Publish pending outbox events to Kafka |
| `process_manifest_task` | Every 15s | Generate DASH MPD for PENDING tasks, upload to MinIO |
| `process_failed_manifest_tasks` | Every 15s | Cleanup / publish `manifest.failed` |
| `process_completed_manifest_task` | Every 15s | Cleanup temp files, publish `manifest.completed` |

## Kafka Topics

| Topic | Role | Purpose |
|-------|------|---------|
| `job.completed` | Consume | Media processing finished; triggers manifest task |
| `manifest.generating` | Produce (via outbox) | Manifest generation started |
| `manifest.completed` | Produce (via outbox) | Manifest ready; Video Service sets COMPLETED + `manifest_url` |
| `manifest.failed` | Produce (via outbox) | Terminal manifest failure |

## Database Schema

### manifest_tasks

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(256) PK | `manifest:{uuid}` |
| `video_id` | VARCHAR(255) | Source video identifier |
| `job_id` | VARCHAR(255) | Parent media-processing job |
| `status` | VARCHAR(16) | PENDING → COMPLETED / FAILED |
| `metadata` | JSON | Renditions, duration, media prefix, etc. |
| `published` | BOOLEAN | Whether outbox event was published |
| `manifest_url` | VARCHAR(1024) | MinIO URL of the generated `.mpd` |
| `num_of_retries` | INT | Retry counter |
| `retry_after` | TIMESTAMP | Next retry window |
| `created_at` | TIMESTAMP | Creation time (UTC) |

### outbox

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(36) PK | UUID |
| `topic` | VARCHAR(255) | Kafka topic |
| `payload` | JSON | Event payload |
| `status` | VARCHAR(32) | PENDING → PROCESSED / FAILED |
| `retry_count` | INT | Publish retry counter |

## Distributed Locking

Dual Redis locks with a nested pattern:

- **PROCESSING lock** — guards MPD generation and MinIO upload
- **COMMITTING lock** — guards database status/URL writes

Both use a 2-minute TTL with a short blocking timeout and are always released in `finally` blocks.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe (always 200) |
| `GET` | `/ready` | Readiness: Redis, DB, MinIO, RabbitMQ, Kafka |

### `/ready` contract

```json
{
  "status": "ok",
  "checks": {
    "redis": "ok",
    "db": "ok",
    "minio": "ok",
    "rabbitmq": "ok",
    "kafka": "ok"
  }
}
```

- **200** when every check is `"ok"`
- **500** with `"status": "not_ok"` when any dependency fails (failed keys are `"not_ok"`)

## Configuration

Settings load from environment variables / `.env` via `pydantic-settings`.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka brokers |
| `KAFKA_TOPIC` | `job.completed` | Incoming topic |
| `KAFKA_CONSUMER_GROUP_ID` | `meridian-manifest-consumer-group` | Consumer group |
| `DATABASE_URL` | `postgresql+asyncpg://...manifest_db` | PostgreSQL DSN |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `redis` / `6379` / `4` | Distributed locks |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO address |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `minioadmin` | MinIO credentials |
| `MINIO_SECURE` | `false` | Use HTTPS |
| `CELERY_BROKER_URL` | `amqp://guest:guest@rabbitmq:5672//` | RabbitMQ broker |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/1` | Celery results |
| `LOG_LEVEL` | `info` | Logging level |

## Project Structure

```
manifest_service/
├── app/
│   ├── main.py                 # FastAPI app + Kafka consumer lifespan
│   ├── consumer.py             # job.completed → ManifestTask + outbox
│   ├── producer.py             # Sync Kafka producer for outbox publish
│   ├── celery_app.py           # Celery config + beat schedule
│   ├── config.py               # Pydantic settings
│   ├── lock.py                 # Redis PROCESSING/COMMITTING locks
│   ├── utils.py                # build_object_url, resolve_object_key, cleanup
│   ├── db_config/
│   │   ├── database.py         # Async engine (API/consumer)
│   │   └── database_sync.py    # Sync engine (Celery)
│   ├── generating_manifest/
│   │   └── __init__.py         # Static DASH MPD builder
│   ├── minio_client/
│   │   └── __init__.py         # MinIO client helpers
│   ├── models/
│   │   ├── manifest_task.py    # ManifestTask + ManifestStatus
│   │   └── outbox.py           # Transactional outbox
│   ├── routes/
│   │   ├── health.py           # GET /health
│   │   └── ready.py            # GET /ready (unified readiness)
│   └── tasks/
│       ├── process_manifest_task.py
│       ├── process_outbox_task.py
│       ├── process_failed_manifest_tasks.py
│       └── process_completed_manifest_task.py
├── tests/                      # pytest suite (ready, tasks, consumer, MPD, utils)
├── Dockerfile                  # FastAPI + Kafka consumer
├── Dockerfile.celery           # Celery worker (solo pool) + beat
├── requirements.txt
└── start.sh                    # Container entrypoint (uvicorn)
```

## Docker

```bash
# From MERIDIAN monorepo root
docker compose up --build manifest-service manifest-celery-worker manifest-celery-beat

# Standalone web + consumer
docker build -t manifest-service .
docker run -p 8002:8002 --env-file .env manifest-service

# Celery worker
docker build -f Dockerfile.celery -t manifest-celery .
docker run --env-file .env manifest-celery
```

## Local Development

```bash
cd manifest_service
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS/Linux
pip install -r requirements.txt

uvicorn app.main:app --reload --host 0.0.0.0 --port 8002

celery -A app.celery_app:celery_app worker --loglevel=info --pool=solo -Q manifest
celery -A app.celery_app:celery_app beat --loglevel=info
```

## Testing

```bash
cd manifest_service
python -m pytest tests/ -v
```

| Suite | Coverage |
|-------|----------|
| `test_ready.py` | Unified `/ready` shape + per-dependency failure cases |
| `test_tasks.py` | Manifest generation, outbox publish, failed/completed publishers, locks, retries |
| `test_consumer.py` | Kafka consumer persistence + outbox events |
| `test_generating_manifest.py` | DASH MPD XML structure |
| `test_utils.py` | Object URL/key helpers, temp cleanup |
| `test_smoke.py` | Imports and router wiring |

Latest local run: **86 passed**.

## License

Private — MERIDIAN Project

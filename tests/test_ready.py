from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import Response


def _make_async_session(execute_side_effect=None):
    mock_session = AsyncMock()
    if execute_side_effect:
        mock_session.execute.side_effect = execute_side_effect
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=mock_cm)


def _make_kafka(start_side_effect=None):
    producer = AsyncMock()
    if start_side_effect:
        producer.start.side_effect = start_side_effect
    return producer


def _run_ready(
    *,
    redis_ping_side_effect=None,
    db_execute_side_effect=None,
    minio_bucket_side_effect=None,
    rabbitmq_side_effect=None,
    kafka_start_side_effect=None,
):
    import asyncio

    from app.routes import ready as ready_mod

    mock_redis = MagicMock()
    if redis_ping_side_effect:
        mock_redis.ping.side_effect = redis_ping_side_effect
    else:
        mock_redis.ping.return_value = True

    factory = _make_async_session(execute_side_effect=db_execute_side_effect)

    mock_minio = MagicMock()
    if minio_bucket_side_effect:
        mock_minio.bucket_exists.side_effect = minio_bucket_side_effect
    else:
        mock_minio.bucket_exists.return_value = True

    mock_kombu_conn = MagicMock()
    mock_kombu_conn.__enter__ = MagicMock(return_value=mock_kombu_conn)
    mock_kombu_conn.__exit__ = MagicMock(return_value=False)
    if rabbitmq_side_effect:
        mock_kombu_conn.connect.side_effect = rabbitmq_side_effect
    else:
        mock_kombu_conn.connect.return_value = True

    mock_kafka = _make_kafka(start_side_effect=kafka_start_side_effect)

    with patch.object(ready_mod, "redis_client", mock_redis), \
         patch.object(ready_mod, "async_session_factory", factory), \
         patch.object(ready_mod, "minio_client", mock_minio), \
         patch.object(ready_mod, "Connection", MagicMock(return_value=mock_kombu_conn)), \
         patch.object(ready_mod, "AIOKafkaProducer", MagicMock(return_value=mock_kafka)):
        response = Response()
        result = asyncio.run(ready_mod.ready(response=response))
    return response, result


class TestReadyEndpoint:
    def test_returns_200_when_all_healthy(self):
        response, result = _run_ready()

        assert response.status_code == 200
        assert result == {
            "status": "ok",
            "checks": {
                "redis": "ok",
                "db": "ok",
                "minio": "ok",
                "rabbitmq": "ok",
                "kafka": "ok",
            },
        }

    def test_returns_500_when_redis_down(self):
        response, result = _run_ready(redis_ping_side_effect=Exception("Connection refused"))

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"]["redis"] == "not_ok"
        assert result["checks"]["db"] == "ok"

    def test_returns_500_when_db_down(self):
        response, result = _run_ready(db_execute_side_effect=Exception("Connection refused"))

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"]["db"] == "not_ok"

    def test_returns_500_when_minio_down(self):
        response, result = _run_ready(minio_bucket_side_effect=Exception("Connection refused"))

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"]["minio"] == "not_ok"

    def test_returns_500_when_rabbitmq_down(self):
        response, result = _run_ready(rabbitmq_side_effect=Exception("Connection refused"))

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"]["rabbitmq"] == "not_ok"

    def test_returns_500_when_kafka_down(self):
        response, result = _run_ready(kafka_start_side_effect=Exception("Connection refused"))

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"]["kafka"] == "not_ok"

    def test_returns_500_when_all_down(self):
        response, result = _run_ready(
            redis_ping_side_effect=Exception("Connection refused"),
            db_execute_side_effect=Exception("Connection refused"),
            minio_bucket_side_effect=Exception("Connection refused"),
            rabbitmq_side_effect=Exception("Connection refused"),
            kafka_start_side_effect=Exception("Connection refused"),
        )

        assert response.status_code == 500
        assert result["status"] == "not_ok"
        assert result["checks"] == {
            "redis": "not_ok",
            "db": "not_ok",
            "minio": "not_ok",
            "rabbitmq": "not_ok",
            "kafka": "not_ok",
        }

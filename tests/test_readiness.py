"""El health debe reflejar el estado real de Ollama, Kafka y Redis."""
import asyncio
from types import SimpleNamespace

from fastapi import Response

from routes import health


class FakeLlm:
    def __init__(self, available):
        self.available = available

    async def is_available(self):
        return self.available


class FakeCache:
    def __init__(self, available):
        self.available = available

    async def ping(self):
        return self.available


def check_health(*, ollama=True, kafka=True, redis=True):
    response = Response()
    body = asyncio.run(health(
        response=response,
        llm_client=FakeLlm(ollama),
        summary_consumer=SimpleNamespace(is_connected=kafka),
        summary_cache=FakeCache(redis),
    ))
    return response.status_code, body


def test_healthy_requires_all_dependencies():
    status, body = check_health()
    assert status == 200
    assert body["status"] == "healthy"
    assert body["ollama"] == "reachable"
    assert body["kafka"] == "connected"
    assert body["redis"] == "ok"


def test_unavailable_kafka_is_not_fake_200():
    status, body = check_health(kafka=False)
    assert status == 503
    assert body["kafka"] == "disconnected"
    assert body["status"] == "unhealthy"


def test_unavailable_redis_is_not_fake_200():
    status, body = check_health(redis=False)
    assert status == 503
    assert body["redis"] == "down"


def test_unavailable_ollama_is_not_fake_200():
    status, body = check_health(ollama=False)
    assert status == 503
    assert body["ollama"] == "unreachable"

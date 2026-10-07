"""Tests del puerto de caché: contrato, Noop y adaptador Redis (fail-open)."""

import pytest

from cache import NoopSummaryCache, RedisSummaryCache, SummaryCache


class InMemoryRedisClient:
    """Doble del cliente redis.asyncio: guarda bytes con su TTL."""

    def __init__(self, data: dict[str, bytes | None] | None = None) -> None:
        self.data = data or {}
        self.set_kwargs: list[tuple[str, str, dict]] = []

    async def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    async def set(self, key: str, value: str, **kwargs) -> bool:
        self.set_kwargs.append((key, value, kwargs))
        self.data[key] = value.encode()
        return True


class BrokenRedisClient:
    """Simula a Redis caído: cada operación lanza un error de conexión."""

    async def get(self, key: str) -> bytes:
        raise ConnectionError("redis caído")

    async def set(self, key: str, value: str, **kwargs) -> bool:
        raise ConnectionError("redis caído")


def test_adapters_cumplen_el_protocolo():
    assert isinstance(NoopSummaryCache(), SummaryCache)
    assert isinstance(
        RedisSummaryCache(
            "redis://fake:6379/0", ttl_seconds=60, enabled=True, client=InMemoryRedisClient()
        ),
        SummaryCache,
    )


@pytest.mark.anyio
async def test_noop_siempre_reporta_miss_y_falla_el_set():
    cache = NoopSummaryCache()

    assert await cache.get("summary:doc-1") is None
    assert await cache.set("summary:doc-1", '{"status":"done"}') is False


@pytest.mark.anyio
async def test_redis_cache_guarda_y_devuelve_con_ttl():
    client = InMemoryRedisClient()
    cache = RedisSummaryCache(
        "redis://fake:6379/0", ttl_seconds=300, enabled=True, client=client
    )

    assert await cache.set("summary:doc-1", '{"status":"done"}') is True
    assert await cache.get("summary:doc-1") == '{"status":"done"}'
    assert client.set_kwargs[0][2] == {"ex": 300}


@pytest.mark.anyio
async def test_redis_cache_get_con_redis_caido_reporta_miss(caplog):
    cache = RedisSummaryCache(
        "redis://fake:6379/0", ttl_seconds=300, enabled=True, client=BrokenRedisClient()
    )

    assert await cache.get("summary:doc-1") is None
    assert "Redis" in caplog.text


@pytest.mark.anyio
async def test_redis_cache_set_con_redis_caido_degrada_a_sincrono(caplog):
    cache = RedisSummaryCache(
        "redis://fake:6379/0", ttl_seconds=300, enabled=True, client=BrokenRedisClient()
    )

    assert await cache.set("summary:doc-1", '{"status":"queued"}') is False
    assert "Redis" in caplog.text


@pytest.mark.anyio
async def test_cache_deshabilitada_se_comporta_como_noop():
    client = InMemoryRedisClient()
    cache = RedisSummaryCache(
        "redis://fake:6379/0", ttl_seconds=300, enabled=False, client=client
    )

    assert await cache.get("summary:doc-1") is None
    assert await cache.set("summary:doc-1", '{"status":"queued"}') is False
    assert client.set_kwargs == []

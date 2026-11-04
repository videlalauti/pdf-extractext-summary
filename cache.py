"""Puerto de caché de resúmenes y sus adaptadores: Redis y Noop.

Mismo patrón que :mod:`llm` (Protocol + adaptador): los servicios dependen de
:class:`SummaryCache`, los tests inyectan dobles y el adaptador Redis cierra
sobre una sola clave TTL por documento (estado + resultado), que además sirve
de caché del resumen.
"""

import logging
from typing import Protocol, runtime_checkable

import redis.asyncio as aioredis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


@runtime_checkable
class SummaryCache(Protocol):
    """Estado y resultado del resumen de un documento, clave única por documento.

    `get` devuelve None como miss (nada guardado o Redis caído; fail-open).
    `set` devuelve False cuando el valor no se pudo guardar (caché deshabilitada
    o Redis caído): sin estado persistible no se infiere, para no perder el
    resultado ni duplicar intentos.
    `ping` distingue un miss real de una caída: el consumer lo usa para no
    quemar los 3 intentos de entrega mientras Redis no puede guardar nada.
    """

    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, *, ttl_seconds: int | None = None) -> bool: ...

    async def ping(self) -> bool: ...

    async def scan_keys(self, prefix: str) -> list[str]: ...


class NoopSummaryCache:
    """Caché que no guarda nada; el flujo asíncrono no puede persistir estado.

    No es un modo degradado: los endpoints responden 503 y el consumer no se
    arranca cuando la caché está deshabilitada por configuración.
    """

    async def get(self, key: str) -> str | None:
        return None

    async def set(self, key: str, value: str, *, ttl_seconds: int | None = None) -> bool:
        return False

    async def ping(self) -> bool:
        return False

    async def scan_keys(self, prefix: str) -> list[str]:
        return []


class RedisSummaryCache:
    """Adaptador sobre Redis con claves con TTL y fail-open.

    Fail-open: si Redis no responde, `get` se reporta como miss y `set` devuelve
    False (el flujo degrada a síncrono), en vez de provocar un 5xx por la caché.
    """

    def __init__(
        self,
        url: str,
        *,
        ttl_seconds: int,
        enabled: bool,
        client: aioredis.Redis | None = None,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._enabled = enabled
        self._owns_client = client is None
        self._client = client or aioredis.from_url(url)

    async def get(self, key: str) -> str | None:
        if not self._enabled:
            return None
        try:
            raw = await self._client.get(key)
        except (RedisError, OSError) as exc:
            logger.warning("Redis no disponible al leer %s: %s", key, exc)
            return None
        return raw.decode("utf-8") if raw is not None else None

    async def set(
        self, key: str, value: str, *, ttl_seconds: int | None = None
    ) -> bool:
        if not self._enabled:
            return False
        try:
            await self._client.set(
                key,
                value,
                ex=self._ttl_seconds if ttl_seconds is None else ttl_seconds,
            )
        except (RedisError, OSError) as exc:
            logger.warning("Redis no disponible al escribir %s: %s", key, exc)
            return False
        return True

    async def ping(self) -> bool:
        """True si Redis responde; False si está deshabilitado o caído."""
        if not self._enabled:
            return False
        try:
            return bool(await self._client.ping())
        except (RedisError, OSError) as exc:
            logger.warning("Redis no disponible al hacer ping: %s", exc)
            return False

    async def scan_keys(self, prefix: str) -> list[str]:
        """Claves que empiezan con ``prefix`` (para reconciliar al arrancar).

        Fail-open: ante un error devuelve [] y el startup sigue; el consumer
        mantiene su guard de caché para los mensajes.
        """
        if not self._enabled:
            return []
        try:
            keys: list[str] = []
            async for key in self._client.scan_iter(match=f"{prefix}*"):
                keys.append(key.decode("utf-8") if isinstance(key, bytes) else str(key))
            return keys
        except (RedisError, OSError) as exc:
            logger.warning("Redis no disponible al escanear %s: %s", prefix, exc)
            return []

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

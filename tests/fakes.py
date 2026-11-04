"""Doubles usados por los tests: implementan los puertos sin tocar librerías."""

import asyncio

import httpx

DEFAULT_CONTENT = "texto de prueba"


class FakeLlmClient:
    """Implementación en memoria de :class:`LlmClient` que registra los prompts."""

    def __init__(
        self,
        summary: str = "resumen de prueba",
        *,
        available: bool = True,
        error: Exception | None = None,
        delay: float | None = None,
    ) -> None:
        self.summary = summary
        self.available = available
        self.error = error
        self.delay = delay
        self.prompts: list[str] = []
        self.models: list[str] = []

    async def generate(self, model: str, prompt: str) -> str:
        self.prompts.append(prompt)
        self.models.append(model)
        # El delay aplica también al error: permite simular un timeout que tardó N segundos.
        if self.delay is not None:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.summary

    async def is_available(self) -> bool:
        return self.available


class FakeSummaryCache:
    """Implementación en memoria de :class:`SummaryCache` que registra escrituras.

    Con `fail_open=True` simula a Redis caído: `get` es miss, `set` devuelve
    False y `ping` devuelve False.
    """

    def __init__(self, *, fail_open: bool = False) -> None:
        self.fail_open = fail_open
        self.data: dict[str, str] = {}
        self.sets: list[tuple[str, str]] = []
        self.set_calls: list[tuple[str, str, int | None]] = []

    async def get(self, key: str) -> str | None:
        if self.fail_open:
            return None
        return self.data.get(key)

    async def set(
        self, key: str, value: str, *, ttl_seconds: int | None = None
    ) -> bool:
        if self.fail_open:
            return False
        self.sets.append((key, value))
        self.set_calls.append((key, value, ttl_seconds))
        self.data[key] = value
        return True

    async def ping(self) -> bool:
        return not self.fail_open

    async def scan_keys(self, prefix: str) -> list[str]:
        if self.fail_open:
            return []
        return [key for key in self.data if key.startswith(prefix)]

    async def aclose(self) -> None:
        return None


class FakeKafkaConsumer:
    """Doble de AIOKafkaConsumer: registra commits y simula fallos de arranque."""

    def __init__(self, messages=(), *, start_errors: list[Exception] | None = None) -> None:
        self.messages = list(messages)
        self.start_errors = list(start_errors or [])
        self.start_attempts = 0
        self.commits = 0
        self.commit_offsets: list[dict] = []
        self.seeks: list[tuple] = []
        self.stopped = False

    async def start(self) -> None:
        self.start_attempts += 1
        if self.start_errors:
            raise self.start_errors.pop(0)

    async def commit(self, offsets=None) -> None:
        self.commits += 1
        if offsets is not None:
            self.commit_offsets.append(offsets)

    def seek(self, tp, offset) -> None:
        self.seeks.append((tp, offset))

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self):
        return self._stream()

    async def _stream(self):
        for message in self.messages:
            yield message


class FakeSummaryConsumer:
    """Doble del consumer para el lifespan: estado de conexión controlable."""

    def __init__(self, *, connected: bool = True) -> None:
        self.connected = connected
        self.started = False
        self.stopped = False

    @property
    def is_connected(self) -> bool:
        return self.connected

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


def document_client(
    content: str = DEFAULT_CONTENT,
    *,
    status_code: int = 200,
    connect_error: bool = False,
) -> httpx.AsyncClient:
    """Cliente de persistence con transporte simulado, sin red ni monkeypatch."""
    document_id = "doc-1"

    def handler(request: httpx.Request) -> httpx.Response:
        if connect_error:
            raise httpx.ConnectError("connection refused", request=request)
        if status_code != 200:
            return httpx.Response(status_code, json={"detail": "documento no encontrado"})
        return httpx.Response(
            200, json={"id": document_id, "content": content, "checksum": "abc"}
        )

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))

"""Doubles usados por los tests: implementan los puertos sin tocar librerías."""

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
    ) -> None:
        self.summary = summary
        self.available = available
        self.error = error
        self.prompts: list[str] = []
        self.models: list[str] = []

    async def generate(self, model: str, prompt: str) -> str:
        self.prompts.append(prompt)
        self.models.append(model)
        if self.error is not None:
            raise self.error
        return self.summary

    async def is_available(self) -> bool:
        return self.available


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

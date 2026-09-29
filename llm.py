"""Puerto de generación de texto y adaptador HTTP para Ollama."""

import logging
from typing import Protocol, runtime_checkable

import httpx

from errors import (
    LlmModelNotFoundError,
    LlmPromptRejectedError,
    LlmTimeoutError,
    LlmUnavailableError,
)

logger = logging.getLogger(__name__)

PROMPT_REJECTED_STATUSES = frozenset({400, 422})
HEALTH_CHECK_TIMEOUT_SECONDS = 3.0


@runtime_checkable
class LlmClient(Protocol):
    """Puerto de salida hacia el proveedor de modelos.

    La aplicación depende de este contrato, no de httpx ni de Ollama, así los
    tests pueden inyectar un doble sin monkeypatchear librerías.
    """

    async def generate(self, model: str, prompt: str) -> str:
        """Devuelve el texto generado para el prompt dado."""

    async def is_available(self) -> bool:
        """Indica si el proveedor responde, para el chequeo de health."""


class OllamaLlmClient:
    """Implementación de :class:`LlmClient` sobre la API HTTP de Ollama."""

    def __init__(
        self,
        base_url: str,
        timeout: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def generate(self, model: str, prompt: str) -> str:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": 300},
        }
        try:
            response = await self._client.post(f"{self._base_url}/api/generate", json=payload)
        except httpx.TimeoutException as exc:
            logger.warning("timeout de Ollama generating con el modelo %s", model)
            raise LlmTimeoutError from exc
        except httpx.RequestError as exc:
            logger.warning("no se pudo conectar con Ollama en %s: %s", self._base_url, type(exc).__name__)
            raise LlmUnavailableError from exc

        self._raise_for_status(response, model)
        return response.json().get("response", "")

    async def is_available(self) -> bool:
        try:
            response = await self._client.get(
                f"{self._base_url}/api/tags", timeout=HEALTH_CHECK_TIMEOUT_SECONDS
            )
        except httpx.RequestError as exc:
            logger.warning("health check de Ollama falló: %s", type(exc).__name__)
            return False
        return response.status_code == 200

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _raise_for_status(self, response: httpx.Response, model: str) -> None:
        status = response.status_code
        if status < 400:
            return
        logger.warning("Ollama respondió %s para el modelo %s", status, model)
        if status == 404:
            raise LlmModelNotFoundError(model)
        if status in PROMPT_REJECTED_STATUSES:
            raise LlmPromptRejectedError
        raise LlmUnavailableError

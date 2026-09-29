"""Summary service: capa de aplicación, orquestación con persistence y Ollama."""

import logging

import httpx
from pydantic import Field
from pydantic_settings import BaseSettings

from errors import (
    DocumentNotFoundError,
    LlmModelNotFoundError,
    LlmPromptRejectedError,
    LlmTimeoutError,
    LlmUnavailableError,
    PersistenceUnavailableError,
)

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = "Resumí en español el siguiente texto:\n\n{content}"
TRUNCATED_SUMMARY_PROMPT = (
    "Resumí en español el siguiente texto. Ojo: el contenido fue truncado a los primeros "
    "{max_chars} caracteres porque el documento excede el límite de contexto del modelo.\n\n"
    "{content}"
)


class Settings(BaseSettings):
    persistence_service_url: str = "http://persistence-service:8000"
    persistence_timeout_seconds: float = 30.0
    ollama_url: str = "http://ollama:11434"
    ollama_model: str = "llama3.2"
    ollama_timeout_seconds: float = 300.0
    max_summary_chars: int = Field(default=12000, gt=0)
    log_level: str = "INFO"


settings = Settings()


def build_prompt(content: str, max_chars: int) -> str:
    """Acota el contenido al límite de contexto y lo aclara en el prompt."""
    if len(content) <= max_chars:
        return SUMMARY_PROMPT.format(content=content)
    logger.info(
        "contenido de %d caracteres truncado a %d para no exceder la ventana de contexto",
        len(content),
        max_chars,
    )
    return TRUNCATED_SUMMARY_PROMPT.format(content=content[:max_chars], max_chars=max_chars)


async def fetch_document(document_id: str) -> dict:
    url = f"{settings.persistence_service_url}/documents/{document_id}"
    try:
        async with httpx.AsyncClient(timeout=settings.persistence_timeout_seconds) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise DocumentNotFoundError from exc
        logger.warning("persistence-service respondió %s", exc.response.status_code)
        raise PersistenceUnavailableError from exc
    except httpx.RequestError as exc:
        logger.warning("no se pudo conectar con persistence-service: %s", type(exc).__name__)
        raise PersistenceUnavailableError from exc


async def call_ollama(text: str) -> str:
    payload = {
        "model": settings.ollama_model,
        "prompt": build_prompt(text, settings.max_summary_chars),
        "stream": False,
        "options": {"num_predict": 300},
    }
    try:
        async with httpx.AsyncClient(timeout=settings.ollama_timeout_seconds) as client:
            response = await client.post(f"{settings.ollama_url}/api/generate", json=payload)
            response.raise_for_status()
            return response.json().get("response", "")
    except httpx.TimeoutException as exc:
        logger.warning("timeout de Ollama generando con el modelo %s", settings.ollama_model)
        raise LlmTimeoutError from exc
    except httpx.HTTPStatusError as exc:
        raise _translate_ollama_status(exc.response.status_code, settings.ollama_model) from exc
    except httpx.RequestError as exc:
        logger.warning("no se pudo conectar con Ollama: %s", type(exc).__name__)
        raise LlmUnavailableError from exc


def _translate_ollama_status(status_code: int, model: str) -> Exception:
    if status_code == 404:
        return LlmModelNotFoundError(model)
    if status_code in (400, 422):
        return LlmPromptRejectedError
    logger.warning("Ollama respondió %s para el modelo %s", status_code, model)
    return LlmUnavailableError


async def summarize_document(document_id: str) -> dict:
    document = await fetch_document(document_id)
    summary = await call_ollama(document.get("content") or "")
    return {"summary": summary, "document_id": document_id}

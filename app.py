"""Summary service: capa de aplicación, orquestación con persistence y Ollama."""

import logging

import httpx
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from errors import (
    DocumentNotFoundError,
    PersistenceUnavailableError,
)
from llm import LlmClient

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


def get_settings() -> Settings:
    return settings


class SummaryResponse(BaseModel):
    summary: str
    document_id: str


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


class SummaryService:
    """Orquesta la obtaining del documento, el recorte y la llamada al LLM."""

    def __init__(
        self,
        document_client: httpx.AsyncClient,
        persistence_url: str,
        llm_client: LlmClient,
        model: str,
        max_summary_chars: int,
    ) -> None:
        self._document_client = document_client
        self._persistence_url = persistence_url.rstrip("/")
        self._llm_client = llm_client
        self._model = model
        self._max_summary_chars = max_summary_chars

    async def summarize(self, document_id: str) -> SummaryResponse:
        document = await self._fetch_document(document_id)
        content = document.get("content") or ""
        prompt = build_prompt(content, self._max_summary_chars)
        summary = await self._llm_client.generate(self._model, prompt)
        return SummaryResponse(summary=summary, document_id=document_id)

    async def _fetch_document(self, document_id: str) -> dict:
        url = f"{self._persistence_url}/documents/{document_id}"
        try:
            response = await self._document_client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise DocumentNotFoundError from exc
            logger.warning("persistence-service respondió %s", exc.response.status_code)
            raise PersistenceUnavailableError from exc
        except httpx.RequestError as exc:
            logger.warning("no se pudo conectar con persistence-service: %s", type(exc).__name__)
            raise PersistenceUnavailableError from exc
        return response.json()

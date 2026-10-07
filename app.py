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
HEAD_TAIL_SUMMARY_PROMPT = (
    "Resumí en español el siguiente texto. Ojo: el contenido fue recortado porque el documento "
    "excede el límite de contexto del modelo: llegan los primeros {half} caracteres y los últimos "
    "{half} caracteres de un total de {max_chars} caracteres.\n\n{content}"
)
OMITTED_CONTENT_MARKER = "[contenido intermedio omitido]"


class Settings(BaseSettings):
    persistence_service_url: str = "http://persistence-service:8000"
    persistence_timeout_seconds: float = 30.0
    ollama_url: str = "http://ollama:11434"
    ollama_model: str = "llama3.2"
    ollama_timeout_seconds: float = 300.0
    # gt=1 porque el recorte es mitad y mitad: con 1, content[-0:] devolvería el documento entero.
    max_summary_chars: int = Field(default=2400, gt=1)
    log_level: str = "INFO"


settings = Settings()


def get_settings() -> Settings:
    return settings


class SummaryResponse(BaseModel):
    summary: str
    document_id: str


def build_prompt(content: str, max_chars: int) -> str:
    """Acota el contenido al límite de contexto con inicio + final y lo aclara en el prompt.

    En CPU el costo dominante es el prefill del prompt, así que si el documento excede el
    presupuesto se conserva la cabeza y la cola (donde está la conclusión) en vez de solo
    los primeros caracteres.
    """
    if len(content) <= max_chars:
        return SUMMARY_PROMPT.format(content=content)
    half = max_chars // 2
    logger.info(
        "contenido de %d caracteres recortado a inicio (%d) + final (%d) para no exceder "
        "la ventana de contexto",
        len(content),
        half,
        half,
    )
    return HEAD_TAIL_SUMMARY_PROMPT.format(
        content=f"{content[:half]}\n\n{OMITTED_CONTENT_MARKER}\n\n{content[-half:]}",
        half=half,
        max_chars=max_chars,
    )


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

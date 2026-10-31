"""Summary service: capa de aplicación, orquestación con persistence y Ollama."""

import json
import logging
import time
from dataclasses import dataclass

import httpx
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from shared.web.logging import request_id_header

from cache import NoopSummaryCache, SummaryCache
from errors import (
    DocumentNotFoundError,
    PersistenceUnavailableError,
)
from llm import LlmClient

logger = logging.getLogger(__name__)

JOB_STATE_QUEUED = "queued"
JOB_STATE_PROCESSING = "processing"
JOB_STATE_DONE = "done"

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
    redis_url: str = "redis://redis:6379/0"
    summary_cache_ttl_seconds: int = Field(default=3600, gt=0)
    summary_cache_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:9092"
    log_level: str = "INFO"


settings = Settings()


def get_settings() -> Settings:
    return settings


class SummaryResponse(BaseModel):
    summary: str
    document_id: str


class SummaryJob(BaseModel):
    """Estado y resultado del resumen, guardado como JSON en una sola clave Redis."""

    status: str
    summary: str | None = None


def build_summary_key(document_id: str) -> str:
    return f"summary:{document_id}"


def serialize_job(status: str, *, summary: str | None = None) -> str:
    job = {"status": status}
    if summary is not None:
        job["summary"] = summary
    return json.dumps(job)


def parse_job(raw: str | None) -> SummaryJob | None:
    """Interpreta lo guardado en la caché; un valor corrupto se trata como miss."""
    if raw is None:
        return None
    try:
        return SummaryJob.model_validate_json(raw)
    except ValueError:
        logger.warning("valor de caché inválido para un resumen; se trata como miss")
        return None


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


@dataclass
class SummaryTimings:
    """Tiempos del job en milisegundos; se escriben aunque el resumen falle.

    El formatter no imprime ``extra``, así que estos valores se embeben en el
    mensaje de log. Se acumulan en un objeto mutable porque en el camino de
    excepción un valor de retorno se perdería y la línea de error saldría en
    ceros.
    """

    fetch_ms: float = 0.0
    ollama_ms: float = 0.0
    total_ms: float = 0.0


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000


class SummaryService:
    """Obtiene el documento, lo recorta y lo resume; deja estado y resultado en la caché.

    El trigger del trabajo es el consumer de Kafka (`documents.extracted`), que
    llama a `process`; los endpoints solo leen el estado con `peek_job` y lo
    marcan como `queued` con `enqueue`.
    """

    def __init__(
        self,
        document_client: httpx.AsyncClient,
        persistence_url: str,
        llm_client: LlmClient,
        model: str,
        max_summary_chars: int,
        cache: SummaryCache | None = None,
    ) -> None:
        self._document_client = document_client
        self._persistence_url = persistence_url.rstrip("/")
        self._llm_client = llm_client
        self._model = model
        self._max_summary_chars = max_summary_chars
        self._cache = cache if cache is not None else NoopSummaryCache()

    async def peek_job(self, document_id: str) -> SummaryJob | None:
        """Devuelve el job guardado en la caché (estado + resultado) o None si es miss."""
        return parse_job(await self._cache.get(build_summary_key(document_id)))

    async def enqueue(self, document_id: str) -> bool:
        """Marca el job como encolado. False = no se pudo guardar (Redis caído)."""
        return await self._cache.set(
            build_summary_key(document_id), serialize_job(JOB_STATE_QUEUED)
        )

    async def process(self, document_id: str) -> bool:
        """Resumen un documento: processing → resumen → done + resultado en la caché.

        Devuelve True solo si el resultado quedó guardado; si algo falla se
        loguea (con los tiempos medidos hasta la excepción) y devuelve False
        (el estado queda en processing hasta que expire el TTL), sin propagar:
        quien decide el reintento es el consumer, que confirma el offset
        únicamente con True (at-least-once).
        """
        key = build_summary_key(document_id)
        await self._cache.set(key, serialize_job(JOB_STATE_PROCESSING))
        timings = SummaryTimings()
        try:
            summary = await self._generate_summary(document_id, timings)
        except Exception as error:
            # Una sola línea por job: métricas primero y traceback adjunto al
            # mismo record (exc_info), nunca el mensaje del error (filtra URL).
            logger.error(
                "resumen fallido document_id=%s fetch_ms=%.2f ollama_ms=%.2f total_ms=%.2f "
                "error=%s",
                document_id,
                timings.fetch_ms,
                timings.ollama_ms,
                timings.total_ms,
                type(error).__name__,
                exc_info=error,
            )
            return False
        logger.info(
            "resumen completado document_id=%s fetch_ms=%.2f ollama_ms=%.2f total_ms=%.2f",
            document_id,
            timings.fetch_ms,
            timings.ollama_ms,
            timings.total_ms,
        )
        return await self._cache.set(key, serialize_job(JOB_STATE_DONE, summary=summary))

    async def _generate_summary(self, document_id: str, timings: SummaryTimings) -> str:
        """Genera el resumen midiendo fetch y Ollama; total = la generación completa.

        Los ``try/finally`` dejan escrito el tiempo transcurrido aunque la
        excepción vuele: un timeout de Ollama reporta ``ollama_ms`` real y un
        fallo de persistence reporta ``fetch_ms`` con ``ollama_ms`` en cero.
        """
        started_at = time.perf_counter()
        try:
            fetch_started_at = time.perf_counter()
            try:
                document = await self._fetch_document(document_id)
            finally:
                timings.fetch_ms = _elapsed_ms(fetch_started_at)
            content = document.get("content") or ""
            prompt = build_prompt(content, self._max_summary_chars)
            ollama_started_at = time.perf_counter()
            try:
                return await self._llm_client.generate(self._model, prompt)
            finally:
                timings.ollama_ms = _elapsed_ms(ollama_started_at)
        finally:
            timings.total_ms = _elapsed_ms(started_at)

    async def _fetch_document(self, document_id: str) -> dict:
        url = f"{self._persistence_url}/documents/{document_id}"
        try:
            response = await self._document_client.get(url, headers=request_id_header())
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


def build_summary_service(
    document_client: httpx.AsyncClient,
    llm_client: LlmClient,
    cache: SummaryCache,
    config: Settings | None = None,
) -> SummaryService:
    """Compone el servicio con la config dada (o la del entorno).

    Lo usan el lifespan (para el consumer de Kafka) y `routes.py` (para los
    endpoints), así ambos caminos corren la misma lógica de resumen.
    """
    config = config if config is not None else settings
    return SummaryService(
        document_client=document_client,
        persistence_url=config.persistence_service_url,
        llm_client=llm_client,
        model=config.ollama_model,
        max_summary_chars=config.max_summary_chars,
        cache=cache,
    )

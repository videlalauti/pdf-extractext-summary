"""Summary service: capa de aplicación, orquestación con persistence y Ollama."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

import httpx
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from shared.web.logging import request_id_header

from cache import NoopSummaryCache, SummaryCache
from errors import (
    DocumentNotFoundError,
    EmptyDocumentError,
    PersistenceUnavailableError,
)
from llm import LlmClient

logger = logging.getLogger(__name__)

JOB_STATE_QUEUED = "queued"
JOB_STATE_PROCESSING = "processing"
JOB_STATE_DONE = "done"
JOB_STATE_FAILED = "failed"
TERMINAL_JOB_STATES = frozenset({JOB_STATE_DONE, JOB_STATE_FAILED})
JOB_STATES = frozenset(
    {JOB_STATE_QUEUED, JOB_STATE_PROCESSING, JOB_STATE_DONE, JOB_STATE_FAILED}
)
# Tope de intentos iniciados por job. Es la única fuente del límite: el consumer
# usa el mismo valor para decidir cuándo descartar una entrega.
MAX_SUMMARY_ATTEMPTS = 3
MAX_ATTEMPTS_REASON = "max_attempts_exhausted"
SUMMARY_KEY_PREFIX = "summary:"
# Un job sin updated_at (esquema viejo) se considera viejo y reconciliable.
LEGACY_UPDATED_AT = "1970-01-01T00:00:00+00:00"


class EnqueueResult(str, Enum):
    """Resultado de encolar: distingue un job done de un fallo de persistencia."""

    QUEUED = "queued"
    DONE = "done"
    UNAVAILABLE = "unavailable"

# Locks por documento compartidos por todo el proceso: el consumer de Kafka y
# el BackgroundTask del POST usan instancias distintas de SummaryService, así
# que un lock de instancia no evitaría que ambos infieran el mismo documento.
_document_locks: dict[str, asyncio.Lock] = {}


def _lock_for(document_id: str) -> asyncio.Lock:
    lock = _document_locks.get(document_id)
    if lock is None:
        lock = asyncio.Lock()
        _document_locks[document_id] = lock
    return lock


def _release_lock(document_id: str, lock: asyncio.Lock) -> None:
    """Limpia el lock solo si nadie lo está usando ni esperando."""
    if not lock.locked() and _document_locks.get(document_id) is lock:
        _document_locks.pop(document_id, None)

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
    # TTL de los estados no terminales (queued/processing).
    summary_cache_ttl_seconds: int = Field(default=3600, gt=0)
    # TTL de los estados terminales (done/failed): el resultado no debe
    # desaparecer a la hora.
    summary_result_ttl_seconds: int = Field(default=604800, gt=0)
    # Un queued fresco no se reintenta desde GET/POST antes de este backoff; un
    # processing más viejo que esto se considera huérfano y se puede reanudar.
    summary_retry_backoff_seconds: int = Field(default=30, gt=0)
    summary_stale_seconds: int = Field(default=600, gt=0)
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
    """Estado y resultado del resumen, guardado como JSON en una sola clave Redis.

    ``attempts`` cuenta intentos **iniciados** (se incrementa antes de inferir);
    un crash en medio de una inferencia deja el intento contado y reconcile no
    lo vuelve a incrementar. ``updated_at`` es ISO-8601 UTC y se usa para el
    backoff de la ruta y para detectar jobs viejos.
    """

    status: Literal["queued", "processing", "done", "failed"]
    summary: str | None = None
    attempts: int = 0
    updated_at: str = LEGACY_UPDATED_AT
    # Token saneado: type(error).__name__, empty_document o
    # max_attempts_exhausted. Nunca el mensaje de la excepción.
    reason: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_timestamp(value: str | None) -> datetime | None:
    """Interpreta ISO-8601 o un epoch numérico; None si no se puede.

    Un valor sin zona se asume UTC. Nunca se devuelve un datetime naive para
    que la resta contra ``utc_now()`` no explote.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_stale(
    updated_at: str, threshold_seconds: int, *, now: datetime | None = None
) -> bool:
    """True si ``updated_at`` es inválido o tiene al menos ``threshold_seconds``.

    Un timestamp inválido se trata como viejo: es preferible reanudar un job
    dudoso que dejarlo bloqueado para siempre.
    """
    updated = parse_timestamp(updated_at)
    if updated is None:
        return True
    reference = now or utc_now()
    return (reference - updated).total_seconds() >= threshold_seconds


def build_summary_key(document_id: str) -> str:
    return f"{SUMMARY_KEY_PREFIX}{document_id}"


def serialize_job(
    status: str,
    *,
    summary: str | None = None,
    attempts: int = 0,
    updated_at: str | None = None,
    reason: str | None = None,
) -> str:
    job = SummaryJob(
        status=status,
        summary=summary,
        attempts=attempts,
        updated_at=updated_at or utc_now_iso(),
        reason=reason,
    )
    return job.model_dump_json(exclude_none=True)


def parse_job(raw: str | None) -> SummaryJob | None:
    """Interpreta lo guardado en la caché; un valor corrupto se trata como miss.

    Acepta jobs del esquema viejo (sin attempts/updated_at/reason): Pydantic
    aplica los defaults. Un estado desconocido falla la validación y también se
    trata como miss, para que el GET recupere desde persistence.
    """
    if raw is None:
        return None
    try:
        return SummaryJob.model_validate_json(raw)
    except ValueError:
        logger.warning("valor de caché inválido para un resumen; se trata como miss")
        return None


def _reason_for(error: Exception) -> str:
    """Token saneado para el job: nunca el mensaje (filtra URLs o contenido)."""
    reason = getattr(error, "reason", None)
    if isinstance(reason, str) and reason:
        return reason[:80]
    return type(error).__name__[:80]


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

    El trigger automático del trabajo es el consumer de Kafka
    (`documents.extracted`), que llama a `process`; los endpoints leen el
    estado con `peek_job`, lo marcan como `queued` con `enqueue` y pueden
    disparar el procesamiento desde un `BackgroundTask`.
    """

    def __init__(
        self,
        document_client: httpx.AsyncClient,
        persistence_url: str,
        llm_client: LlmClient,
        model: str,
        max_summary_chars: int,
        cache: SummaryCache | None = None,
        *,
        cache_ttl_seconds: int = 3600,
        result_ttl_seconds: int = 604800,
        retry_backoff_seconds: int = 30,
        stale_seconds: int = 600,
    ) -> None:
        self._document_client = document_client
        self._persistence_url = persistence_url.rstrip("/")
        self._llm_client = llm_client
        self._model = model
        self._max_summary_chars = max_summary_chars
        self._cache = cache if cache is not None else NoopSummaryCache()
        self._cache_ttl_seconds = cache_ttl_seconds
        self._result_ttl_seconds = result_ttl_seconds
        self._retry_backoff_seconds = retry_backoff_seconds
        self._stale_seconds = stale_seconds

    async def peek_job(self, document_id: str) -> SummaryJob | None:
        """Devuelve el job guardado en la caché (estado + resultado) o None si es miss."""
        return parse_job(await self._cache.get(build_summary_key(document_id)))

    async def enqueue(self, document_id: str) -> EnqueueResult:
        """Marca el job como encolado, reseteando intentos y resultado.

        Nunca pisa un job ``done``: si ya hay un resultado listo devuelve
        ``DONE`` y el llamador puede responder 200 sin reencolar.
        ``UNAVAILABLE`` significa que no se pudo persistir el estado (Redis
        caído o deshabilitado).
        """
        key = build_summary_key(document_id)
        current = await self.peek_job(document_id)
        if current is not None and current.status == JOB_STATE_DONE:
            return EnqueueResult.DONE
        saved = await self._save_job(key, JOB_STATE_QUEUED, attempts=0)
        return EnqueueResult.QUEUED if saved else EnqueueResult.UNAVAILABLE

    async def is_state_available(self) -> bool:
        """True si Redis puede guardar el estado del job (no alcanza con leer)."""
        return await self._cache.ping()

    def _is_queued_stale(self, job: SummaryJob) -> bool:
        return _is_stale(job.updated_at, self._retry_backoff_seconds)

    def _is_processing_stale(self, job: SummaryJob) -> bool:
        return _is_stale(job.updated_at, self._stale_seconds)

    def should_schedule(self, job: SummaryJob) -> bool:
        """True si GET/POST debe disparar ``process`` para este job.

        - queued: recién pasado el backoff de la ruta (o si ya agotó intentos,
          para que ``process`` lo marque failed sin inferir).
        - processing: solo si superó ``SUMMARY_STALE_SECONDS`` (huérfano).
        - done/failed: nunca (terminales).
        """
        if job.status == JOB_STATE_QUEUED:
            # attempts == 0 es un job recién encolado: nunca se re-dispara solo
            # por un POST/GET que lo creó. Con intentos previos, se espera el
            # backoff; si ya agotó, se dispara para que process lo marque failed.
            return (
                job.attempts == 0
                or job.attempts >= MAX_SUMMARY_ATTEMPTS
                or self._is_queued_stale(job)
            )
        if job.status == JOB_STATE_PROCESSING:
            return self._is_processing_stale(job)
        return False

    async def fetch_document(self, document_id: str) -> dict:
        """Recupera el documento desde persistence (404/503 como errores de dominio).

        Es público porque el GET lo usa para distinguir un miss de caché real
        (documento inexistente) de un parpadeo de Redis, antes de reencolar.
        """
        return await self._fetch_document(document_id)

    async def mark_failed(self, document_id: str, reason: str = MAX_ATTEMPTS_REASON) -> bool:
        """Deja el job en ``failed`` de forma idempotente.

        No pisa un ``done`` y conserva el ``reason`` real si el job ya estaba
        ``failed`` (por ejemplo, el tercer fallo de ``process``).
        """
        key = build_summary_key(document_id)
        job = await self.peek_job(document_id)
        if job is not None and job.status == JOB_STATE_DONE:
            return False
        if job is not None and job.status == JOB_STATE_FAILED:
            return True
        attempts = job.attempts if job is not None else 0
        return await self._save_job(
            key, JOB_STATE_FAILED, attempts=attempts, reason=reason
        )

    async def process(
        self, document_id: str, *, trigger: Literal["route", "consumer"] = "consumer"
    ) -> bool:
        """Resumen un documento: processing → resumen → done + resultado en la caché.

        Es idempotente: si el job ya está ``done`` o ``failed`` no vuelve a
        inferir. El lock por documento (compartido por todo el proceso) evita
        que el consumer y el BackgroundTask del POST infieran dos veces el
        mismo documento.

        ``attempts`` cuenta intentos iniciados: se incrementa dentro del lock,
        justo antes de escribir ``processing``. Si el proceso muere durante la
        inferencia, el intento ya quedó contado; reconcile preserva el valor.

        ``trigger`` distingue el driver:
        - ``consumer`` (at-least-once): reintenta un ``queued`` fresco sin
          esperar el backoff de la ruta, porque Kafka ya es el reloj del retry.
        - ``route`` (GET/POST): no re-infiera un ``queued`` fresco con intentos
          previos; espera ``SUMMARY_RETRY_BACKOFF_SECONDS``. Un ``processing``
          más viejo que ``SUMMARY_STALE_SECONDS`` se trata como ``queued`` para
          recuperar un job huérfano.

        Devuelve True si el job quedó resuelto (``done`` o ``failed``). Un
        fallo con intentos restantes deja el job ``queued`` y devuelve False;
        el consumer decide el reintento (at-least-once) y la ruta lo hace tras
        el backoff: si ese reintento vuelve a fallar, el siguiente GET stale
        (pasado el backoff) lo recupera.
        """
        job = await self.peek_job(document_id)
        if job is not None and job.status in TERMINAL_JOB_STATES:
            logger.info("documento %s ya resuelto (%s); no se infiere", document_id, job.status)
            return True

        key = build_summary_key(document_id)
        lock = _lock_for(document_id)
        try:
            async with lock:
                # Re-chequeo: otro camino (consumer o background) pudo resolverlo
                # mientras esperábamos el lock. La decisión de intento y el
                # incremento de attempts se hacen acá adentro: dos triggers
                # concurrentes sobre un queued stale incrementan una sola vez.
                job = await self.peek_job(document_id)
                if job is not None and job.status in TERMINAL_JOB_STATES:
                    logger.info(
                        "documento %s ya resuelto (%s); no se infiere",
                        document_id,
                        job.status,
                    )
                    return True

                attempts = job.attempts if job is not None else 0
                if job is not None and job.status == JOB_STATE_QUEUED:
                    if attempts >= MAX_SUMMARY_ATTEMPTS:
                        logger.warning(
                            "documento %s agotó los %d intentos; se marca failed sin inferir",
                            document_id,
                            MAX_SUMMARY_ATTEMPTS,
                        )
                        await self._save_job(
                            key,
                            JOB_STATE_FAILED,
                            attempts=attempts,
                            reason=MAX_ATTEMPTS_REASON,
                        )
                        return True
                    if (
                        trigger == "route"
                        and attempts > 0
                        and not self._is_queued_stale(job)
                    ):
                        logger.info(
                            "documento %s queued hace menos de %ss; la ruta espera el backoff",
                            document_id,
                            self._retry_backoff_seconds,
                        )
                        return False
                elif job is not None and job.status == JOB_STATE_PROCESSING:
                    if not self._is_processing_stale(job):
                        logger.info(
                            "documento %s sigue processing; no se re-infere", document_id
                        )
                        return False
                    # Huérfano: se trata como queued (sin incrementar dos veces).
                    if attempts >= MAX_SUMMARY_ATTEMPTS:
                        logger.warning(
                            "documento %s quedó processing con %d intentos; se marca failed",
                            document_id,
                            attempts,
                        )
                        await self._save_job(
                            key,
                            JOB_STATE_FAILED,
                            attempts=attempts,
                            reason=MAX_ATTEMPTS_REASON,
                        )
                        return True

                attempts += 1
                if not await self._save_job(
                    key, JOB_STATE_PROCESSING, attempts=attempts
                ):
                    # Sin estado persistido no se puede deduplicar ni recuperar:
                    # mejor no gastar una inferencia que se va a perder.
                    logger.warning(
                        "no se pudo persistir el estado de %s; no se infiere", document_id
                    )
                    return False

                timings = SummaryTimings()
                try:
                    summary = await self._generate_summary(document_id, timings)
                except EmptyDocumentError as error:
                    # Determinístico: no hay reintento que pueda mejorarlo.
                    reason = _reason_for(error)
                    await self._save_job(
                        key, JOB_STATE_FAILED, attempts=attempts, reason=reason
                    )
                    self._log_failure(document_id, timings, error)
                    return True
                except Exception as error:  # noqa: BLE001 - se traduce a estado del job
                    reason = _reason_for(error)
                    if attempts >= MAX_SUMMARY_ATTEMPTS:
                        await self._save_job(
                            key, JOB_STATE_FAILED, attempts=attempts, reason=reason
                        )
                    else:
                        await self._save_job(
                            key, JOB_STATE_QUEUED, attempts=attempts
                        )
                    self._log_failure(document_id, timings, error)
                    return False

                logger.info(
                    "resumen completado document_id=%s fetch_ms=%.2f ollama_ms=%.2f total_ms=%.2f",
                    document_id,
                    timings.fetch_ms,
                    timings.ollama_ms,
                    timings.total_ms,
                )
                return await self._save_job(
                    key,
                    JOB_STATE_DONE,
                    summary=summary,
                    attempts=attempts,
                )
        finally:
            _release_lock(document_id, lock)

    def _log_failure(
        self, document_id: str, timings: SummaryTimings, error: Exception
    ) -> None:
        """Una sola línea por job: métricas y tipo de error, nunca el mensaje."""
        logger.error(
            "resumen fallido document_id=%s fetch_ms=%.2f ollama_ms=%.2f "
            "total_ms=%.2f error=%s",
            document_id,
            timings.fetch_ms,
            timings.ollama_ms,
            timings.total_ms,
            type(error).__name__,
            exc_info=error,
        )

    async def _save_job(
        self,
        key: str,
        status: str,
        *,
        summary: str | None = None,
        attempts: int = 0,
        reason: str | None = None,
    ) -> bool:
        """Escribe el job con el TTL que corresponde a su estado."""
        ttl = (
            self._result_ttl_seconds
            if status in TERMINAL_JOB_STATES
            else self._cache_ttl_seconds
        )
        return await self._cache.set(
            key,
            serialize_job(
                status,
                summary=summary,
                attempts=attempts,
                reason=reason,
            ),
            ttl_seconds=ttl,
        )

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
            if not content.strip():
                # Determinístico: no tiene sentido gastar una inferencia (ni
                # reintentarla) sobre un documento vacío.
                raise EmptyDocumentError
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
        cache_ttl_seconds=config.summary_cache_ttl_seconds,
        result_ttl_seconds=config.summary_result_ttl_seconds,
        retry_backoff_seconds=config.summary_retry_backoff_seconds,
        stale_seconds=config.summary_stale_seconds,
    )

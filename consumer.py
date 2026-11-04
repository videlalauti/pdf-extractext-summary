"""Consumo de eventos ``documents.extracted``: puerto y adaptador sobre Kafka.

El servicio no depende de ``aiokafka`` en su lógica: el consumer recibe un
``SummaryService`` (que a su vez depende de los puertos ``LlmClient`` y
``SummaryCache``) y el adaptador AIOKafka se puede inyectar en los tests, igual
que el resto de los adaptadores del repo.

Commit de offset (elección y justificación): ``enable_auto_commit=False`` y se
confirma **explícitamente** el offset del mensaje resuelto, con
``{TopicPartition: offset + 1}``. Nunca se confirma la posición actual: los
commits de Kafka son acumulativos, así que confirmar N+1 con N fallido marcaría
N como consumido para siempre.

Si un mensaje falla no se avanza: se reintenta con ``seek`` al mismo offset.
Al agotar los intentos solo se confirma si el estado terminal quedó guardado;
sin estado persistible se conserva el mensaje y se reintenta con backoff.
Eso da semántica at-least-once: si el proceso se cae, los mensajes sin confirmar
se reprocesan al volver, y el chequeo de idempotencia junto con el lock por
documento hace seguro ese reenvío.

Si Redis no puede guardar estado (``ping`` falso), el fallo no cuenta como una
entrega del mensaje: se espera un intervalo fijo de 10s y se reintenta, con un
contador de 30 esperas que se reinicia sin confirmar el offset. Se usa el log
``cache_unavailable`` para distinguirlo de los errores de inferencia. Con la caché
deshabilitada por configuración el consumer ni siquiera arranca (ver ``main``).
"""

import asyncio
import contextlib
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from shared.web.logging import new_request_id, reset_request_id, set_request_id

from app import (
    JOB_STATE_DONE,
    JOB_STATE_FAILED,
    MAX_SUMMARY_ATTEMPTS,
    SummaryService,
)

logger = logging.getLogger(__name__)

DOCUMENTS_EXTRACTED_TOPIC = "documents.extracted"
CONSUMER_GROUP_ID = "summary-service"
# Timeouts coherentes entre sí: request_timeout_ms debe ser mayor que
# session_timeout_ms para no expulsar al consumer durante un rebalanceo o una
# respuesta lenta del broker. El heartbeat va bien por debajo de la sesión.
KAFKA_SESSION_TIMEOUT_MS = 10_000
KAFKA_HEARTBEAT_INTERVAL_MS = 3_000
KAFKA_REQUEST_TIMEOUT_MS = 30_000
# Reintento del supervisor: 1s → 2s → 4s … con tope de 30s.
CONSUMER_RETRY_INITIAL_SECONDS = 1.0
CONSUMER_RETRY_MAX_SECONDS = 30.0
MESSAGE_RETRY_INITIAL_SECONDS = 2.0
MESSAGE_RETRY_MAX_SECONDS = 30.0
# Mientras Redis no puede guardar estado no tiene sentido contar entregas (se
# perderían al no poder escribir ``processing``/``failed``). Se espera un
# intervalo fijo y acotado: 30 × 10s = 5 min por mensaje como máximo.
CACHE_UNAVAILABLE_RETRY_SECONDS = 10.0
MAX_CACHE_WAIT_ATTEMPTS = 30


def _parse_event(value: bytes | str | None) -> dict | None:
    """Interpreta el mensaje como dict; None (ya logueado) si no es un JSON de objeto."""
    try:
        event = json.loads(value)
    except (ValueError, TypeError):
        event = None
    if not isinstance(event, dict):
        logger.error("mensaje sin JSON interpretable en %s: %r", DOCUMENTS_EXTRACTED_TOPIC, value)
        return None
    return event


def _document_id_of(event: dict | None) -> str | None:
    """Valida el ``document_id`` del evento ya parseado; None si falta o no es un str."""
    if event is None:
        return None
    document_id = event.get("document_id")
    if not isinstance(document_id, str) or not document_id:
        logger.error("evento sin document_id válido en %s: %r", DOCUMENTS_EXTRACTED_TOPIC, event)
        return None
    return document_id


def _request_id_of(event: dict | None) -> str:
    """Hereda el ``request_id`` del evento; si falta o no sirve, genera uno nuevo.

    El campo es aditivo y opcional (productores viejos no lo mandan), por eso se
    tolera su ausencia y se cae a un id nuevo en lugar de propagar ``-``.
    """
    if event is not None:
        request_id = event.get("request_id")
        if isinstance(request_id, str) and request_id and request_id != "-":
            return request_id
    return new_request_id()


class KafkaSummaryConsumer:
    """Consume ``documents.extracted`` y dispara el resumen del documento.

    Corre como tarea en el lifespan (``start``/``stop``): por cada evento hace
    lo mismo que el viejo job en background — fetch desde persistence, recorte,
    Ollama y guardado en la caché — vía ``SummaryService.process``.
    """

    def __init__(
        self,
        service: SummaryService,
        bootstrap_servers: str,
        topic: str = DOCUMENTS_EXTRACTED_TOPIC,
        group_id: str = CONSUMER_GROUP_ID,
        consumer: Any | None = None,
        retry_initial_seconds: float = CONSUMER_RETRY_INITIAL_SECONDS,
        retry_max_seconds: float = CONSUMER_RETRY_MAX_SECONDS,
        message_retry_initial_seconds: float = MESSAGE_RETRY_INITIAL_SECONDS,
        message_retry_max_seconds: float = MESSAGE_RETRY_MAX_SECONDS,
        cache_retry_seconds: float = CACHE_UNAVAILABLE_RETRY_SECONDS,
        max_cache_wait_attempts: int = MAX_CACHE_WAIT_ATTEMPTS,
    ) -> None:
        self._service = service
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        # Inyectable para tests (patrón de fakes del repo); si no, el de aiokafka.
        self._injected_consumer = consumer
        self._consumer: Any | None = None
        self._task: asyncio.Task | None = None
        self._connected = False
        self._retry_initial_seconds = retry_initial_seconds
        self._retry_max_seconds = retry_max_seconds
        self._message_retry_initial_seconds = message_retry_initial_seconds
        self._message_retry_max_seconds = message_retry_max_seconds
        self._cache_retry_seconds = cache_retry_seconds
        self._max_cache_wait_attempts = max_cache_wait_attempts
        # Intentos por (partición, offset) para el tope de descarte controlado.
        self._failures: dict[tuple[TopicPartition, int], int] = {}
        # Esperas por caché no disponible: se limpian al volver el ping y al
        # confirmar, para que un Redis que oscila no arrastre el tope.
        self._cache_waits: dict[tuple[TopicPartition, int], int] = {}

    @property
    def is_connected(self) -> bool:
        """True mientras el consumer haya arrancado y no se haya caído/cerrado."""
        return self._connected

    @property
    def is_running(self) -> bool:
        """True mientras la tarea supervisora siga viva."""
        return self._task is not None and not self._task.done()

    def _build_consumer(self) -> Any:
        return AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            # Un grupo nuevo arranca desde el principio del topic: los eventos
            # publicados antes de que el consumer se suscriba no se pierden.
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
            heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL_MS,
            request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        )

    async def start(self) -> None:
        """Lanza el supervisor que conecta con Kafka y reintenta si falla.

        La tarea arranca en background: un broker caído no frena el lifespan
        (fail-open) y el servicio se reconecta solo cuando Kafka vuelve.
        """
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._supervise())

    async def _supervise(self) -> None:
        """Conecta con backoff exponencial y supervisa el loop de consumo."""
        delay = self._retry_initial_seconds
        while True:
            consumer = self._injected_consumer or self._build_consumer()
            try:
                await consumer.start()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._connected = False
                logger.warning(
                    "Kafka no disponible (%s); reintento en %.1fs: %s",
                    self._bootstrap_servers,
                    delay,
                    type(error).__name__,
                    exc_info=True,
                )
                with contextlib.suppress(Exception):
                    await consumer.stop()
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._retry_max_seconds)
                continue

            self._consumer = consumer
            self._connected = True
            delay = self._retry_initial_seconds
            try:
                async for message in consumer:
                    await self.process_message(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("el consumer de %s terminó inesperadamente", self._topic)
                self._connected = False
                self._consumer = None
                with contextlib.suppress(Exception):
                    await consumer.stop()
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._retry_max_seconds)
                continue

            self._connected = False
            return

    async def process_message(self, message: Any) -> None:
        """Resuelve un mensaje y confirma su offset exacto solo si quedó resuelto.

        Hereda el ``request_id`` del evento (o genera uno si el productor no lo
        trae) y lo restaura en un ``finally``: el contextvar no debe filtrarse
        al mensaje siguiente, ni siquiera cuando el procesamiento falla y el
        offset no se confirma.
        """
        event = _parse_event(message.value)
        token = set_request_id(_request_id_of(event))
        try:
            document_id = _document_id_of(event)
            # Inválido: nunca va a ser procesable; se confirma para no atorar la cola.
            if document_id is None:
                await self._commit(message)
                return

            try:
                resolved = await self._resolve(document_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "no se pudo procesar un mensaje de %s; se reintenta el offset",
                    self._topic,
                )
                resolved = False

            if resolved:
                key = self._failure_key(message)
                self._failures.pop(key, None)
                self._cache_waits.pop(key, None)
                await self._commit(message)
                return
            await self._handle_failure(message, document_id)
        finally:
            reset_request_id(token)

    async def _resolve(self, document_id: str) -> bool:
        """True = mensaje resuelto (procesado o descartado); False = falló (reprocesar).

        ``failed`` es terminal: reintentar la misma entrega no cambia nada y el
        job ya tiene un ``reason``. El consumer lo confirma y el cliente ve 409.
        """
        job = await self._service.peek_job(document_id)
        if job is not None and job.status == JOB_STATE_DONE:
            logger.info("documento %s ya resumido; se omite", document_id)
            return True
        if job is not None and job.status == JOB_STATE_FAILED:
            logger.info(
                "documento %s ya falló (%s); se omite",
                document_id,
                job.reason or "sin reason",
            )
            return True
        return await self._service.process(document_id)

    @staticmethod
    def _failure_key(message: Any) -> tuple[TopicPartition, int]:
        return TopicPartition(message.topic, message.partition), message.offset

    async def _handle_failure(self, message: Any, document_id: str) -> None:
        """Reintenta el offset hasta que haya un estado terminal confirmado.

        Un fallo prolongado de Redis nunca se convierte en un commit: si no se
        pudo guardar el resultado o el error terminal, Kafka debe retener el
        mensaje. Los contadores limitan el backoff, no la recuperación.
        """
        tp, offset = self._failure_key(message)
        key = (tp, offset)

        if not await self._service.is_state_available():
            waits = self._cache_waits.get(key, 0) + 1
            if waits >= self._max_cache_wait_attempts:
                logger.error(
                    "Redis sigue caído; mensaje retenido sin commit "
                    "document_id=%s topic=%s partition=%d offset=%d esperas=%d",
                    document_id,
                    tp.topic,
                    tp.partition,
                    offset,
                    waits,
                )
                # Reiniciar solo el contador de esperas; el offset permanece pendiente.
                self._cache_waits.pop(key, None)
                await asyncio.sleep(self._cache_retry_seconds)
                self._seek(message)
                return
            self._cache_waits[key] = waits
            logger.warning(
                "cache_unavailable document_id=%s offset=%d; se reintenta en %.1fs "
                "(%d/%d)",
                document_id,
                offset,
                self._cache_retry_seconds,
                waits,
                self._max_cache_wait_attempts,
            )
            await asyncio.sleep(self._cache_retry_seconds)
            self._seek(message)
            return

        # Redis volvió: un contador de esperas viejo no debe arrastrar el tope.
        self._cache_waits.pop(key, None)
        attempts = self._failures.get(key, 0) + 1
        if attempts >= MAX_SUMMARY_ATTEMPTS:
            job = await self._service.peek_job(document_id)
            if job is not None and job.attempts >= MAX_SUMMARY_ATTEMPTS:
                await self._service.mark_failed(document_id)
                job = await self._service.peek_job(document_id)

            self._failures.pop(key, None)
            if job is not None and job.status in (JOB_STATE_DONE, JOB_STATE_FAILED):
                # El resultado terminal existe en el almacén: ya se puede confirmar.
                await self._commit(message)
                return

            logger.error(
                "sin estado terminal persistido; mensaje retenido sin commit "
                "document_id=%s topic=%s partition=%d offset=%d",
                document_id,
                tp.topic,
                tp.partition,
                offset,
            )
            await asyncio.sleep(self._message_retry_max_seconds)
            self._seek(message)
            return

        self._failures[key] = attempts
        delay = min(
            self._message_retry_initial_seconds * (2 ** (attempts - 1)),
            self._message_retry_max_seconds,
        )
        await asyncio.sleep(delay)
        self._seek(message)

    def _seek(self, message: Any) -> None:
        tp, offset = self._failure_key(message)
        consumer = self._consumer or self._injected_consumer
        if consumer is not None:
            consumer.seek(tp, offset)

    async def _commit(self, message: Any) -> None:
        """Confirma el offset exacto del mensaje, nunca la posición actual."""
        tp, offset = self._failure_key(message)
        consumer = self._consumer or self._injected_consumer
        if consumer is None:
            return
        try:
            await consumer.commit({tp: offset + 1})
        except Exception:
            # Un commit posterior podría confirmar este mensaje de forma
            # acumulativa. Fallar el loop fuerza al supervisor a reconectar
            # desde el último offset confirmado por el broker.
            logger.exception("no se pudo confirmar %s offset=%d", tp, offset)
            raise

    async def stop(self) -> None:
        """Corta el loop y cierra la conexión (disposability: shutdown ordenado)."""
        if self._task is not None:
            task, self._task = self._task, None
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._connected = False
        if self._consumer is not None:
            consumer, self._consumer = self._consumer, None
            try:
                await consumer.stop()
            except Exception as error:
                logger.warning(
                    "no se pudo cerrar el consumer de Kafka: %s",
                    type(error).__name__,
                    exc_info=True,
                )

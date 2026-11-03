"""Consumo de eventos ``documents.extracted``: puerto y adaptador sobre Kafka.

El servicio no depende de ``aiokafka`` en su lógica: el consumer recibe un
``SummaryService`` (que a su vez depende de los puertos ``LlmClient`` y
``SummaryCache``) y el adaptador AIOKafka se puede inyectar en los tests, igual
que el resto de los adaptadores del repo.

Commit de offset (elección y justificación): ``enable_auto_commit=False`` y el
offset se confirma recién después de resolver el mensaje — procesado con éxito,
resultado ya cacheado o descartado por inválido — y **nunca** si el
procesamiento falló. Eso da semántica at-least-once: si el proceso se cae, los
mensajes sin confirmar se reprocesan al volver, y el chequeo de idempotencia
(evita re-inferir un resumen ya cacheado) hace seguro ese reenvío. El coste de
reprocesar es barato (una llamada a Ollama que se skipea si ya hay resultado);
el de perder un documento porque el offset se confirmó antes de tiempo, no.

Un mensaje fallido no tumba al consumer: se loguea el error, no se confirma el
offset y el loop sigue con el siguiente mensaje.
"""

import asyncio
import contextlib
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
from shared.web.logging import new_request_id, reset_request_id, set_request_id

from app import JOB_STATE_DONE, SummaryService

logger = logging.getLogger(__name__)

DOCUMENTS_EXTRACTED_TOPIC = "documents.extracted"
CONSUMER_GROUP_ID = "summary-service"
# Timeout acotado: si Kafka está caído, fallar rápido en el arranque (fail-open)
# en lugar de dejar el lifespan esperando el timeout por defecto.
KAFKA_REQUEST_TIMEOUT_MS = 2000
# Reintento del supervisor: 1s → 2s → 4s … con tope de 30s.
CONSUMER_RETRY_INITIAL_SECONDS = 1.0
CONSUMER_RETRY_MAX_SECONDS = 30.0


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
        """Resuelve un mensaje y confirma el offset solo si quedó resuelto.

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
            resolved = True if document_id is None else await self._resolve(document_id)
            if resolved:
                await self._commit()
        except Exception:
            logger.exception(
                "no se pudo procesar un mensaje de %s; no se confirma el offset", self._topic
            )
        finally:
            reset_request_id(token)

    async def _resolve(self, document_id: str) -> bool:
        """True = mensaje resuelto (procesado o descartado); False = falló (reprocesar)."""
        job = await self._service.peek_job(document_id)
        if job is not None and job.status == JOB_STATE_DONE:
            logger.info("documento %s ya resumido; se omite", document_id)
            return True
        return await self._service.process(document_id)

    async def _commit(self) -> None:
        try:
            await self._consumer.commit()
        except Exception:
            # No propagar: el offset se confirma en el siguiente mensaje o al reiniciar.
            logger.warning("no se pudo confirmar el offset en %s", self._topic, exc_info=True)

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

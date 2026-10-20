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

from app import JOB_STATE_DONE, SummaryService

logger = logging.getLogger(__name__)

DOCUMENTS_EXTRACTED_TOPIC = "documents.extracted"
CONSUMER_GROUP_ID = "summary-service"
# Timeout acotado: si Kafka está caído, fallar rápido en el arranque (fail-open)
# en lugar de dejar el lifespan esperando el timeout por defecto.
KAFKA_REQUEST_TIMEOUT_MS = 2000


def _document_id_of(value: bytes | str | None) -> str | None:
    """Extrae el ``document_id`` del evento; None si el mensaje no es interpretable."""
    try:
        event = json.loads(value)
        document_id = event["document_id"]
    except (ValueError, TypeError, KeyError):
        logger.error("mensaje sin JSON interpretable en %s: %r", DOCUMENTS_EXTRACTED_TOPIC, value)
        return None
    if not isinstance(document_id, str) or not document_id:
        logger.error("evento sin document_id válido en %s: %r", DOCUMENTS_EXTRACTED_TOPIC, value)
        return None
    return document_id


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
    ) -> None:
        self._service = service
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        # Inyectable para tests (patrón de fakes del repo); si no, el de aiokafka.
        self._consumer = consumer
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Arranca el loop de consumo en background; un broker caído no frena el arranque.

        aiokafka acota su propio retry de bootstrap (~4 s si el broker rechaza
        la conexión): el fail-open garantiza que el servicio levante igual, sin
        consumer, en vez de morir en el lifespan.
        """
        consumer = self._consumer or AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            # Un grupo nuevo arranca desde el principio del topic: los eventos
            # publicados antes de que el consumer se suscriba no se pierden.
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        )
        try:
            await consumer.start()
        except Exception as error:
            logger.warning(
                "Kafka no disponible (%s); el servicio arranca sin consumer: %s",
                self._bootstrap_servers,
                type(error).__name__,
                exc_info=True,
            )
            with contextlib.suppress(Exception):
                await consumer.stop()
            return
        self._consumer = consumer
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            async for message in self._consumer:
                await self.process_message(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("el consumer de %s terminó inesperadamente", self._topic)

    async def process_message(self, message: Any) -> None:
        """Resuelve un mensaje y confirma el offset solo si quedó resuelto."""
        try:
            resolved = await self._resolve(message.value)
        except Exception:
            logger.exception(
                "no se pudo procesar un mensaje de %s; no se confirma el offset", self._topic
            )
            return
        if resolved:
            await self._commit()

    async def _resolve(self, value: bytes | str | None) -> bool:
        """True = mensaje resuelto (procesado o descartado); False = falló (reprocesar)."""
        document_id = _document_id_of(value)
        if document_id is None:
            # Inválido: nunca va a ser procesable; confirmar para no atorar la cola.
            return True
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

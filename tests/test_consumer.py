"""Tests del consumer de Kafka: procesamiento, idempotencia y commit de offset.

Reemplazan a los viejos tests de jobs en background (``test_async_summary.py``):
el trigger del resumen ya no es una tarea asyncio en el proceso, sino el
consumer de ``documents.extracted`` corriendo en el lifespan.
"""

import asyncio
import contextlib
import io
import json
import logging
from collections.abc import Iterator
from types import SimpleNamespace

import httpx
import pytest
from aiokafka import TopicPartition
from fakes import (
    DEFAULT_CONTENT,
    FakeKafkaConsumer,
    FakeLlmClient,
    FakeSummaryCache,
    document_client,
)
from shared.web.logging import (
    RequestIdFormatter,
    get_request_id,
    reset_request_id,
    set_request_id,
)

from app import (
    JOB_STATE_DONE,
    JOB_STATE_PROCESSING,
    Settings,
    build_summary_service,
    settings,
)
from cache import SummaryCache
from consumer import DOCUMENTS_EXTRACTED_TOPIC, KafkaSummaryConsumer
from errors import LlmTimeoutError


class RecordingLlmClient(FakeLlmClient):
    """Registra el request_id vigente en cada generación (el contextvar del mensaje)."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.request_ids: list[str] = []

    async def generate(self, model: str, prompt: str) -> str:
        self.request_ids.append(get_request_id())
        return await super().generate(model, prompt)


@contextlib.contextmanager
def formatted_logs() -> Iterator[io.StringIO]:
    """Captura logs formateados en el momento del emit, con el request-id del contextvar.

    ``caplog`` no aplica ``RequestIdFormatter`` y formatear los records al final
    daría ``-`` (el consumer ya reseteó el contextvar): el handler tiene que
    formatear mientras el mensaje se está procesando.
    """
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(RequestIdFormatter("summary-service"))
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield stream
    finally:
        root.removeHandler(handler)
        handler.close()


def event_message(
    document_id: str = "doc-1",
    request_id: str | None = None,
    *,
    offset: int = 0,
    partition: int = 0,
    topic: str = DOCUMENTS_EXTRACTED_TOPIC,
) -> SimpleNamespace:
    """Mensaje con el schema v1 del evento document.extracted (de extraction)."""
    payload = {
        "schema_version": 1,
        "document_id": document_id,
        "checksum": "abc",
        "source": "extraction",
    }
    # Aditivo y opcional, igual que en extraction: solo viaja dentro de una request.
    if request_id is not None:
        payload["request_id"] = request_id
    return SimpleNamespace(
        value=json.dumps(payload).encode("utf-8"),
        topic=topic,
        partition=partition,
        offset=offset,
    )


def build_consumer(
    llm: FakeLlmClient | None = None,
    cache: FakeSummaryCache | None = None,
    content: str = DEFAULT_CONTENT,
    kafka: FakeKafkaConsumer | None = None,
    config: Settings | None = None,
    status_code: int = 200,
    client: httpx.AsyncClient | None = None,
) -> KafkaSummaryConsumer:
    service = build_summary_service(
        document_client=(
            client if client is not None else document_client(content, status_code=status_code)
        ),
        llm_client=llm if llm is not None else FakeLlmClient(),
        cache=cache if cache is not None else FakeSummaryCache(),
        config=config if config is not None else settings,
    )
    return KafkaSummaryConsumer(
        service,
        bootstrap_servers="kafka:9092",
        # Por defecto llega un único evento: los tests de flujo solo necesitan
        # un documento encolado por extraction.
        consumer=kafka if kafka is not None else FakeKafkaConsumer([event_message()]),
        # Backoff corto: los tests no esperan los 1s/30s de producción.
        retry_initial_seconds=0.01,
        retry_max_seconds=0.05,
        # Reintento del mensaje fallido sin esperas reales.
        message_retry_initial_seconds=0.0,
        message_retry_max_seconds=0.0,
    )


async def _consume_all(consumer: KafkaSummaryConsumer) -> None:
    """Arranca, deja terminar el stream inyectado y apaga (ciclo de lifespan)."""
    await consumer.start()
    if consumer._task is not None:
        await consumer._task
    await consumer.stop()


def test_fake_summary_cache_cumple_el_protocolo():
    assert isinstance(FakeSummaryCache(), SummaryCache)


def test_procesa_el_evento_y_guarda_el_resumen_en_cache():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    kafka = FakeKafkaConsumer([event_message()])
    consumer = build_consumer(llm=llm, cache=cache, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert llm.models == [settings.ollama_model]
    assert json.loads(cache.data["summary:doc-1"]) == {
        "status": JOB_STATE_DONE,
        "summary": "resumen de prueba",
    }
    assert kafka.commits == 1


def test_no_re_procesa_un_documento_ya_resumido():
    """Idempotencia: un evento repetido (reentrega) no vuelve a inferir."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    cache.data["summary:doc-1"] = json.dumps({"status": JOB_STATE_DONE, "summary": "ya listo"})
    kafka = FakeKafkaConsumer([event_message()])
    consumer = build_consumer(llm=llm, cache=cache, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert llm.prompts == []
    assert json.loads(cache.data["summary:doc-1"])["summary"] == "ya listo"
    assert kafka.commits == 1


def test_un_fallo_de_llm_no_confirma_offset_ni_tumba_al_consumer():
    """At-least-once: sin commit, el mensaje se reprocesa al reiniciar.

    El consumer sigue con el siguiente mensaje aunque uno falle.
    """
    llm = FakeLlmClient(error=LlmTimeoutError())
    cache = FakeSummaryCache()
    kafka = FakeKafkaConsumer(
        [event_message("doc-1", offset=0), event_message("doc-2", offset=1)]
    )
    consumer = build_consumer(llm=llm, cache=cache, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_PROCESSING
    assert json.loads(cache.data["summary:doc-2"])["status"] == JOB_STATE_PROCESSING
    assert kafka.commits == 0
    assert kafka.commit_offsets == []


class FailOnceLlmClient(FakeLlmClient):
    """Falla la primera generación y funciona en la siguiente."""

    def __init__(self) -> None:
        super().__init__(error=LlmTimeoutError())

    async def generate(self, model: str, prompt: str) -> str:
        try:
            return await super().generate(model, prompt)
        finally:
            self.error = None


def test_un_mensaje_fallido_no_queda_confirmado_por_el_siguiente():
    """Regresión H02: N debe resolverse antes de que N+1 avance el offset."""
    llm = FailOnceLlmClient()
    kafka = FakeKafkaConsumer(
        [
            event_message("doc-1", offset=0),
            # Kafka reentrega N tras el seek; recién después llega N+1.
            event_message("doc-1", offset=0),
            event_message("doc-2", offset=1),
        ]
    )
    consumer = build_consumer(llm=llm, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    tp = TopicPartition(DOCUMENTS_EXTRACTED_TOPIC, 0)
    assert kafka.seeks == [(tp, 0)]
    assert kafka.commit_offsets == [{tp: 1}, {tp: 2}]
    # Tres intentos: N falla, N se reintenta con éxito y N+1 se procesa.
    assert len(llm.prompts) == 3


def test_un_mensaje_que_falla_tres_veces_se_descarta_controladamente(caplog):
    """Tras el tope se confirma para no atorar la partición, con log ERROR."""
    llm = FakeLlmClient(error=LlmTimeoutError())
    kafka = FakeKafkaConsumer([event_message("doc-1", offset=0)] * 3)
    consumer = build_consumer(llm=llm, kafka=kafka)
    caplog.set_level(logging.ERROR)

    asyncio.run(_consume_all(consumer))

    tp = TopicPartition(DOCUMENTS_EXTRACTED_TOPIC, 0)
    assert len(kafka.seeks) == 2
    assert kafka.commit_offsets == [{tp: 1}]
    assert "mensaje descartado tras 3 intentos" in caplog.text


def test_documento_inexistente_no_confirma_el_offset():
    kafka = FakeKafkaConsumer([event_message("no-existe")])
    consumer = build_consumer(kafka=kafka, status_code=404)

    asyncio.run(_consume_all(consumer))

    assert kafka.commits == 0


def test_mensaje_invalido_se_descarta_sin_tumbar_al_consumer():
    """Un mensaje ilegible se confirma como descartado y la cola sigue."""
    llm = FakeLlmClient()
    kafka = FakeKafkaConsumer(
        [
            SimpleNamespace(
                value=b"no es json",
                topic=DOCUMENTS_EXTRACTED_TOPIC,
                partition=0,
                offset=0,
            ),
            SimpleNamespace(
                value=json.dumps({"sin": "document_id"}).encode("utf-8"),
                topic=DOCUMENTS_EXTRACTED_TOPIC,
                partition=0,
                offset=1,
            ),
            event_message(offset=2),
        ]
    )
    consumer = build_consumer(llm=llm, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert len(llm.prompts) == 1
    assert kafka.commits == 3


def test_el_estado_pasa_por_processing_mientras_genera():
    llm = FakeLlmClient(delay=0.1)
    cache = FakeSummaryCache()
    consumer = build_consumer(llm=llm, cache=cache)

    async def scenario():
        task = asyncio.create_task(consumer.process_message(event_message()))
        while not llm.prompts:
            await asyncio.sleep(0.001)
        en_curso = json.loads(cache.data["summary:doc-1"])["status"]
        await task
        return en_curso, json.loads(cache.data["summary:doc-1"])["status"]

    en_durante, final = asyncio.run(scenario())

    assert en_durante == JOB_STATE_PROCESSING
    assert final == JOB_STATE_DONE


def test_start_reintenta_cuando_el_broker_no_esta_disponible():
    """Fail-open + reconexión: no levanta y sigue intentando con backoff."""
    kafka = FakeKafkaConsumer(start_errors=[RuntimeError("connection refused")] * 10)
    consumer = build_consumer(kafka=kafka)

    async def scenario():
        await consumer.start()
        await asyncio.sleep(0.05)
        attempts = kafka.start_attempts
        connected = consumer.is_connected
        await consumer.stop()
        return attempts, connected

    attempts, connected = asyncio.run(scenario())

    assert attempts >= 2
    assert connected is False


def test_start_conecta_cuando_el_broker_vuelve():
    class WaitingKafkaConsumer(FakeKafkaConsumer):
        async def _stream(self):
            while True:
                await asyncio.sleep(0.01)
                if False:  # marca la función como async generator sin emitir mensajes
                    yield

    kafka = WaitingKafkaConsumer(start_errors=[RuntimeError("connection refused")])
    consumer = build_consumer(kafka=kafka)

    async def scenario():
        await consumer.start()
        for _ in range(100):
            if consumer.is_connected:
                break
            await asyncio.sleep(0.01)
        connected = consumer.is_connected
        running = consumer.is_running
        await consumer.stop()
        return connected, running

    connected, running = asyncio.run(scenario())

    assert kafka.start_attempts == 2
    assert connected is True
    assert running is True


def test_stop_cancela_el_loop_y_cierra_el_consumer():
    class EndlessKafkaConsumer(FakeKafkaConsumer):
        async def _stream(self):
            while True:
                # Cede el event loop: sin esto la fake en tight loop starva al
                # resto de las tasks (la real se suspendería en la red).
                await asyncio.sleep(0)
                yield event_message()

    llm = FakeLlmClient()
    kafka = EndlessKafkaConsumer()
    consumer = build_consumer(llm=llm, kafka=kafka)

    async def scenario():
        await consumer.start()
        task = consumer._task
        while not llm.prompts:
            await asyncio.sleep(0.001)
        await consumer.stop()
        return task

    task = asyncio.run(scenario())

    assert task.cancelled()
    assert kafka.stopped


def test_el_consumer_recorta_el_contenido_a_inicio_mas_final():
    llm = FakeLlmClient()
    content = "#" * (settings.max_summary_chars + 5000)
    half = settings.max_summary_chars // 2
    consumer = build_consumer(llm=llm, content=content)

    asyncio.run(_consume_all(consumer))

    prompt = llm.prompts[0]
    assert prompt.count("#") == settings.max_summary_chars
    assert prompt.endswith(content[-half:])
    assert content[:half] in prompt
    assert "recortado" in prompt


def test_el_consumer_no_trunca_un_contenido_corto():
    llm = FakeLlmClient()
    consumer = build_consumer(llm=llm, content="texto corto")

    asyncio.run(_consume_all(consumer))

    prompt = llm.prompts[0]
    assert prompt.endswith("texto corto")
    assert "recortado" not in prompt


def test_el_consumer_respeta_el_tope_configurado():
    llm = FakeLlmClient()
    content = "a" * 50 + "b" * 50
    consumer = build_consumer(llm=llm, content=content, config=Settings(max_summary_chars=20))

    asyncio.run(_consume_all(consumer))

    prompt = llm.prompts[0]
    assert content[:10] in prompt
    assert content[-10:] in prompt
    assert "a" * 11 not in prompt
    assert "b" * 11 not in prompt
    assert "20 caracteres" in prompt


def test_hereda_el_request_id_del_evento():
    llm = RecordingLlmClient()
    consumer = build_consumer(
        llm=llm, kafka=FakeKafkaConsumer([event_message(request_id="req-abc")])
    )

    asyncio.run(_consume_all(consumer))

    assert llm.request_ids == ["req-abc"]


@pytest.mark.parametrize("request_id", [None, "", "-"])
def test_genera_un_request_id_si_el_evento_no_lo_trae(request_id):
    llm = RecordingLlmClient()
    consumer = build_consumer(
        llm=llm, kafka=FakeKafkaConsumer([event_message(request_id=request_id)])
    )

    asyncio.run(_consume_all(consumer))

    assert len(llm.request_ids) == 1
    assert llm.request_ids[0] not in {"", "-"}


def test_no_filtra_el_request_id_entre_mensajes():
    llm = RecordingLlmClient()
    kafka = FakeKafkaConsumer(
        [
            event_message("doc-1", request_id="req-1", offset=0),
            event_message("doc-2", request_id="req-2", offset=1),
            event_message("doc-3", offset=2),
        ]
    )
    consumer = build_consumer(llm=llm, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert llm.request_ids[:2] == ["req-1", "req-2"]
    generado = llm.request_ids[2]
    assert generado not in {"", "-", "req-1", "req-2"}


def test_los_logs_del_consumer_llevan_el_request_id_heredado():
    consumer = build_consumer(kafka=FakeKafkaConsumer([event_message(request_id="req-abc")]))

    with formatted_logs() as stream:
        asyncio.run(_consume_all(consumer))

    salida = stream.getvalue()
    assert "resumen completado" in salida
    assert "req-abc" in salida


def test_el_request_id_no_se_filtra_al_mensaje_siguiente_en_los_logs():
    kafka = FakeKafkaConsumer(
        [event_message("doc-1", request_id="req-abc"), event_message("doc-2")]
    )
    consumer = build_consumer(kafka=kafka)

    with formatted_logs() as stream:
        asyncio.run(_consume_all(consumer))

    completados = [
        line for line in stream.getvalue().splitlines() if "resumen completado" in line
    ]
    assert len(completados) == 2
    assert "req-abc" in completados[0]
    assert "req-abc" not in completados[1]


def test_propaga_el_request_id_a_persistence():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"id": "doc-1", "content": DEFAULT_CONTENT, "checksum": "abc"}
        )

    consumer = build_consumer(
        kafka=FakeKafkaConsumer([event_message(request_id="req-abc")]),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    asyncio.run(_consume_all(consumer))

    assert requests[0].headers["x-request-id"] == "req-abc"


def test_restaura_el_request_id_previo_tras_un_exito():
    consumer = build_consumer(kafka=FakeKafkaConsumer())

    async def scenario():
        token = set_request_id("req-previo")
        try:
            await consumer.process_message(event_message(request_id="req-evento"))
            return get_request_id()
        finally:
            reset_request_id(token)

    assert asyncio.run(scenario()) == "req-previo"


def test_restaura_el_request_id_previo_tras_un_error():
    kafka = FakeKafkaConsumer()
    consumer = build_consumer(llm=FakeLlmClient(error=LlmTimeoutError()), kafka=kafka)

    async def scenario():
        token = set_request_id("req-previo")
        try:
            await consumer.process_message(event_message(request_id="req-evento"))
            return get_request_id()
        finally:
            reset_request_id(token)

    assert asyncio.run(scenario()) == "req-previo"
    assert kafka.commits == 0

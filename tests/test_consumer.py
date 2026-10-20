"""Tests del consumer de Kafka: procesamiento, idempotencia y commit de offset.

Reemplazan a los viejos tests de jobs en background (``test_async_summary.py``):
el trigger del resumen ya no es una tarea asyncio en el proceso, sino el
consumer de ``documents.extracted`` corriendo en el lifespan.
"""

import asyncio
import json
from types import SimpleNamespace

from fakes import (
    DEFAULT_CONTENT,
    FakeKafkaConsumer,
    FakeLlmClient,
    FakeSummaryCache,
    document_client,
)

from app import (
    JOB_STATE_DONE,
    JOB_STATE_PROCESSING,
    Settings,
    build_summary_service,
    settings,
)
from cache import SummaryCache
from consumer import KafkaSummaryConsumer
from errors import LlmTimeoutError


def event_message(document_id: str = "doc-1") -> SimpleNamespace:
    """Mensaje con el schema v1 del evento document.extracted (de extraction)."""
    payload = {
        "schema_version": 1,
        "document_id": document_id,
        "checksum": "abc",
        "source": "extraction",
    }
    return SimpleNamespace(value=json.dumps(payload).encode("utf-8"))


def build_consumer(
    llm: FakeLlmClient | None = None,
    cache: FakeSummaryCache | None = None,
    content: str = DEFAULT_CONTENT,
    kafka: FakeKafkaConsumer | None = None,
    config: Settings | None = None,
    status_code: int = 200,
) -> KafkaSummaryConsumer:
    service = build_summary_service(
        document_client=document_client(content, status_code=status_code),
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
    kafka = FakeKafkaConsumer([event_message("doc-1"), event_message("doc-2")])
    consumer = build_consumer(llm=llm, cache=cache, kafka=kafka)

    asyncio.run(_consume_all(consumer))

    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_PROCESSING
    assert json.loads(cache.data["summary:doc-2"])["status"] == JOB_STATE_PROCESSING
    assert kafka.commits == 0


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
            SimpleNamespace(value=b"no es json"),
            SimpleNamespace(value=json.dumps({"sin": "document_id"}).encode("utf-8")),
            event_message(),
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


def test_start_sin_broker_no_tumba_al_servicio():
    kafka = FakeKafkaConsumer(start_error=RuntimeError("connection refused"))
    consumer = build_consumer(kafka=kafka)

    async def scenario():
        await consumer.start()
        return consumer._task

    assert asyncio.run(scenario()) is None


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

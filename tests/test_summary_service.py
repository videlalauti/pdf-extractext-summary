"""Tests del contrato HTTP de resumen: POST acepta, GET consulta, health y Settings."""

import asyncio
import json

import pytest
from fakes import FakeLlmClient, FakeSummaryCache, FakeSummaryConsumer, document_client
from pydantic import ValidationError

from app import (
    JOB_STATE_DONE,
    JOB_STATE_PROCESSING,
    JOB_STATE_QUEUED,
    Settings,
    build_summary_service,
    settings,
)
from llm import LlmClient


def _seed(cache: FakeSummaryCache, document_id: str, status: str, summary: str | None = None):
    """Deja un estado de job en la caché, como lo dejaría el consumer."""
    job = {"status": status}
    if summary is not None:
        job["summary"] = summary
    cache.data[f"summary:{document_id}"] = json.dumps(job)


def test_health_healthy_cuando_ollama_y_kafka_responden(build_client):
    with build_client(llm_client=FakeLlmClient(available=True)) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "summary-service",
        "ollama": "reachable",
        "kafka": "connected",
    }


def test_health_unhealthy_cuando_ollama_no_responde(build_client):
    with build_client(llm_client=FakeLlmClient(available=False)) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "service": "summary-service",
        "ollama": "unreachable",
        "kafka": "connected",
    }


def test_health_unhealthy_cuando_kafka_esta_desconectado(build_client):
    consumer = FakeSummaryConsumer(connected=False)

    with build_client(llm_client=FakeLlmClient(available=True), consumer=consumer) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "service": "summary-service",
        "ollama": "reachable",
        "kafka": "disconnected",
    }


def test_fake_del_puerto_cumple_el_protocolo():
    assert isinstance(FakeLlmClient(), LlmClient)


def test_el_tope_de_contexto_tiene_que_permitir_mitad_y_mitad():
    with pytest.raises(ValidationError):
        Settings(max_summary_chars=1)


# --------------------------------------------------------------------------- #
# POST: acepta el trabajo; el trigger es el consumer de Kafka
# --------------------------------------------------------------------------- #


def test_post_responde_202_queued_cuando_no_hay_resumen(build_client):
    with build_client(llm_client=FakeLlmClient(), cache=FakeSummaryCache()) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED}


def test_post_dispara_el_procesamiento_aunque_no_llegue_el_evento(build_client):
    """Red de seguridad: el POST agenda process y no depende solo de Kafka."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED}
    assert len(llm.prompts) == 1
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_DONE


def test_post_con_job_queued_reintenta_el_procesamiento(build_client):
    """Un job que quedó encolado sin evento también se destraba con el POST."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED}
    assert len(llm.prompts) == 1


def test_post_con_job_processing_no_agenda_otra_inferencia(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert llm.prompts == []


def test_post_devuelve_200_cuando_el_resumen_ya_esta_cacheado(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="ya resumido")

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "ya resumido", "document_id": "doc-1"}
    assert llm.prompts == []


def test_post_devuelve_202_con_el_estado_actual_si_esta_en_curso(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(llm_client=FakeLlmClient(), cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_PROCESSING}


def test_post_responde_202_aun_con_redis_caido(build_client):
    """Sin Redis no hay resultado cacheado: 202 y el trabajo lo hace el consumer."""
    with build_client(llm_client=FakeLlmClient(), cache=FakeSummaryCache(fail_open=True)) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202


# --------------------------------------------------------------------------- #
# GET: consulta el estado que dejan POST y el consumer
# --------------------------------------------------------------------------- #


def test_get_devuelve_200_con_el_resumen_done(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="resumen de prueba")

    with build_client(cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}


def test_get_devuelve_202_mientras_el_consumer_procesa(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_PROCESSING}


def test_get_devuelve_200_tras_el_post_que_dispara_el_resumen(build_client):
    with build_client(cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        response = client.get("/summary/doc-1")

    assert response.status_code == 200
    assert response.json()["summary"] == "resumen de prueba"


def test_get_sin_resumen_devuelve_404(build_client):
    with build_client(cache=FakeSummaryCache()) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 404


# --------------------------------------------------------------------------- #
# process: idempotencia y concurrencia entre consumer y background
# --------------------------------------------------------------------------- #


def _service(llm: FakeLlmClient, cache: FakeSummaryCache):
    return build_summary_service(
        document_client=document_client(),
        llm_client=llm,
        cache=cache,
        config=settings,
    )


def test_process_no_vuelve_a_inferir_un_job_done():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="ya resumido")

    assert asyncio.run(_service(llm, cache).process("doc-1")) is True

    assert llm.prompts == []
    assert json.loads(cache.data["summary:doc-1"])["summary"] == "ya resumido"


def test_consumer_y_background_comparten_el_lock_por_documento():
    """Dos instancias del servicio (consumer y POST) infieren una sola vez."""
    llm = FakeLlmClient(delay=0.05)
    cache = FakeSummaryCache()
    consumer_service = _service(llm, cache)
    background_service = _service(llm, cache)

    async def scenario():
        return await asyncio.gather(
            consumer_service.process("doc-1"),
            background_service.process("doc-1"),
        )

    assert asyncio.run(scenario()) == [True, True]
    assert len(llm.prompts) == 1
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_DONE

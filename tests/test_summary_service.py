"""Tests del contrato HTTP de resumen: POST acepta, GET consulta, health y Settings."""

import json

import pytest
from fakes import FakeLlmClient, FakeSummaryCache
from pydantic import ValidationError

from app import (
    JOB_STATE_DONE,
    JOB_STATE_PROCESSING,
    JOB_STATE_QUEUED,
    Settings,
)
from llm import LlmClient


def _seed(cache: FakeSummaryCache, document_id: str, status: str, summary: str | None = None):
    """Deja un estado de job en la caché, como lo dejaría el consumer."""
    job = {"status": status}
    if summary is not None:
        job["summary"] = summary
    cache.data[f"summary:{document_id}"] = json.dumps(job)


def test_health_healthy_cuando_ollama_responde(build_client):
    with build_client(llm_client=FakeLlmClient(available=True)) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "summary-service",
        "ollama": "reachable",
    }


def test_health_unhealthy_cuando_ollama_no_responde(build_client):
    with build_client(llm_client=FakeLlmClient(available=False)) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"


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


def test_post_no_dispara_el_procesamiento(build_client):
    """Ni llama al LLM ni lanza tareas: el resumen lo dispara el evento."""
    llm = FakeLlmClient()

    with build_client(llm_client=llm, cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        assert client.post("/summary/doc-1").status_code == 202

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


def test_get_devuelve_202_queued_tras_el_post(build_client):
    with build_client(cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED}


def test_get_sin_resumen_devuelve_404(build_client):
    with build_client(cache=FakeSummaryCache()) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 404

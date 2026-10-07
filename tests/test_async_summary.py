"""Tests del flujo asíncrono del resumen: estados en Redis, idempotencia y degrade."""

import time

from fakes import FakeLlmClient, FakeSummaryCache

from app import settings
from cache import SummaryCache
from errors import LlmTimeoutError


def _wait_until_done(client, document_id: str, timeout: float = 5.0):
    """Espera a que el job en background termine y devuelve la respuesta GET final."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/summary/{document_id}")
        if response.status_code == 200:
            return response
        time.sleep(0.02)
    raise AssertionError("el job no terminó a tiempo")


def test_fake_summary_cache_cumple_el_protocolo():
    assert isinstance(FakeSummaryCache(), SummaryCache)


def test_post_encola_y_responde_202_queued(build_client):
    with build_client(llm_client=FakeLlmClient(), cache=FakeSummaryCache()) as client:
        response = client.post("/summary/doc-1")

        assert response.status_code == 202
        assert response.json() == {"status": "queued"}
        _wait_until_done(client, "doc-1")


def test_get_devuelve_processing_mientras_corre(build_client):
    llm = FakeLlmClient(delay=0.5)

    with build_client(llm_client=llm, cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        deadline = time.monotonic() + 5
        while not llm.prompts and time.monotonic() < deadline:
            time.sleep(0.01)
        response = client.get("/summary/doc-1")
        _wait_until_done(client, "doc-1")

    assert response.status_code == 202
    assert response.json()["status"] == "processing"


def test_get_devuelve_el_summary_al_terminar(build_client):
    cache = FakeSummaryCache()

    with build_client(llm_client=FakeLlmClient(), cache=cache) as client:
        assert client.post("/summary/doc-1").status_code == 202
        response = _wait_until_done(client, "doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}
    assert cache.data["summary:doc-1"]


def test_post_con_doc_ya_resumido_responde_al_instante_sin_reinferir(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()

    with build_client(llm_client=llm, cache=cache) as client:
        assert client.post("/summary/doc-1").status_code == 202
        _wait_until_done(client, "doc-1")
        n_generates = len(llm.models)
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}
    assert len(llm.models) == n_generates


def test_post_no_duplica_job_si_ya_esta_en_curso(build_client):
    llm = FakeLlmClient(delay=0.5)

    with build_client(llm_client=llm, cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        deadline = time.monotonic() + 5
        while not llm.prompts and time.monotonic() < deadline:
            time.sleep(0.01)
        second = client.post("/summary/doc-1")
        _wait_until_done(client, "doc-1")

    assert second.status_code == 202
    assert len(llm.models) == 1


def test_post_degrada_a_sincrono_cuando_redis_no_responde(build_client):
    llm = FakeLlmClient()

    with build_client(llm_client=llm, cache=FakeSummaryCache(fail_open=True)) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}
    assert llm.models == [settings.ollama_model]


def test_post_degrada_a_sincrono_y_respeta_errores_de_dominio(build_client):
    llm = FakeLlmClient(error=LlmTimeoutError())

    with build_client(llm_client=llm, cache=FakeSummaryCache(fail_open=True)) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 504


def test_get_sin_job_ni_resumen_devuelve_404(build_client):
    with build_client(llm_client=FakeLlmClient(), cache=FakeSummaryCache()) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 404

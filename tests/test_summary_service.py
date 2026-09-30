"""Tests del servicio de resumen: endpoints, recorte de contexto y errores de Ollama."""

import pytest
from fakes import FakeLlmClient
from pydantic import ValidationError

from app import Settings, get_settings, settings
from errors import (
    LlmModelNotFoundError,
    LlmPromptRejectedError,
    LlmTimeoutError,
    LlmUnavailableError,
)
from llm import LlmClient
from main import app


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


def test_summary_devuelve_resumen_y_documento(build_client, llm):
    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}
    assert llm.models == [settings.ollama_model]


def test_fake_del_puerto_cumple_el_protocolo():
    assert isinstance(FakeLlmClient(), LlmClient)


def test_summary_recorta_el_contenido_a_inicio_mas_final(build_client, llm):
    content = "#" * (settings.max_summary_chars + 5000)
    half = settings.max_summary_chars // 2

    with build_client(llm_client=llm, content=content) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    prompt = llm.prompts[0]
    assert prompt.count("#") == settings.max_summary_chars
    assert prompt.endswith(content[-half:])
    assert content[:half] in prompt
    assert "recortado" in prompt


def test_summary_avisa_que_falta_la_parte_del_medio(build_client, llm):
    content = "#" * (settings.max_summary_chars + 5000)

    with build_client(llm_client=llm, content=content) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert "omitido" in llm.prompts[0]


def test_summary_no_trunca_un_contenido_corto(build_client, llm):
    with build_client(llm_client=llm, content="texto corto") as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    prompt = llm.prompts[0]
    assert prompt.endswith("texto corto")
    assert "recortado" not in prompt


def test_summary_respeta_el_tope_configurado(build_client, llm):
    app.dependency_overrides[get_settings] = lambda: Settings(max_summary_chars=20)
    content = "a" * 50 + "b" * 50
    try:
        with build_client(llm_client=llm, content=content) as client:
            response = client.post("/summary/doc-1")
    finally:
        app.dependency_overrides.pop(get_settings, None)

    assert response.status_code == 200
    prompt = llm.prompts[0]
    assert content[:10] in prompt
    assert content[-10:] in prompt
    assert "a" * 11 not in prompt
    assert "b" * 11 not in prompt
    assert "20 caracteres" in prompt


def test_el_tope_de_contexto_tiene_que_permitir_mitad_y_mitad():
    with pytest.raises(ValidationError):
        Settings(max_summary_chars=1)


def test_summary_documento_no_encontrado(build_client):
    with build_client(status_code=404) as client:
        response = client.post("/summary/no-existe")

    assert response.status_code == 404
    assert response.json()["detail"] == "Documento no encontrado"


def test_summary_persistence_no_disponible(build_client):
    with build_client(connect_error=True) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 502


def test_summary_modelo_inexistente_devuelve_503_de_configuracion(build_client, llm):
    llm.error = LlmModelNotFoundError("modelo-fantasma")

    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "modelo-fantasma" in detail
    assert "OLLAMA_MODEL" in detail


def test_summary_prompt_rechazado_devuelve_422(build_client, llm):
    llm.error = LlmPromptRejectedError()

    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 422
    assert "MAX_SUMMARY_CHARS" in response.json()["detail"]


def test_summary_ollama_no_disponible_devuelve_502(build_client, llm):
    llm.error = LlmUnavailableError()

    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 502
    assert response.json()["detail"] == "Ollama no está disponible"


def test_summary_timeout_de_ollama_devuelve_504(build_client, llm):
    llm.error = LlmTimeoutError()

    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 504
    assert response.json()["detail"] == "Ollama tardó demasiado en responder"


@pytest.mark.parametrize("error", [LlmTimeoutError, LlmUnavailableError])
def test_summary_no_expone_internals_de_httpx(build_client, llm, error):
    llm.error = error

    with build_client(llm_client=llm) as client:
        response = client.post("/summary/doc-1")

    assert "httpx" not in response.json()["detail"]
    assert "Traceback" not in response.json()["detail"]


def test_summary_error_inesperado_devuelve_500_sin_internals(build_client, llm):
    llm.error = RuntimeError("se rompio algo interno")

    with build_client(llm_client=llm, raise_server_exceptions=False) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 500
    assert response.json() == {"detail": "Error interno"}

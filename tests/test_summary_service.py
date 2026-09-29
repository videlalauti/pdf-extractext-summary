"""Tests del servicio de resumen: recorte de contexto y errores accionables."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi.testclient import TestClient

from app import settings
from main import app

client = TestClient(app)


def document_response(content: str = "texto de prueba") -> MagicMock:
    return MagicMock(
        status_code=200,
        json=lambda: {"id": "doc-1", "content": content, "checksum": "abc"},
        raise_for_status=lambda: None,
    )


def ollama_response(status_code: int = 200, **payload) -> MagicMock:
    return MagicMock(
        status_code=status_code,
        json=lambda: payload,
        raise_for_status=lambda: None,
    )


def failing_status(status_code: int) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError(
        f"status {status_code}",
        request=MagicMock(),
        response=MagicMock(status_code=status_code),
    )


def post_side_effect(error: Exception) -> AsyncMock:
    return AsyncMock(side_effect=error)


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "summary-service"


def test_summary_returns_summary():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post",
        new=AsyncMock(return_value=ollama_response(response="resumen de prueba")),
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}


def test_summary_sends_correct_payload_to_ollama():
    captured_payload = {}

    async def mock_post(url, json=None, **kwargs):
        captured_payload["url"] = url
        captured_payload["json"] = json
        return ollama_response(response="resumen")

    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post", new=AsyncMock(side_effect=mock_post)
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert captured_payload["url"] == f"{settings.ollama_url}/api/generate"
    payload = captured_payload["json"]
    assert payload["stream"] is False
    assert payload["model"] == settings.ollama_model
    assert payload["options"]["num_predict"] == 300
    assert "prompt" in payload
    assert "texto" in payload["prompt"]


def test_summary_truncates_long_content_to_max_summary_chars():
    captured_payload = {}

    async def mock_post(url, json=None, **kwargs):
        captured_payload.update(json)
        return ollama_response(response="resumen")

    content = "#" * (settings.max_summary_chars + 5000)
    with patch(
        "httpx.AsyncClient.get", new=AsyncMock(return_value=document_response(content))
    ), patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=mock_post)):
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    prompt = captured_payload["prompt"]
    assert prompt.count("#") == settings.max_summary_chars
    assert "truncado" in prompt


def test_summary_when_document_not_found():
    with patch("httpx.AsyncClient.get", new=post_side_effect(failing_status(404))):
        response = client.post("/summary/no-existe")

    assert response.status_code == 404
    assert response.json()["detail"] == "Documento no encontrado"


def test_summary_when_persistence_is_down():
    with patch(
        "httpx.AsyncClient.get",
        new=post_side_effect(httpx.ConnectError("connection refused")),
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 502
    assert response.json()["detail"] == "El persistence-service no está disponible"


def test_summary_when_model_does_not_exist_returns_503():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post", new=post_side_effect(failing_status(404))
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert settings.ollama_model in detail
    assert "OLLAMA_MODEL" in detail


def test_summary_when_prompt_is_too_long_returns_422():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post", new=post_side_effect(failing_status(400))
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 422
    assert "MAX_SUMMARY_CHARS" in response.json()["detail"]


def test_summary_when_ollama_fails_returns_502():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post", new=post_side_effect(failing_status(500))
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 502
    assert "httpx" not in response.json()["detail"]


def test_summary_when_ollama_times_out_returns_504():
    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=document_response())), patch(
        "httpx.AsyncClient.post",
        new=post_side_effect(httpx.ReadTimeout("timeout")),
    ):
        response = client.post("/summary/doc-1")

    assert response.status_code == 504
    assert response.json()["detail"] == "Ollama tardó demasiado en responder"

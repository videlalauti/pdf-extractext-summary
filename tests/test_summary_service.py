import httpx
from app import settings
from fastapi.testclient import TestClient
from main import app
from unittest.mock import AsyncMock, MagicMock, patch

client = TestClient(app)


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "summary-service"


def test_summary_returns_summary():
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(
            return_value=MagicMock(
                status_code=200,
                json=lambda: {"id": "doc-1", "content": "texto largo", "checksum": "abc"},
                raise_for_status=lambda: None,
            )
        ),
    ):
        with patch(
            "httpx.AsyncClient.post",
            new=AsyncMock(
                return_value=MagicMock(
                    status_code=200,
                    json=lambda: {"response": "resumen de prueba"},
                    raise_for_status=lambda: None,
                )
            ),
        ):
            response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json()["summary"] == "resumen de prueba"
    assert response.json()["document_id"] == "doc-1"


def test_summary_when_document_not_found():
    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(
            return_value=MagicMock(
                status_code=404,
                raise_for_status=lambda: (_ for _ in ()).throw(
                    httpx.HTTPStatusError(
                        "404",
                        request=MagicMock(),
                        response=MagicMock(status_code=404),
                    )
                ),
            )
        ),
    ):
        response = client.post("/summary/no-existe")

    assert response.status_code == 404


def test_summary_sends_correct_payload_to_ollama():
    captured_payload = {}

    async def mock_post(url, json=None, **kwargs):
        captured_payload["url"] = url
        captured_payload["json"] = json
        return MagicMock(
            status_code=200,
            json=lambda: {"response": "resumen"},
            raise_for_status=lambda: None,
        )

    with patch(
        "httpx.AsyncClient.get",
        new=AsyncMock(
            return_value=MagicMock(
                status_code=200,
                json=lambda: {"id": "doc-1", "content": "texto", "checksum": "abc"},
                raise_for_status=lambda: None,
            )
        ),
    ):
        with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=mock_post)):
            response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert captured_payload["url"] == f"{settings.ollama_url}/api/generate"
    payload = captured_payload["json"]
    assert payload["stream"] is False
    assert payload["model"] == settings.ollama_model
    assert payload["options"]["num_predict"] == 300
    assert "prompt" in payload
    assert "texto" in payload["prompt"]
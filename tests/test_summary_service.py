import httpx
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
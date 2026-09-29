"""Fixtures compartidas: cliente HTTP con las dependencias del puerto inyectadas."""

from collections.abc import Callable, Iterator

import pytest
from fakes import DEFAULT_CONTENT, FakeLlmClient, document_client
from fastapi.testclient import TestClient

from main import app
from routes import get_document_client, get_llm_client


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def llm() -> FakeLlmClient:
    return FakeLlmClient()


@pytest.fixture
def build_client() -> Iterator[Callable[..., TestClient]]:
    """Devuelve un TestClient con LLM y persistence resueltos por `Depends`."""

    def build(
        *,
        llm_client: FakeLlmClient | None = None,
        content: str = DEFAULT_CONTENT,
        status_code: int = 200,
        connect_error: bool = False,
    ) -> TestClient:
        fake_llm = llm_client if llm_client is not None else FakeLlmClient()
        persistence = document_client(
            content, status_code=status_code, connect_error=connect_error
        )
        app.dependency_overrides[get_llm_client] = lambda: fake_llm
        app.dependency_overrides[get_document_client] = lambda: persistence
        return TestClient(app)

    try:
        yield build
    finally:
        app.dependency_overrides.clear()

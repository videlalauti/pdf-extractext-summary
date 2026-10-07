"""Fixtures compartidas: cliente HTTP con las dependencias del puerto inyectadas."""

from collections.abc import Callable, Iterator

import pytest
from fakes import DEFAULT_CONTENT, FakeLlmClient, FakeSummaryCache, document_client
from fastapi.testclient import TestClient

from cache import NoopSummaryCache
from main import app
from routes import get_document_client, get_llm_client, get_summary_cache


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def llm() -> FakeLlmClient:
    return FakeLlmClient()


@pytest.fixture
def build_client() -> Iterator[Callable[..., TestClient]]:
    """Devuelve un TestClient con LLM, persistence y caché resueltos por `Depends`."""

    def build(
        *,
        llm_client: FakeLlmClient | None = None,
        content: str = DEFAULT_CONTENT,
        status_code: int = 200,
        connect_error: bool = False,
        raise_server_exceptions: bool = True,
        cache: FakeSummaryCache | NoopSummaryCache | None = None,
    ) -> TestClient:
        fake_llm = llm_client if llm_client is not None else FakeLlmClient()
        persistence = document_client(
            content, status_code=status_code, connect_error=connect_error
        )
        fake_cache = cache if cache is not None else NoopSummaryCache()
        app.dependency_overrides[get_llm_client] = lambda: fake_llm
        app.dependency_overrides[get_document_client] = lambda: persistence
        app.dependency_overrides[get_summary_cache] = lambda: fake_cache
        return TestClient(app, raise_server_exceptions=raise_server_exceptions)

    try:
        yield build
    finally:
        app.dependency_overrides.clear()

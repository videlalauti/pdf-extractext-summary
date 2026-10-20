"""Fixtures compartidas: cliente HTTP con las dependencias del puerto inyectadas."""

from collections.abc import Callable, Iterator

import pytest
from fakes import (
    DEFAULT_CONTENT,
    FakeKafkaConsumer,
    FakeLlmClient,
    FakeSummaryCache,
    document_client,
)
from fastapi.testclient import TestClient

import main
from cache import NoopSummaryCache
from consumer import KafkaSummaryConsumer
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
    """Devuelve un TestClient con LLM, persistence y caché resueltos por `Depends`.

    También reemplaza el consumer de Kafka del lifespan por un fake: los tests
    no hablan con ningún broker (y el real tarda ~4 s en fallar sin uno).
    """

    def build(
        *,
        llm_client: FakeLlmClient | None = None,
        content: str = DEFAULT_CONTENT,
        cache: FakeSummaryCache | NoopSummaryCache | None = None,
    ) -> TestClient:
        fake_llm = llm_client if llm_client is not None else FakeLlmClient()
        persistence = document_client(content)
        fake_cache = cache if cache is not None else NoopSummaryCache()
        app.dependency_overrides[get_llm_client] = lambda: fake_llm
        app.dependency_overrides[get_document_client] = lambda: persistence
        app.dependency_overrides[get_summary_cache] = lambda: fake_cache
        return TestClient(app)

    original_factory = main.build_summary_consumer
    main.build_summary_consumer = lambda summary_service: KafkaSummaryConsumer(
        summary_service, "kafka:9092", consumer=FakeKafkaConsumer()
    )
    try:
        yield build
    finally:
        main.build_summary_consumer = original_factory
        app.dependency_overrides.clear()

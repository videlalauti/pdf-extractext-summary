"""Summary service FastAPI application: bootstrap y montaje de componentes."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from shared.web.cors import add_cors
from shared.web.logging import RequestIdMiddleware, TimingMiddleware, setup_logging

from app import SummaryService, build_summary_service, settings
from cache import RedisSummaryCache
from consumer import KafkaSummaryConsumer
from error_handlers import register_error_handlers
from llm import OllamaLlmClient
from routes import router

SERVICE_NAME = "summary-service"


def build_summary_consumer(summary_service: SummaryService) -> KafkaSummaryConsumer:
    """Compone el consumer de Kafka; los tests lo reemplazan por un fake sin broker."""
    return KafkaSummaryConsumer(summary_service, settings.kafka_bootstrap_servers)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        document_client = httpx.AsyncClient(timeout=settings.persistence_timeout_seconds)
        llm_client = OllamaLlmClient(
            base_url=settings.ollama_url, timeout=settings.ollama_timeout_seconds
        )
        summary_cache = RedisSummaryCache(
            settings.redis_url,
            ttl_seconds=settings.summary_cache_ttl_seconds,
            enabled=settings.summary_cache_enabled,
        )
        summary_service = build_summary_service(
            document_client=document_client,
            llm_client=llm_client,
            cache=summary_cache,
            config=settings,
        )
        # El trigger del resumen es el consumer, no el request: corre como
        # tarea en el lifespan y se cierra ordenadamente en el shutdown.
        consumer = build_summary_consumer(summary_service)
        application.state.document_client = document_client
        application.state.llm_client = llm_client
        application.state.summary_cache = summary_cache
        application.state.summary_consumer = consumer
        try:
            await consumer.start()
            yield
        finally:
            await consumer.stop()
            await summary_cache.aclose()
            await llm_client.aclose()
            await document_client.aclose()

    application = FastAPI(title="PDF Summary Service", version="1.0.0", lifespan=lifespan)
    # Timing se registra antes de RequestId (en Starlette el último queda más
    # afuera): así corre por dentro y su log de duración lleva el request_id.
    application.add_middleware(TimingMiddleware)
    application.add_middleware(RequestIdMiddleware)
    add_cors(application)
    register_error_handlers(application)
    application.include_router(router)
    return application


setup_logging(SERVICE_NAME, level=getattr(logging, settings.log_level.upper(), logging.INFO))
app = create_app()

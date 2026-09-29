"""Summary service FastAPI application: bootstrap y montaje de componentes."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from shared.web.cors import add_cors
from shared.web.logging import RequestIdMiddleware, setup_logging

from app import settings
from error_handlers import register_error_handlers
from llm import OllamaLlmClient
from routes import router

SERVICE_NAME = "summary-service"


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        document_client = httpx.AsyncClient(timeout=settings.persistence_timeout_seconds)
        llm_client = OllamaLlmClient(
            base_url=settings.ollama_url, timeout=settings.ollama_timeout_seconds
        )
        application.state.document_client = document_client
        application.state.llm_client = llm_client
        try:
            yield
        finally:
            await llm_client.aclose()
            await document_client.aclose()

    application = FastAPI(title="PDF Summary Service", version="1.0.0", lifespan=lifespan)
    application.add_middleware(RequestIdMiddleware)
    add_cors(application)
    register_error_handlers(application)
    application.include_router(router)
    return application


setup_logging(SERVICE_NAME, level=getattr(logging, settings.log_level.upper(), logging.INFO))
app = create_app()

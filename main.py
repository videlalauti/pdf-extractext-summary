"""Summary service FastAPI application: bootstrap y montaje de componentes."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import settings
from error_handlers import register_error_handlers
from llm import OllamaLlmClient
from middleware import RequestIdLoggingMiddleware
from routes import router


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

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    application = FastAPI(title="PDF Summary Service", version="1.0.0", lifespan=lifespan)
    application.add_middleware(RequestIdLoggingMiddleware)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )
    register_error_handlers(application)
    application.include_router(router)
    return application


app = create_app()

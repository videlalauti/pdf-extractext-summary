"""Traducción de errores de dominio a respuestas HTTP, sin filtrar internals."""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from errors import SummaryError

logger = logging.getLogger(__name__)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(SummaryError)
    async def handle_summary_error(request: Request, exc: SummaryError) -> JSONResponse:
        logger.info("error de dominio en %s: %s", request.url.path, type(exc).__name__)
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # El request_id lo agrega el formatter de shared.web.logging a cada record.
        logger.exception("error inesperado en %s", request.url.path)
        return JSONResponse(status_code=500, content={"detail": "Error interno"})

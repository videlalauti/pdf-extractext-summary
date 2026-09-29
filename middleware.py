"""Middlewares HTTP: correlación de requests y logging de acceso."""

import logging
import time
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"


class RequestIdLoggingMiddleware(BaseHTTPMiddleware):
    """Propaga un request-id y registra método, ruta, status y duración."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid4().hex
        request.state.request_id = request_id
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "%s %s falló [request_id=%s]",
                request.method,
                request.url.path,
                request_id,
            )
            raise
        duration_ms = (time.perf_counter() - started) * 1000
        logger.info(
            "%s %s -> %s en %.1fms [request_id=%s]",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            request_id,
        )
        response.headers[REQUEST_ID_HEADER] = request_id
        return response

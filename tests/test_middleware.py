"""Registro del TimingMiddleware dentro del stack de middleware de la app."""

from shared.web.logging import RequestIdMiddleware, TimingMiddleware

from main import app


def test_timing_middleware_corre_por_dentro_de_request_id():
    """Timing va dentro de RequestId para que su log de duración lleve el request_id.

    En Starlette el último `add_middleware` queda más afuera del stack.
    """
    registrados = [middleware.cls for middleware in app.user_middleware]

    assert TimingMiddleware in registrados
    assert registrados.index(TimingMiddleware) > registrados.index(RequestIdMiddleware)

"""Tests de las métricas de tiempo del resumen: fetch vs Ollama, éxito y fallo."""

import asyncio
import json
import logging
import re

import httpx
from fakes import DEFAULT_CONTENT, FakeLlmClient, FakeSummaryCache

from app import JOB_STATE_PROCESSING, build_summary_service, settings
from errors import LlmTimeoutError

COMPLETED_PATTERN = re.compile(
    r"resumen completado document_id=(\S+) fetch_ms=(\d+\.\d+) "
    r"ollama_ms=(\d+\.\d+) total_ms=(\d+\.\d+)"
)
FAILED_PATTERN = re.compile(
    r"resumen fallido document_id=(\S+) fetch_ms=(\d+\.\d+) "
    r"ollama_ms=(\d+\.\d+) total_ms=(\d+\.\d+) error=(\S+)"
)


def _service(
    llm: FakeLlmClient,
    *,
    content: str = DEFAULT_CONTENT,
    status_code: int = 200,
    fetch_delay: float = 0.0,
    cache: FakeSummaryCache | None = None,
):
    """Servicio con persistence simulada; el retardo hace medibles los tiempos."""

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(fetch_delay)
        if status_code != 200:
            return httpx.Response(status_code, json={"detail": "documento no encontrado"})
        return httpx.Response(
            200, json={"id": "doc-1", "content": content, "checksum": "abc"}
        )

    return build_summary_service(
        document_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        llm_client=llm,
        cache=cache if cache is not None else FakeSummaryCache(),
        config=settings,
    )


def _records(caplog, prefix: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "app" and record.getMessage().startswith(prefix)
    ]


def test_loguea_los_tiempos_de_un_resumen_exitoso(caplog):
    llm = FakeLlmClient(delay=0.05)
    service = _service(llm, fetch_delay=0.02)
    caplog.set_level(logging.INFO)

    assert asyncio.run(service.process("doc-1")) is True

    completed = _records(caplog, "resumen completado")
    assert len(completed) == 1
    match = COMPLETED_PATTERN.fullmatch(completed[0].getMessage())
    assert match is not None
    document_id, fetch_ms, ollama_ms, total_ms = match.groups()
    assert document_id == "doc-1"
    assert float(fetch_ms) > 0
    assert float(ollama_ms) >= 40
    assert float(total_ms) >= float(fetch_ms) + float(ollama_ms) - 1
    assert _records(caplog, "resumen fallido") == []


def test_loguea_los_tiempos_cuando_ollama_timeoutea(caplog):
    llm = FakeLlmClient(error=LlmTimeoutError(), delay=0.05)
    cache = FakeSummaryCache()
    service = _service(llm, fetch_delay=0.02, cache=cache)
    caplog.set_level(logging.INFO)

    assert asyncio.run(service.process("doc-1")) is False

    failed = _records(caplog, "resumen fallido")
    assert len(failed) == 1
    match = FAILED_PATTERN.fullmatch(failed[0].getMessage())
    assert match is not None
    document_id, fetch_ms, ollama_ms, total_ms, error = match.groups()
    assert document_id == "doc-1"
    assert float(fetch_ms) > 0
    assert float(ollama_ms) >= 40
    assert float(total_ms) >= float(ollama_ms)
    assert error == "LlmTimeoutError"
    assert failed[0].exc_info is not None
    assert _records(caplog, "resumen completado") == []
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_PROCESSING


def test_loguea_el_fetch_fallido_con_ollama_en_cero(caplog):
    service = _service(FakeLlmClient(), status_code=404, fetch_delay=0.02)
    caplog.set_level(logging.INFO)

    assert asyncio.run(service.process("doc-1")) is False

    failed = _records(caplog, "resumen fallido")
    assert len(failed) == 1
    match = FAILED_PATTERN.fullmatch(failed[0].getMessage())
    assert match is not None
    document_id, fetch_ms, ollama_ms, total_ms, error = match.groups()
    assert document_id == "doc-1"
    assert float(fetch_ms) > 0
    assert float(ollama_ms) == 0.0
    assert float(total_ms) >= float(fetch_ms)
    assert error == "DocumentNotFoundError"


def test_no_loguea_el_contenido_del_documento(caplog):
    service = _service(
        FakeLlmClient(delay=0.01), content="PDF-CONFIDENCIAL", fetch_delay=0.01
    )
    caplog.set_level(logging.INFO)

    asyncio.run(service.process("doc-1"))

    app_messages = [record.getMessage() for record in caplog.records if record.name == "app"]
    assert app_messages
    assert all("PDF-CONFIDENCIAL" not in message for message in app_messages)


def test_la_linea_de_fallo_no_incluye_el_mensaje_del_error(caplog):
    llm = FakeLlmClient(error=RuntimeError("falló contra http://ollama:11434"))
    service = _service(llm, fetch_delay=0.01)
    caplog.set_level(logging.INFO)

    assert asyncio.run(service.process("doc-1")) is False

    failed = _records(caplog, "resumen fallido")
    assert len(failed) == 1
    assert failed[0].getMessage().endswith("error=RuntimeError")
    assert "http://" not in failed[0].getMessage()

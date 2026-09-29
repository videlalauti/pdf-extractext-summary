"""Tests del adaptador de Ollama: payload enviado y traducción de fallos."""

import json
from collections.abc import Callable

import httpx
import pytest

from errors import (
    LlmModelNotFoundError,
    LlmPromptRejectedError,
    LlmTimeoutError,
    LlmUnavailableError,
)
from llm import OllamaLlmClient

OLLAMA_URL = "http://ollama:11434"
PROMPT = "Resumí este texto"
Handler = Callable[[httpx.Request], httpx.Response]


def build_client(handler: Handler) -> OllamaLlmClient:
    return OllamaLlmClient(
        base_url=OLLAMA_URL,
        timeout=1.0,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def responds_with(status_code: int, **kwargs) -> Handler:
    return lambda request: httpx.Response(status_code, **kwargs)


def connection_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


def read_timeout(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("timeout", request=request)


def records_into(requests: list[httpx.Request], status_code: int = 200, **kwargs) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code, **kwargs)

    return handler


@pytest.mark.anyio
async def test_generate_devuelve_la_respuesta_de_ollama():
    client = build_client(responds_with(200, json={"response": "resumen generado"}))

    try:
        assert await client.generate("llama3.2", PROMPT) == "resumen generado"
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_generate_envia_el_payload_esperado():
    requests: list[httpx.Request] = []
    client = build_client(records_into(requests, json={"response": "resumen"}))

    try:
        await client.generate("llama3.2", PROMPT)
    finally:
        await client.aclose()

    request = requests[0]
    payload = json.loads(request.content)
    assert request.method == "POST"
    assert str(request.url) == f"{OLLAMA_URL}/api/generate"
    assert payload == {
        "model": "llama3.2",
        "prompt": PROMPT,
        "stream": False,
        "options": {"num_predict": 300},
    }


@pytest.mark.parametrize("status_code", [400, 422])
@pytest.mark.anyio
async def test_generate_prompt_rechazado_por_contexto(status_code):
    client = build_client(responds_with(status_code, json={"error": "too long"}))

    try:
        with pytest.raises(LlmPromptRejectedError):
            await client.generate("llama3.2", PROMPT)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_generate_404_sugiere_que_revises_la_configuracion():
    client = build_client(responds_with(404, json={"error": "model not found"}))

    try:
        with pytest.raises(LlmModelNotFoundError, match="modelo-fantasma"):
            await client.generate("modelo-fantasma", PROMPT)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_generate_error_interno_de_ollama():
    client = build_client(responds_with(500, text="boom"))

    try:
        with pytest.raises(LlmUnavailableError):
            await client.generate("llama3.2", PROMPT)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_generate_timeout_de_red():
    client = build_client(read_timeout)

    try:
        with pytest.raises(LlmTimeoutError):
            await client.generate("llama3.2", PROMPT)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_generate_error_de_conexion():
    client = build_client(connection_error)

    try:
        with pytest.raises(LlmUnavailableError):
            await client.generate("llama3.2", PROMPT)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_is_available_consulta_api_tags():
    requests: list[httpx.Request] = []
    client = build_client(records_into(requests, json={"models": []}))

    try:
        assert await client.is_available() is True
    finally:
        await client.aclose()

    assert requests[0].method == "GET"
    assert str(requests[0].url) == f"{OLLAMA_URL}/api/tags"


@pytest.mark.anyio
async def test_is_available_false_si_ollama_no_responde():
    client = build_client(connection_error)

    try:
        assert await client.is_available() is False
    finally:
        await client.aclose()

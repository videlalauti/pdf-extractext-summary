"""Summary service: capa de aplicación, orquestación con persistence y Ollama."""

import httpx
from fastapi import HTTPException
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    persistence_service_url: str = "http://persistence-service:8000"
    ollama_url: str = "http://ollama:11434"
    ollama_model: str = "llama3.2"


settings = Settings()


async def fetch_document(document_id: str) -> dict:
    url = f"{settings.persistence_service_url}/documents/{document_id}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        detail = "Document not found" if status == 404 else f"Error en persistence-service: {str(e)}"
        raise HTTPException(status_code=status, detail=detail) from e
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Error comunicándose con persistence-service: {str(e)}"
        ) from e


async def call_ollama(text: str) -> str:
    url = f"{settings.ollama_url}/api/generate"
    payload = {
        "model": settings.ollama_model,
        "prompt": f"Resumí en español el siguiente texto:\n\n{text}",
        "stream": False,
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json().get("response", "")
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        raise HTTPException(
            status_code=504, detail=f"Error comunicándose con Ollama: {str(e)}"
        ) from e


async def summarize_document(document_id: str) -> dict:
    document = await fetch_document(document_id)
    summary = await call_ollama(document["content"])
    return {"summary": summary, "document_id": document_id}
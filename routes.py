"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

import httpx
from fastapi import APIRouter, Depends, Request, Response, status

from app import Settings, SummaryResponse, SummaryService, get_settings
from llm import LlmClient

router = APIRouter()


def get_llm_client(request: Request) -> LlmClient:
    return request.app.state.llm_client


def get_document_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.document_client


def get_summary_service(
    document_client: httpx.AsyncClient = Depends(get_document_client),
    llm_client: LlmClient = Depends(get_llm_client),
    config: Settings = Depends(get_settings),
) -> SummaryService:
    return SummaryService(
        document_client=document_client,
        persistence_url=config.persistence_service_url,
        llm_client=llm_client,
        model=config.ollama_model,
        max_summary_chars=config.max_summary_chars,
    )


@router.post("/summary/{document_id}", response_model=SummaryResponse)
async def get_summary(
    document_id: str, service: SummaryService = Depends(get_summary_service)
) -> SummaryResponse:
    return await service.summarize(document_id)


@router.get("/health")
async def health(response: Response, llm_client: LlmClient = Depends(get_llm_client)) -> dict:
    if not await llm_client.is_available():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unhealthy", "service": "summary-service", "ollama": "unreachable"}
    return {"status": "healthy", "service": "summary-service", "ollama": "reachable"}

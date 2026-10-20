"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

import httpx
from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from app import (
    JOB_STATE_DONE,
    JOB_STATE_QUEUED,
    Settings,
    SummaryResponse,
    SummaryService,
    build_summary_service,
    get_settings,
)
from cache import SummaryCache
from errors import DocumentNotFoundError
from llm import LlmClient

router = APIRouter()


def get_llm_client(request: Request) -> LlmClient:
    return request.app.state.llm_client


def get_document_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.document_client


def get_summary_cache(request: Request) -> SummaryCache:
    return request.app.state.summary_cache


def get_summary_service(
    document_client: httpx.AsyncClient = Depends(get_document_client),
    llm_client: LlmClient = Depends(get_llm_client),
    cache: SummaryCache = Depends(get_summary_cache),
    config: Settings = Depends(get_settings),
) -> SummaryService:
    return build_summary_service(
        document_client=document_client, llm_client=llm_client, cache=cache, config=config
    )


@router.post("/summary/{document_id}", response_model=None)
async def submit_summary(
    document_id: str, service: SummaryService = Depends(get_summary_service)
) -> SummaryResponse | JSONResponse:
    """Devuelve el resumen si ya existe; si no, acepta el trabajo con 202.

    El trabajo lo dispara el consumer de Kafka con el evento
    `documents.extracted`; el POST solo marca `queued` en la caché
    (best-effort) para que GET responda 202 y no 404 mientras el consumer lo
    toma.
    """
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"status": job.status}
        )
    await service.enqueue(document_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED, content={"status": JOB_STATE_QUEUED}
    )


@router.get("/summary/{document_id}", response_model=None)
async def get_summary(
    document_id: str, service: SummaryService = Depends(get_summary_service)
) -> SummaryResponse | JSONResponse:
    """Consulta el estado o resultado del resumen; 404 si nunca se pidió."""
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"status": job.status}
        )
    raise DocumentNotFoundError


@router.get("/health")
async def health(response: Response, llm_client: LlmClient = Depends(get_llm_client)) -> dict:
    if not await llm_client.is_available():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unhealthy", "service": "summary-service", "ollama": "unreachable"}
    return {"status": "healthy", "service": "summary-service", "ollama": "reachable"}

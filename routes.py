"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

import asyncio

import httpx
from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from app import (
    JOB_STATE_DONE,
    JOB_STATE_QUEUED,
    Settings,
    SummaryResponse,
    SummaryService,
    get_settings,
)
from cache import SummaryCache
from errors import DocumentNotFoundError
from llm import LlmClient

router = APIRouter()

# Referencias vivas a los jobs en background: sin ellas, asyncio los descartaría.
_pending_tasks: set[asyncio.Task] = set()


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
    return SummaryService(
        document_client=document_client,
        persistence_url=config.persistence_service_url,
        llm_client=llm_client,
        model=config.ollama_model,
        max_summary_chars=config.max_summary_chars,
        cache=cache,
    )


def _launch(job_task: asyncio.Task) -> None:
    """Mantiene viva la tarea de background y la descarta al terminar."""
    _pending_tasks.add(job_task)
    job_task.add_done_callback(_pending_tasks.discard)


@router.post("/summary/{document_id}", response_model=None)
async def submit_summary(
    document_id: str, service: SummaryService = Depends(get_summary_service)
) -> SummaryResponse | JSONResponse:
    """Devuelve el resumen si ya está cacheado, o lo encola en background (202)."""
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"status": job.status}
        )
    if await service.enqueue(document_id):
        _launch(asyncio.create_task(service.process(document_id)))
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"status": JOB_STATE_QUEUED}
        )
    return await service.summarize(document_id)


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

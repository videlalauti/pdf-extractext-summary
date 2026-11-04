"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from app import (
    JOB_STATE_DONE,
    JOB_STATE_FAILED,
    JOB_STATE_QUEUED,
    MAX_ATTEMPTS_REASON,
    Settings,
    SummaryResponse,
    SummaryService,
    build_summary_service,
    get_settings,
)
from cache import SummaryCache
from consumer import KafkaSummaryConsumer
from errors import DocumentNotFoundError
from llm import LlmClient

router = APIRouter()


def get_llm_client(request: Request) -> LlmClient:
    return request.app.state.llm_client


def get_document_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.document_client


def get_summary_cache(request: Request) -> SummaryCache:
    return request.app.state.summary_cache


def get_summary_consumer(request: Request) -> KafkaSummaryConsumer:
    return request.app.state.summary_consumer


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
    document_id: str,
    background: BackgroundTasks,
    service: SummaryService = Depends(get_summary_service),
) -> SummaryResponse | JSONResponse:
    """Devuelve el resumen si ya existe; si no, acepta el trabajo con 202.

    El trabajo lo dispara el consumer de Kafka con el evento
    `documents.extracted`, pero el POST también lo agenda en background: si el
    evento se perdió (publicación fail-open, offset ya confirmado), el pedido
    explícito del usuario no puede quedar en `queued` para siempre.
    `BackgroundTasks` corre después de responder, así que el 202 no espera a
    la inferencia de Ollama.
    """
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None:
        if job.status == JOB_STATE_QUEUED:
            background.add_task(service.process, document_id)
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": job.status, "attempts": job.attempts},
        )
    await service.enqueue(document_id)
    background.add_task(service.process, document_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": JOB_STATE_QUEUED, "attempts": 0},
    )


@router.get("/summary/{document_id}", response_model=None)
async def get_summary(
    document_id: str, service: SummaryService = Depends(get_summary_service)
) -> SummaryResponse | JSONResponse:
    """Consulta el estado o resultado del resumen; 404 si nunca se pidió."""
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None and job.status == JOB_STATE_FAILED:
        # 409 (no 200) para que 200 siga significando "resumen listo" y el
        # cliente no confunda un fallo terminal con un resultado.
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "status": JOB_STATE_FAILED,
                "attempts": job.attempts,
                "reason": job.reason or MAX_ATTEMPTS_REASON,
            },
        )
    if job is not None:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": job.status, "attempts": job.attempts},
        )
    raise DocumentNotFoundError


@router.get("/health")
async def health(
    response: Response,
    llm_client: LlmClient = Depends(get_llm_client),
    summary_consumer: KafkaSummaryConsumer = Depends(get_summary_consumer),
    summary_cache: SummaryCache = Depends(get_summary_cache),
) -> dict:
    """Estado real del servicio: Ollama, Kafka y el estado persistible (Redis)."""
    ollama_ok = await llm_client.is_available()
    kafka_ok = summary_consumer.is_connected
    redis_ok = await summary_cache.ping()
    body = {
        "status": "healthy" if ollama_ok and kafka_ok and redis_ok else "unhealthy",
        "service": "summary-service",
        "ollama": "reachable" if ollama_ok else "unreachable",
        "kafka": "connected" if kafka_ok else "disconnected",
        "redis": "ok" if redis_ok else "down",
    }
    if not ollama_ok or not kafka_ok or not redis_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return body

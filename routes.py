"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Request, Response, status
from fastapi.responses import JSONResponse

from app import (
    JOB_STATE_DONE,
    JOB_STATE_FAILED,
    JOB_STATE_PROCESSING,
    JOB_STATE_QUEUED,
    MAX_ATTEMPTS_REASON,
    EnqueueResult,
    Settings,
    SummaryResponse,
    SummaryService,
    build_summary_service,
    get_settings,
)
from cache import SummaryCache
from consumer import KafkaSummaryConsumer
from errors import SummaryStateUnavailableError
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
    `documents.extracted`, pero el POST también lo agenda en background cuando
    corresponde: si el evento se perdió (publicación fail-open, offset ya
    confirmado), el pedido explícito del usuario no puede quedar en `queued`
    para siempre. Un `queued` fresco no se re-dispara antes del backoff, para
    no pisar el reintento del consumer.
    """
    job = await service.peek_job(document_id)
    if job is not None and job.status == JOB_STATE_DONE:
        return SummaryResponse(summary=job.summary or "", document_id=document_id)
    if job is not None and job.status in (JOB_STATE_QUEUED, JOB_STATE_PROCESSING):
        if service.should_schedule(job):
            background.add_task(service.process, document_id, trigger="route")
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": job.status, "attempts": job.attempts},
        )

    # Miss o failed: comprobar existencia antes de aceptar el trabajo.
    # Un 202 sobre un documento inexistente sería un falso positivo de API.
    await service.fetch_document(document_id)
    result = await service.enqueue(document_id)
    if result is EnqueueResult.DONE:
        job = await service.peek_job(document_id)
        return SummaryResponse(
            summary=(job.summary if job is not None else "") or "",
            document_id=document_id,
        )
    if result is EnqueueResult.UNAVAILABLE:
        raise SummaryStateUnavailableError
    background.add_task(service.process, document_id, trigger="route")
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": JOB_STATE_QUEUED, "attempts": 0},
    )


@router.get("/summary/{document_id}", response_model=None)
async def get_summary(
    document_id: str,
    background: BackgroundTasks,
    service: SummaryService = Depends(get_summary_service),
) -> SummaryResponse | JSONResponse:
    """Consulta el estado o resultado; un miss de caché se recupera de persistence.

    El GET no es solo lectura: si Redis no tiene el job pero el documento existe,
    lo reencola y agenda el procesamiento. Así un Redis vacío o una clave
    expirada no dejan al cliente con un 404 engañoso.
    """
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
        if service.should_schedule(job):
            background.add_task(service.process, document_id, trigger="route")
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": job.status, "attempts": job.attempts},
        )

    # Miss real: distinguir documento inexistente (404) de Redis caído (503)
    # antes de reencolar. fetch_document levanta el error de dominio adecuado.
    await service.fetch_document(document_id)
    result = await service.enqueue(document_id)
    if result is EnqueueResult.DONE:
        job = await service.peek_job(document_id)
        return SummaryResponse(
            summary=(job.summary if job is not None else "") or "",
            document_id=document_id,
        )
    if result is EnqueueResult.UNAVAILABLE:
        raise SummaryStateUnavailableError
    background.add_task(service.process, document_id, trigger="route")
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": JOB_STATE_QUEUED, "attempts": 0},
    )


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

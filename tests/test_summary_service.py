"""Tests del contrato HTTP de resumen: POST acepta, GET consulta, health y Settings."""

import asyncio
import json

import pytest
from fakes import FakeLlmClient, FakeSummaryCache, FakeSummaryConsumer, document_client
from pydantic import ValidationError

from app import (
    JOB_STATE_DONE,
    JOB_STATE_FAILED,
    JOB_STATE_PROCESSING,
    JOB_STATE_QUEUED,
    MAX_SUMMARY_ATTEMPTS,
    EnqueueResult,
    Settings,
    build_summary_service,
    parse_job,
    serialize_job,
    settings,
    utc_now_iso,
)
from errors import LlmTimeoutError
from llm import LlmClient


def _seed(
    cache: FakeSummaryCache,
    document_id: str,
    status: str,
    summary: str | None = None,
    *,
    attempts: int = 0,
    updated_at: str | None = None,
    reason: str | None = None,
):
    """Deja un estado de job en la caché, como lo dejaría el consumer."""
    job = {
        "status": status,
        "attempts": attempts,
        "updated_at": updated_at or utc_now_iso(),
    }
    if summary is not None:
        job["summary"] = summary
    if reason is not None:
        job["reason"] = reason
    cache.data[f"summary:{document_id}"] = json.dumps(job)


def test_health_healthy_cuando_ollama_y_kafka_responden(build_client):
    with build_client(
        llm_client=FakeLlmClient(available=True), cache=FakeSummaryCache()
    ) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "summary-service",
        "ollama": "reachable",
        "kafka": "connected",
        "redis": "ok",
    }


def test_health_unhealthy_cuando_ollama_no_responde(build_client):
    with build_client(
        llm_client=FakeLlmClient(available=False), cache=FakeSummaryCache()
    ) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "service": "summary-service",
        "ollama": "unreachable",
        "kafka": "connected",
        "redis": "ok",
    }


def test_health_unhealthy_cuando_kafka_esta_desconectado(build_client):
    consumer = FakeSummaryConsumer(connected=False)

    with build_client(
        llm_client=FakeLlmClient(available=True),
        consumer=consumer,
        cache=FakeSummaryCache(),
    ) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "service": "summary-service",
        "ollama": "reachable",
        "kafka": "disconnected",
        "redis": "ok",
    }


def test_health_unhealthy_cuando_redis_esta_caido(build_client):
    with build_client(
        llm_client=FakeLlmClient(available=True),
        cache=FakeSummaryCache(fail_open=True),
    ) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "service": "summary-service",
        "ollama": "reachable",
        "kafka": "connected",
        "redis": "down",
    }


def test_fake_del_puerto_cumple_el_protocolo():
    assert isinstance(FakeLlmClient(), LlmClient)


def test_el_tope_de_contexto_tiene_que_permitir_mitad_y_mitad():
    with pytest.raises(ValidationError):
        Settings(max_summary_chars=1)


def test_cache_deshabilitada_falla_el_startup():
    from main import validate_runtime_settings

    with pytest.raises(RuntimeError, match="SUMMARY_CACHE_ENABLED=false"):
        validate_runtime_settings(Settings(summary_cache_enabled=False))


# --------------------------------------------------------------------------- #
# POST: acepta el trabajo; el trigger es el consumer de Kafka
# --------------------------------------------------------------------------- #


def test_post_responde_202_queued_cuando_no_hay_resumen(build_client):
    with build_client(llm_client=FakeLlmClient(), cache=FakeSummaryCache()) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 0}


def test_post_dispara_el_procesamiento_aunque_no_llegue_el_evento(build_client):
    """Red de seguridad: el POST agenda process y no depende solo de Kafka."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 0}
    assert len(llm.prompts) == 1
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_DONE


def test_post_con_job_queued_reintenta_el_procesamiento(build_client):
    """Un job que quedó encolado sin evento también se destraba con el POST."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 0}
    assert len(llm.prompts) == 1


def test_post_con_job_processing_no_agenda_otra_inferencia(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert llm.prompts == []


def test_post_con_job_queued_fresco_no_pisa_el_backoff(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED, attempts=1)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 1}
    assert llm.prompts == []


def test_post_con_job_queued_stale_reintenta_despues_del_backoff(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_QUEUED,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 1}
    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_post_sobre_failed_reencola_y_resetea_intentos(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_FAILED,
        attempts=MAX_SUMMARY_ATTEMPTS,
        reason="LlmTimeoutError",
    )

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 0}
    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 1


def test_post_devuelve_200_cuando_el_resumen_ya_esta_cacheado(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="ya resumido")

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "ya resumido", "document_id": "doc-1"}
    assert llm.prompts == []


def test_post_devuelve_202_con_el_estado_actual_si_esta_en_curso(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(llm_client=FakeLlmClient(), cache=cache) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_PROCESSING, "attempts": 0}


def test_post_responde_503_si_no_puede_persistir_estado(build_client):
    """Sin estado persistible no se inicia una inferencia que no se pueda recuperar."""
    llm = FakeLlmClient()

    with build_client(llm_client=llm, cache=FakeSummaryCache(fail_open=True)) as client:
        response = client.post("/summary/doc-1")

    assert response.status_code == 503
    assert response.json() == {
        "detail": (
            "El estado del resumen no está disponible (Redis caído o deshabilitado); "
            "reintentá más tarde"
        )
    }
    assert llm.prompts == []


# --------------------------------------------------------------------------- #
# GET: consulta el estado que dejan POST y el consumer
# --------------------------------------------------------------------------- #


def test_get_devuelve_200_con_el_resumen_done(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="resumen de prueba")

    with build_client(cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 200
    assert response.json() == {"summary": "resumen de prueba", "document_id": "doc-1"}


def test_get_devuelve_202_mientras_el_consumer_procesa(build_client):
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING)

    with build_client(cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_PROCESSING, "attempts": 0}


def test_get_devuelve_409_con_failed_attempts_y_reason(build_client):
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_FAILED,
        attempts=MAX_SUMMARY_ATTEMPTS,
        reason="LlmTimeoutError",
    )

    with build_client(cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 409
    assert response.json() == {
        "status": JOB_STATE_FAILED,
        "attempts": MAX_SUMMARY_ATTEMPTS,
        "reason": "LlmTimeoutError",
    }


def test_get_queued_fresco_no_dispara_el_retry_antes_del_backoff(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED, attempts=1)

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 1}
    assert llm.prompts == []


def test_get_queued_stale_dispara_el_retry(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_QUEUED,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 1}
    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_get_processing_stale_reanuda_el_job_huerfano(build_client):
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_PROCESSING,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_PROCESSING, "attempts": 1}
    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_get_devuelve_200_tras_el_post_que_dispara_el_resumen(build_client):
    with build_client(cache=FakeSummaryCache()) as client:
        assert client.post("/summary/doc-1").status_code == 202
        response = client.get("/summary/doc-1")

    assert response.status_code == 200
    assert response.json()["summary"] == "resumen de prueba"


def test_get_sin_resumen_devuelve_404_si_el_documento_no_existe(build_client):
    with build_client(
        cache=FakeSummaryCache(), persistence_status_code=404
    ) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 404


def test_get_sin_resumen_recupera_el_job_desde_persistence(build_client):
    """Redis vacío no es un 404 si el documento existe: se reencola y procesa."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()

    with build_client(llm_client=llm, cache=cache) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 202
    assert response.json() == {"status": JOB_STATE_QUEUED, "attempts": 0}
    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 1


def test_get_sin_estado_responde_503_si_redis_no_persiste(build_client):
    llm = FakeLlmClient()

    with build_client(llm_client=llm, cache=FakeSummaryCache(fail_open=True)) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 503
    assert "El estado del resumen no está disponible" in response.json()["detail"]
    assert llm.prompts == []


def test_get_sin_estado_responde_503_si_persistence_cae(build_client):
    with build_client(
        cache=FakeSummaryCache(), persistence_connect_error=True
    ) as client:
        response = client.get("/summary/doc-1")

    assert response.status_code == 503
    assert response.json() == {"detail": "El persistence-service no está disponible"}


# --------------------------------------------------------------------------- #
# process: idempotencia y concurrencia entre consumer y background
# --------------------------------------------------------------------------- #


def _service(llm: FakeLlmClient, cache: FakeSummaryCache, *, content: str = "texto de prueba"):
    return build_summary_service(
        document_client=document_client(content),
        llm_client=llm,
        cache=cache,
        config=settings,
    )


def test_process_no_vuelve_a_inferir_un_job_done():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_DONE, summary="ya resumido")

    assert asyncio.run(_service(llm, cache).process("doc-1")) is True

    assert llm.prompts == []
    assert json.loads(cache.data["summary:doc-1"])["summary"] == "ya resumido"


def test_consumer_y_background_comparten_el_lock_por_documento():
    """Dos instancias del servicio (consumer y POST) infieren una sola vez."""
    llm = FakeLlmClient(delay=0.05)
    cache = FakeSummaryCache()
    consumer_service = _service(llm, cache)
    background_service = _service(llm, cache)

    async def scenario():
        return await asyncio.gather(
            consumer_service.process("doc-1"),
            background_service.process("doc-1"),
        )

    assert asyncio.run(scenario()) == [True, True]
    assert len(llm.prompts) == 1
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_DONE


def test_los_defaults_nuevos_son_los_documentados():
    config = Settings()

    assert config.summary_result_ttl_seconds == 604800


def test_un_fallo_deja_queued_con_attempts_y_el_tercero_failed():
    llm = FakeLlmClient(error=LlmTimeoutError())
    cache = FakeSummaryCache()
    service = _service(llm, cache)

    async def scenario():
        return [await service.process("doc-1") for _ in range(MAX_SUMMARY_ATTEMPTS)]

    assert asyncio.run(scenario()) == [False, False, False]

    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_FAILED
    assert job.attempts == MAX_SUMMARY_ATTEMPTS
    assert job.reason == "LlmTimeoutError"
    assert len(llm.prompts) == MAX_SUMMARY_ATTEMPTS


def test_empty_document_es_terminal_en_el_primer_intento():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    service = _service(llm, cache, content="   ")

    assert asyncio.run(service.process("doc-1")) is True

    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_FAILED
    assert job.attempts == 1
    assert job.reason == "empty_document"
    assert llm.prompts == []


def test_process_no_reintenta_un_failed_terminal():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_FAILED,
        attempts=MAX_SUMMARY_ATTEMPTS,
        reason="LlmTimeoutError",
    )

    assert asyncio.run(_service(llm, cache).process("doc-1")) is True

    assert llm.prompts == []
    assert json.loads(cache.data["summary:doc-1"])["status"] == JOB_STATE_FAILED


def test_el_resultado_done_usa_el_ttl_largo():
    cache = FakeSummaryCache()

    asyncio.run(_service(FakeLlmClient(), cache).process("doc-1"))

    _, raw, ttl = cache.set_calls[-1]
    assert parse_job(raw).status == JOB_STATE_DONE
    assert ttl == settings.summary_result_ttl_seconds


def test_los_estados_no_terminales_usan_el_ttl_corto():
    cache = FakeSummaryCache()

    asyncio.run(
        _service(FakeLlmClient(error=LlmTimeoutError()), cache).process("doc-1")
    )

    _, raw, ttl = cache.set_calls[-1]
    assert parse_job(raw).status == JOB_STATE_QUEUED
    assert ttl == settings.summary_cache_ttl_seconds


def test_parse_job_acepta_el_esquema_viejo_y_descarta_corruptos():
    legacy = parse_job('{"status":"done","summary":"viejo"}')

    assert legacy is not None
    assert legacy.status == JOB_STATE_DONE
    assert legacy.attempts == 0
    assert legacy.updated_at.startswith("1970-01-01")
    assert parse_job("no es json") is None
    assert parse_job('{"status":"desconocido"}') is None


def test_serialize_job_omite_los_campos_opcionales_vacios():
    raw = serialize_job(JOB_STATE_QUEUED, attempts=2)

    assert json.loads(raw) == {
        "status": JOB_STATE_QUEUED,
        "attempts": 2,
        "updated_at": json.loads(raw)["updated_at"],
    }


def test_el_reason_no_incluye_el_mensaje_del_error():
    llm = FakeLlmClient(error=RuntimeError("falló contra http://ollama:11434"))
    cache = FakeSummaryCache()
    service = _service(llm, cache)

    async def scenario():
        for _ in range(MAX_SUMMARY_ATTEMPTS):
            await service.process("doc-1")

    asyncio.run(scenario())

    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_FAILED
    assert job.reason == "RuntimeError"
    assert "http://" not in cache.data["summary:doc-1"]


def test_enqueue_distingue_queued_done_y_unavailable():
    cache = FakeSummaryCache()
    service = _service(FakeLlmClient(), cache)

    async def scenario():
        first = await service.enqueue("doc-1")
        cache.data["summary:doc-1"] = serialize_job(
            JOB_STATE_DONE, summary="ya listo", attempts=1
        )
        second = await service.enqueue("doc-1")
        return first, second

    assert asyncio.run(scenario()) == (EnqueueResult.QUEUED, EnqueueResult.DONE)
    unavailable = asyncio.run(
        _service(FakeLlmClient(), FakeSummaryCache(fail_open=True)).enqueue("doc-2")
    )
    assert unavailable is EnqueueResult.UNAVAILABLE


def test_process_route_no_infiere_un_queued_fresco_con_intentos():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED, attempts=1)

    assert (
        asyncio.run(_service(llm, cache).process("doc-1", trigger="route")) is False
    )

    assert llm.prompts == []
    assert parse_job(cache.data["summary:doc-1"]).attempts == 1


def test_process_consumer_si_infiere_un_queued_fresco_con_intentos():
    """El consumer es el driver at-least-once: no le aplica el backoff de la ruta."""
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED, attempts=1)

    assert asyncio.run(_service(llm, cache).process("doc-1")) is True

    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_process_no_infiere_un_processing_fresco():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_PROCESSING, attempts=1)

    service = _service(llm, cache)

    assert asyncio.run(service.process("doc-1", trigger="route")) is False
    assert asyncio.run(service.process("doc-1", trigger="consumer")) is False

    assert llm.prompts == []
    assert parse_job(cache.data["summary:doc-1"]).attempts == 1


def test_process_trata_un_processing_stale_como_queued():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_PROCESSING,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )

    assert asyncio.run(_service(llm, cache).process("doc-1", trigger="route")) is True

    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_process_queued_con_attempts_max_se_marca_failed_sin_inferir():
    llm = FakeLlmClient()
    cache = FakeSummaryCache()
    _seed(cache, "doc-1", JOB_STATE_QUEUED, attempts=MAX_SUMMARY_ATTEMPTS)

    assert asyncio.run(_service(llm, cache).process("doc-1", trigger="route")) is True

    assert llm.prompts == []
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_FAILED
    assert job.attempts == MAX_SUMMARY_ATTEMPTS
    assert job.reason == "max_attempts_exhausted"


def test_dos_process_concurrentes_sobre_queued_stale_incrementan_una_vez():
    llm = FakeLlmClient(delay=0.05)
    cache = FakeSummaryCache()
    _seed(
        cache,
        "doc-1",
        JOB_STATE_QUEUED,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )
    service = _service(llm, cache)

    async def scenario():
        return await asyncio.gather(
            service.process("doc-1", trigger="route"),
            service.process("doc-1", trigger="route"),
        )

    assert asyncio.run(scenario()) == [True, True]

    assert len(llm.prompts) == 1
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_DONE
    assert job.attempts == 2


def test_un_queued_que_vuelve_a_fallar_se_recupera_con_el_siguiente_get_stale():
    """Mecanismo runtime: tras un retry fallido, el GET stale vuelve a intentar."""
    llm = FakeLlmClient(error=LlmTimeoutError())
    cache = FakeSummaryCache()
    service = _service(llm, cache)

    assert asyncio.run(service.process("doc-1", trigger="route")) is False
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_QUEUED
    assert job.attempts == 1

    # El GET inmediato no lo re-dispara; pasado el backoff, sí.
    assert asyncio.run(service.process("doc-1", trigger="route")) is False
    assert len(llm.prompts) == 1
    cache.data["summary:doc-1"] = serialize_job(
        JOB_STATE_QUEUED,
        attempts=1,
        updated_at="2020-01-01T00:00:00+00:00",
    )
    assert asyncio.run(service.process("doc-1", trigger="route")) is False
    job = parse_job(cache.data["summary:doc-1"])
    assert job is not None
    assert job.status == JOB_STATE_QUEUED
    assert job.attempts == 2
    assert len(llm.prompts) == 2

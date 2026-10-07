# Summary Service

Servicio FastAPI que resume documentos obtenidos desde el persistence-service usando un modelo de Ollama.

## Instalación de dependencias

```
pip install -r requirements.txt
```

## Cómo correr

```
uvicorn main:app --reload --port 8004
```

## Cómo correr los tests

```
pytest tests/ -v
```

## Variables de entorno

- `PERSISTENCE_SERVICE_URL`: URL base del persistence-service (ej: `http://persistence-service:8000`).
- `PERSISTENCE_TIMEOUT_SECONDS`: timeout de la consulta del documento (default `30`).
- `OLLAMA_URL`: URL base de Ollama (ej: `http://ollama:11434`).
- `OLLAMA_MODEL`: modelo usado para resumir (default `llama3.2`).
- `OLLAMA_TIMEOUT_SECONDS`: timeout de la generación (default `300`).
- `MAX_SUMMARY_CHARS`: tope de caracteres del documento que se envían a Ollama (default `2400`,
  ≈ 800 tokens). El contenido que lo excede se recorta a inicio + final (mitad y mitad) y el
  prompt avisa que fue recortado y que falta la parte del medio. El default es chico a propósito:
  el costo dominante en CPU es el prefill del prompt, no la generación.
- `REDIS_URL`: URL de Redis que guarda el estado/resultado del resumen (default `redis://redis:6379/0`).
- `SUMMARY_CACHE_TTL_SECONDS`: TTL de la clave de cada resumen en Redis (default `3600`);
  pasado ese tiempo hay que volver a resumir.
- `SUMMARY_CACHE_ENABLED`: si es `false`, el servicio degrada a resumen síncrono siempre.
- `LOG_LEVEL`: nivel de logging (default `INFO`).
- `CORS_ORIGINS`: orígenes permitidos en CSV (default `http://localhost`). `*` está
  rechazado porque el middleware habilita credenciales.
- `PORT`: puerto del contenedor; también se usa en el `HEALTHCHECK` (default `8000`).

Ver `.env.example` para un ejemplo.

## Resumen asíncrono

`POST /summary/{document_id}` y `GET /summary/{document_id}` comparten la misma clave en Redis
(`summary:{document_id}`, con TTL) como estado y caché del resumen:

- **Miss**: `POST` responde `202 {"status": "queued"}` y el resumen corre en background
  (tarea asyncio en proceso, sin colas externas). Si viene un segundo `POST` mientras corre,
  responde `202` con el estado actual sin duplicar el job; si el resumen ya terminó, responde
  `200` al instante sin volver a inferir.
- **`GET`** devuelve `200 {summary, document_id}` si terminó, `202 {"status": "processing"}` si
  está corriendo, y `404` si nunca se pidió.
- **Fail-open**: si Redis no responde, `POST` degrada al flujo síncrono de siempre y responde
  `200`; la caché nunca provoca un 5xx.

## Integración en el compose

El compose vive en el repo de infraestructura. El servicio espera estas variables:

```yaml
environment:
  PERSISTENCE_SERVICE_URL: http://persistence-service:8000
  OLLAMA_URL: http://ollama:11434
  OLLAMA_MODEL: llama3.2
  MAX_SUMMARY_CHARS: "2400"
  REDIS_URL: redis://redis:6379/0
  SUMMARY_CACHE_TTL_SECONDS: "3600"
  CORS_ORIGINS: http://localhost
ports:
  - "8004:8000"
```

`OLLAMA_MODEL` debe coincidir con un modelo descargado en Ollama (`ollama pull llama3.2`).

## Comportamiento ante errores

| Situación | Respuesta |
| --- | --- |
| Documento inexistente | `404` |
| persistence-service caído o con error | `502` |
| `OLLAMA_MODEL` inexistente en Ollama (404) | `503` con mensaje de configuración |
| Prompt/contexto demasiado largo (400) | `422` indicando `MAX_SUMMARY_CHARS` |
| Ollama caído o error interno (5xx) | `502` |
| Timeout de Ollama | `504` |

`/health` responde `200` solo si Ollama responde a `GET /api/tags` (timeout corto, ~3 s);
si no, responde `503` con `"status": "unhealthy"`.

## Diseño interno

- `llm.py`: puerto `LlmClient` (Protocol) y adaptador `OllamaLlmClient`, que es el único lugar
  que conoce la API de Ollama y traduce sus fallos a errores de dominio.
- `cache.py`: puerto `SummaryCache` (Protocol, `get`/`set`), `NoopSummaryCache` (flujo síncrono)
  y `RedisSummaryCache` (clave única con TTL y fail-open); mismo patrón que `llm.py`.
- `app.py`: `SummaryService` (obtener documento → recortar a inicio + final → resumir), el
  estado del job y `Settings`.
- `routes.py`: endpoints que resuelven las dependencias con `Depends`; los tests inyectan un
  doble del puerto con `app.dependency_overrides`, sin monkeypatchear librerías.
- `error_handlers.py`: traduce los errores de dominio a respuestas HTTP sin exponer internals.
- `main.py`: logging con request-id y CORS salen de `pdf-extractext-shared`
  (`shared.web.logging.setup_logging` + `RequestIdMiddleware` y `shared.web.cors.add_cors`),
  igual que en los otros servicios del proyecto.

El paquete `pdf-extractext-shared` se instala desde el tag, no desde PyPI:

```
pip install "pdf-extractext-shared @ https://github.com/videlalauti/pdf-extractext-shared/archive/refs/tags/v1.0.0.zip"
```

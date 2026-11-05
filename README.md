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
- `SUMMARY_CACHE_TTL_SECONDS`: TTL de los estados **no terminales** (`queued`/`processing`) en Redis
  (default `3600`). Pasado ese tiempo sin resolverse, el job se considera perdido.
- `SUMMARY_RESULT_TTL_SECONDS`: TTL de los estados **terminales** (`done`/`failed`) en Redis
  (default `604800`, 7 días). Es más largo a propósito: el resultado no debe desaparecer a la hora.
- `SUMMARY_RETRY_BACKOFF_SECONDS`: backoff mínimo antes de que `GET`/`POST` reintenten un job
  `queued` con intentos previos (default `30`). Antes de ese plazo no se re-dispara ni se cuenta
  un intento nuevo.
- `SUMMARY_STALE_SECONDS`: antigüedad a partir de la cual un `processing` se considera huérfano
  (crash/redeploy) y se puede reanudar (default `600`). También es el umbral de reconciliación al
  arrancar. **Tiene que ser mayor que el P99 de Ollama** (el timeout por defecto de Ollama es
  `300`); si se baja, un resumen legítimamente largo puede re-dispararse. El default `600` es
  seguro para el timeout de `300`.
- `SUMMARY_CACHE_ENABLED`: debe quedar en `true` (default). Con `false` el estado del job no se
  puede persistir: el startup falla a propósito con `RuntimeError` (modo no soportado, no
  degradado). Ver la nota del `HEALTHCHECK` más abajo.
- `LOG_LEVEL`: nivel de logging (default `INFO`).
- `CORS_ORIGINS`: orígenes permitidos en CSV (default `http://localhost`). `*` está
  rechazado porque el middleware habilita credenciales.
- `PORT`: puerto del contenedor; también se usa en el `HEALTHCHECK` (default `8000`).

Ver `.env.example` para un ejemplo.

## Resumen asíncrono

`POST /summary/{document_id}` y `GET /summary/{document_id}` comparten la misma clave en Redis
(`summary:{document_id}`) como estado y caché del resultado. El valor es un JSON con `status`,
`summary`, `attempts`, `updated_at` y `reason`. El trigger automático es el consumer de Kafka
(`KafkaSummaryConsumer`, corriendo en el lifespan), que al recibir `documents.extracted` publica
por extraction resume el documento (fetch desde persistence, recorte, Ollama). `POST` agenda el
mismo trabajo en `BackgroundTasks` cuando corresponde, como red de seguridad.

### Estados y contrato HTTP

| Estado | `GET` | `POST` | Body |
| --- | --- | --- | --- |
| `done` | `200` | `200` | `{"summary": "...", "document_id": "..."}` |
| `queued` | `202` | `202` | `{"status": "queued", "attempts": N}` (sin `document_id`) |
| `processing` | `202` | `202` | `{"status": "processing", "attempts": N}` (sin `document_id`) |
| `failed` | `409` | `202` (reencola) | `GET`: `{"status": "failed", "attempts": N, "reason": "..."}` |
| sin estado y documento inexistente | `404` | `404` (comprueba que exista antes de encolar) | — |
| sin estado persistible | `503` | `503` | `{"detail": "El estado del resumen no está disponible (...); reintentá más tarde"}` |

- **`200` solo significa resumen listo**: un `failed` responde `409` (con `reason` y `attempts`) para
  no confundir un fallo terminal con un resultado. El `409` se devuelve con `JSONResponse` directo,
  no pasa por `error_handlers`.
- **`202` no incluye `document_id`**: solo `status` y `attempts`, que es lo que el cliente necesita
  para el poll.
- **`503` sin estado persistible**: sale por `error_handlers` como cualquier error de dominio, con
  `{"detail": ...}`. No hay fallback síncrono: si no se puede guardar el estado, no se inicia una
  inferencia que después no se pueda recuperar.
- **`attempts` cuenta intentos iniciados**, no fallos: se incrementa dentro del lock por documento,
  justo antes de escribir `processing`. Si el proceso muere durante la inferencia, ese intento ya
  quedó contado y la reconciliación lo preserva.
- **`reason` es un token saneado** de una lista cerrada: `max_attempts_exhausted`,
  `empty_document` o `type(error).__name__` (por ejemplo `LlmTimeoutError`). Nunca incluye el
  mensaje de la excepción (podría filtrar URLs o contenido).
- **`empty_document` es terminal en el primer intento**: un documento sin contenido pasa a `failed`
  con `attempts=1` y `reason="empty_document"` sin gastar una inferencia ni reintentar 3 veces.

### Retry y recuperación runtime

- Un `queued` fresco **no se re-dispara** desde `GET`/`POST` antes de
  `SUMMARY_RETRY_BACKOFF_SECONDS`; un `processing` fresco tampoco. Así el consumer y la ruta no se
  pisan y no queman dos intentos seguidos.
- Pasado el backoff, el próximo `GET`/`POST` re-dispara el job (`attempts+1`). Si vuelve a fallar,
  queda `queued` con `updated_at` nuevo y el **siguiente GET stale** (otros 30 s) lo recupera. Ese
  es el mecanismo runtime, además del retry del consumer.
- **El consumer ignora el backoff de la ruta**: es el driver at-least-once y reintenta sus propias
  entregas. El lock por documento + el re-chequeo dentro del lock garantizan que dos triggers
  concurrentes sobre un `queued` stale incrementen `attempts` una sola vez.
- **Recuperación desde persistence**: si Redis no tiene el job pero el documento existe, el `GET`
  lo reencola y agenda el procesamiento en vez de devolver `404`. Un `404` queda reservado para un
  documento que realmente no existe en persistence.
- **Reconciliación al arrancar**: los `processing` más viejos que `SUMMARY_STALE_SECONDS` se pasan a
  `queued` preservando `attempts`, y el lifespan los reanuda en **una sola tarea secuencial**
  (concurrencia 1), aislando el error de cada id y cancelando + esperando la tarea en el shutdown.
  Asume una sola réplica: con varias, cada réplica reconciliaría los mismos ids (el lock protege
  dentro del proceso, no entre procesos).
- **Cache caída en el consumer**: si `ping` a Redis falla, el mensaje no cuenta como entrega. Se
  espera un intervalo fijo de `10 s` (reemplaza al backoff exponencial, no se suma) con un tope de
  `30` esperas y un log `cache_unavailable` distinto. El contador se limpia cuando Redis vuelve y
  al confirmar un mensaje.
- **At-least-once + idempotencia**: se confirma el offset exacto del mensaje resuelto, no la
  posición actual; un mensaje fallido se reintenta con `seek` hasta 3 intentos y recién entonces se
  descarta con log `ERROR` para no atorar la partición. Un job `failed` es terminal: el consumer lo
  confirma sin volver a inferir.
- **Fail-open + reconexión**: si Kafka no responde al arrancar, el servicio levanta igual y
  reintenta con backoff hasta que vuelve. Redis es distinto: sin estado persistible los endpoints
  responden `503`; no hay resumen síncrono de respaldo.

Secuencia típica de un job que falla siempre (con los defaults):

1. `POST`/consumer inicia el intento 1 → `queued`/`processing` con `attempts=1`.
2. Falla → `queued`, `attempts=1`, `updated_at=ahora`.
3. A los ~30 s, un `GET`/`POST` stale dispara el intento 2 → `attempts=2`; si falla, vuelve a
   `queued`.
4. A los ~30 s, el intento 3 → `attempts=3`; si falla, queda `failed`.
5. El siguiente `GET` responde `409` con `reason` y `attempts=3`. El smoke tiene margen de sobra
   dentro de 300 s.

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
  SUMMARY_RESULT_TTL_SECONDS: "604800"
  SUMMARY_RETRY_BACKOFF_SECONDS: "30"
  SUMMARY_STALE_SECONDS: "600"
  KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  CORS_ORIGINS: http://localhost
ports:
  - "8004:8000"
```

Los defaults de `Settings` alcanzan para correr el compose; las tres variables nuevas
(`SUMMARY_RESULT_TTL_SECONDS`, `SUMMARY_RETRY_BACKOFF_SECONDS`, `SUMMARY_STALE_SECONDS`) son
opcionales si no se quieren pisar.

`OLLAMA_MODEL` debe coincidir con un modelo descargado en Ollama (`ollama pull llama3.2`).

## Comportamiento ante errores

| Situación | Respuesta |
| --- | --- |
| Documento inexistente | `GET`: `404` si persistence tampoco lo tiene. `POST`: `202` y el job falla en background (`reason="DocumentNotFoundError"`); luego `GET` responde `409` |
| Redis caído o deshabilitado | `503` con `{"detail": ...}` en `POST`/`GET`; no se inicia una inferencia sin estado persistible |
| Job `failed` (3 intentos o documento vacío) | `409` con `status`, `attempts` y `reason`; un `POST` explícito lo reencola |
| Broker de Kafka caído | El servicio levanta igual y reintenta solo con backoff; `POST` sigue pudiendo disparar el resumen |
| Ollama caído, timeout o error de dominio durante el resumen | El job queda `queued` (o `failed` al tercer intento); el consumer no confirma el offset y reintenta |
| Caché caída durante el consumo | El mensaje no cuenta como entrega: espera fija de 10 s, tope de 30, log `cache_unavailable` |

`/health` responde `200` solo si Ollama responde a `GET /api/tags` (timeout corto, ~3 s), el
consumer de Kafka está conectado **y** Redis responde `ping`. En cualquier otro caso responde `503`
con `"status": "unhealthy"`, indicando `ollama`, `kafka` y `redis` por separado.

> **`HEALTHCHECK` y `SUMMARY_CACHE_ENABLED=false`**: como ese modo no está soportado, el startup
> falla y el contenedor queda `unhealthy` a propósito. No es un bug: si se ve ese estado, revisar
> que `SUMMARY_CACHE_ENABLED` esté en `true` y que Redis esté arriba.

## Diseño interno

- `llm.py`: puerto `LlmClient` (Protocol) y adaptador `OllamaLlmClient`, que es el único lugar
  que conoce la API de Ollama y traduce sus fallos a errores de dominio.
- `cache.py`: puerto `SummaryCache` (Protocol, `get`/`set`/`ping`/`scan_keys`),
  `NoopSummaryCache` y `RedisSummaryCache` (clave única con TTL por estado y operaciones
  fail-open); mismo patrón que `llm.py`.
- `consumer.py`: `KafkaSummaryConsumer`, que consume `documents.extracted` en el lifespan,
  reconecta solo con backoff si Kafka se cae, dispara `SummaryService.process` por evento,
  maneja el guard de caché no disponible y confirma el offset solo después de resolverlo
  (at-least-once + idempotencia); el adaptador aiokafka se inyecta en los tests.
- `app.py`: `SummaryService` (obtener documento → recortar a inicio + final → resumir), el
  esquema del job (`status`, `summary`, `attempts`, `updated_at`, `reason`), el backoff de la
  ruta, la reconciliación de `processing` huérfanos y `Settings`.
- `routes.py`: endpoints que resuelven las dependencias con `Depends`; el `GET` recupera un miss
  desde persistence, el `409` de `failed` se devuelve con `JSONResponse` y el `503` sin estado
  persistible pasa por `error_handlers`; los tests inyectan dobles del puerto con
  `app.dependency_overrides`, sin monkeypatchear librerías.
- `error_handlers.py`: traduce los errores de dominio a respuestas HTTP sin exponer internals.
- `main.py`: logging con request-id y CORS salen de `pdf-extractext-shared`
  (`shared.web.logging.setup_logging` + `RequestIdMiddleware` y `shared.web.cors.add_cors`),
  igual que en los otros servicios del proyecto. También valida la config de startup y corre la
  reconciliación + reanudación secuencial en el lifespan.

### Limitaciones conocidas

- **Redis es la única fuente del estado del job**: si se vacía o la clave expira, el `GET`
  reencola y se vuelve a resumir desde persistence. No hay copia durable del resumen en Mongo.
- **Reconciliación de una sola réplica**: el escaneo de `processing` al arrancar asume un único
  pod. Con varias réplicas, cada una reencola los mismos ids; el lock por documento evita la doble
  inferencia dentro de un proceso, no entre procesos.
- **`SUMMARY_STALE_SECONDS` vs. Ollama**: el umbral de `processing` huérfano debe superar el P99
  de inferencia (timeout de Ollama incluido). Con el default de `300 s` para Ollama, `600 s` es
  seguro; si se sube el timeout de Ollama, subir también el stale.
- **`SUMMARY_CACHE_ENABLED=false` no es un modo degradado**: es un modo de tests/no soportado.
  Falla el startup y el `HEALTHCHECK` queda `unhealthy` a propósito.

El paquete `pdf-extractext-shared` se instala desde el tag, no desde PyPI:

```
pip install "pdf-extractext-shared @ https://github.com/videlalauti/pdf-extractext-shared/archive/refs/tags/v1.3.0.zip"
```

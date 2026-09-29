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
- `MAX_SUMMARY_CHARS`: tope de caracteres del documento que se envían a Ollama (default `12000`).
  El contenido que lo excede se trunca y el prompt avisa que está truncado.
- `LOG_LEVEL`: nivel de logging (default `INFO`).

Ver `.env.example` para un ejemplo.

## Integración en el compose

El compose vive en el repo de infraestructura. El servicio espera estas variables:

```yaml
environment:
  PERSISTENCE_SERVICE_URL: http://persistence-service:8000
  OLLAMA_URL: http://ollama:11434
  OLLAMA_MODEL: llama3.2
  MAX_SUMMARY_CHARS: "12000"
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

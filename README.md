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
- `OLLAMA_URL`: URL base de Ollama (ej: `http://ollama:11434`).

Ver `.env.example` para un ejemplo.
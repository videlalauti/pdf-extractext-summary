"""Errores de dominio del servicio de resumen.

Cada error declara el código HTTP con el que se traduce y un detalle seguro
(accionable para quien depura, sin filtrar excepciones internas).
"""


class SummaryError(Exception):
    """Error de dominio traducible a una respuesta HTTP."""

    status_code: int = 500
    detail: str = "Error interno"

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.detail
        super().__init__(self.detail)


class DocumentNotFoundError(SummaryError):
    """El persistence-service no tiene el documento pedido."""

    status_code = 404
    detail = "Documento no encontrado"


class PersistenceUnavailableError(SummaryError):
    """El persistence-service no respondió o falló."""

    status_code = 502
    detail = "El persistence-service no está disponible"


class LlmError(SummaryError):
    """Fallo hablando con el proveedor de modelos."""


class LlmModelNotFoundError(LlmError):
    """El modelo configurado no existe en Ollama: problema de configuración."""

    status_code = 503

    def __init__(self, model: str) -> None:
        super().__init__(
            f"El modelo '{model}' no está disponible en Ollama. "
            f"Verificá OLLAMA_MODEL y descargalo con `ollama pull {model}`."
        )


class LlmPromptRejectedError(LlmError):
    """Ollama rechazó el prompt por exceder su ventana de contexto."""

    status_code = 422
    detail = (
        "Ollama rechazó el prompt porque excede su ventana de contexto. "
        "Bajá MAX_SUMMARY_CHARS para acotar el contenido enviado."
    )


class LlmUnavailableError(LlmError):
    """Ollama no respondió correctamente."""

    status_code = 502
    detail = "Ollama no está disponible"


class LlmTimeoutError(LlmError):
    """Ollama tardó demasiado en responder."""

    status_code = 504
    detail = "Ollama tardó demasiado en responder"

"""Summary service: endpoints HTTP para resumir documentos con Ollama."""

from fastapi import APIRouter
from pydantic import BaseModel

from app import summarize_document

router = APIRouter()


class SummaryResponse(BaseModel):
    summary: str
    document_id: str


@router.post("/summary/{document_id}", response_model=SummaryResponse)
async def get_summary(document_id: str) -> SummaryResponse:
    return SummaryResponse(**await summarize_document(document_id))

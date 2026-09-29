"""Summary service FastAPI application: bootstrap y montaje de componentes."""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import settings
from error_handlers import register_error_handlers
from routes import router

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="PDF Summary Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

register_error_handlers(app)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "summary-service"}

"""
NSOE API Service — FastAPI
Exposes REST endpoints for:
- Alert ingestion
- Graph query
- Entity search
- Analyst feedback
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from nsoe_api.routes import alerts, entities, graph, feedback
from nsoe_api.auth.keycloak import KeycloakMiddleware

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("nsoe_api_starting")
    yield
    logger.info("nsoe_api_stopping")


app = FastAPI(
    title="NSOE API",
    description="National Security Ontology Engine — Analyst API",
    version="0.1.0",
    docs_url="/docs" if os.environ.get("NSOE_ENV") != "production" else None,
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(alerts.router,   prefix="/api/v1/alerts",   tags=["alerts"])
app.include_router(entities.router, prefix="/api/v1/entities", tags=["entities"])
app.include_router(graph.router,    prefix="/api/v1/graph",    tags=["graph"])
app.include_router(feedback.router, prefix="/api/v1/feedback", tags=["feedback"])


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        "nsoe_api.main:app",
        host="0.0.0.0",
        port=int(os.environ.get("NSOE_API_PORT", 8080)),
        workers=int(os.environ.get("NSOE_API_WORKERS", 4)),
    )

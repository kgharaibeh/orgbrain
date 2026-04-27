"""
OrgBrain Control Plane — FastAPI Backend
Central API for all platform operations: services, connectors, governance, brain, agent.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from routers import services, connectors, topics, governance, brain, jobs, agent, ontology

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("OrgBrain Control Plane starting...")
    yield
    log.info("OrgBrain Control Plane shutting down.")


app = FastAPI(
    title="OrgBrain Control Plane",
    description="Central management API for the OrgBrain Banking Intelligence Platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── API routers ──────────────────────────────────────────────────────────────
app.include_router(services.router,    prefix="/api/services",    tags=["Services"])
app.include_router(connectors.router,  prefix="/api/connectors",  tags=["Data Sources"])
app.include_router(topics.router,      prefix="/api/topics",      tags=["Kafka Topics"])
app.include_router(governance.router,  prefix="/api/governance",  tags=["Governance"])
app.include_router(brain.router,       prefix="/api/brain",       tags=["Brain"])
app.include_router(jobs.router,        prefix="/api/jobs",        tags=["Flink Jobs"])
app.include_router(agent.router,       prefix="/api/agent",       tags=["Agent"])
app.include_router(ontology.router,    prefix="/api/ontology",    tags=["Ontology"])


@app.get("/health")
def health():
    return {"status": "ok", "service": "orgbrain-control-plane"}


@app.get("/api/platform/urls")
def platform_urls():
    """Return URLs to all web UIs in the platform."""
    return {
        "control_plane":     "http://localhost:3001",
        "kafka_ui":          "http://localhost:8080",
        "flink_ui":          "http://localhost:8082",
        "schema_registry":   "http://localhost:8081",
        "minio_console":     "http://localhost:9001",
        "vault_ui":          "http://localhost:8200",
        "neo4j_browser":     "http://localhost:7474",
        "grafana":           "http://localhost:3000",
        "portainer":         "http://localhost:9900",
        "agent_api":         "http://localhost:8000/docs",
        "airflow":           "http://localhost:8090",
    }

"""
OrgBrain Control Plane — FastAPI Backend
Central API for all platform operations: services, connectors, governance, brain, agent.
"""

import logging
import os
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routers import services, connectors, topics, governance, brain, jobs, agent, ontology, ingest, auth
from routers.auth import require_auth

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
# Auth is public — no dependency
app.include_router(auth.router,        prefix="/api/auth",        tags=["Auth"])

# All other routes require a valid JWT
_auth_dep = [Depends(require_auth)]
app.include_router(services.router,    prefix="/api/services",    tags=["Services"],      dependencies=_auth_dep)
app.include_router(connectors.router,  prefix="/api/connectors",  tags=["Data Sources"],  dependencies=_auth_dep)
app.include_router(topics.router,      prefix="/api/topics",      tags=["Kafka Topics"],  dependencies=_auth_dep)
app.include_router(governance.router,  prefix="/api/governance",  tags=["Governance"],    dependencies=_auth_dep)
app.include_router(brain.router,       prefix="/api/brain",       tags=["Brain"],         dependencies=_auth_dep)
app.include_router(jobs.router,        prefix="/api/jobs",        tags=["Flink Jobs"],    dependencies=_auth_dep)
app.include_router(agent.router,       prefix="/api/agent",       tags=["Agent"],         dependencies=_auth_dep)
app.include_router(ontology.router,    prefix="/api/ontology",    tags=["Ontology"],      dependencies=_auth_dep)
app.include_router(ingest.router,      prefix="/api/ingest",      tags=["Bulk Ingest"],   dependencies=_auth_dep)


@app.get("/health")
def health():
    return {"status": "ok", "service": "orgbrain-control-plane"}


@app.post("/api/platform/reset")
def platform_reset(clear_audit: bool = Query(False)):
    """
    Wipe all brain-store data and Kafka data topics.
    Neo4j: delete all nodes/edges.
    Qdrant: delete + recreate entity_vectors and event_vectors.
    TimescaleDB: truncate brain_events and brain_signals.
    Kafka: delete all raw.* / clean.* / kafka.pii_audit topics.
    """
    import psycopg2
    from neo4j import GraphDatabase
    from qdrant_client import QdrantClient
    from qdrant_client.http.models import Distance, VectorParams
    from kafka import KafkaAdminClient
    from kafka.errors import UnknownTopicOrPartitionError

    results: dict = {}

    # Neo4j — delete everything
    try:
        driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
            auth=(os.getenv("NEO4J_USER", "neo4j"),
                  os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")),
        )
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
        driver.close()
        results["neo4j"] = "cleared"
    except Exception as e:
        results["neo4j"] = f"error: {e}"

    # Qdrant — delete and recreate collections
    try:
        qdrant = QdrantClient(
            host=os.getenv("QDRANT_HOST", "qdrant"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )
        for coll in ["entity_vectors", "event_vectors"]:
            try:
                qdrant.delete_collection(coll)
            except Exception:
                pass
            qdrant.create_collection(
                coll, vectors_config=VectorParams(size=768, distance=Distance.COSINE)
            )
        results["qdrant"] = "cleared"
    except Exception as e:
        results["qdrant"] = f"error: {e}"

    # TimescaleDB — truncate tables
    try:
        conn = psycopg2.connect(
            host=os.getenv("TIMESCALE_HOST",     "timescale"),
            port=int(os.getenv("TIMESCALE_PORT", "5432")),
            dbname=os.getenv("TIMESCALE_DB",     "orgbrain_metrics"),
            user=os.getenv("TIMESCALE_USER",     "orgbrain"),
            password=os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret"),
        )
        cur = conn.cursor()
        cur.execute("TRUNCATE brain_events, brain_signals")
        if clear_audit:
            cur.execute("TRUNCATE cp_anonymization_audit")
        conn.commit()
        cur.close()
        conn.close()
        results["timescale"] = "cleared"
    except Exception as e:
        results["timescale"] = f"error: {e}"

    # Kafka — delete data topics
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
            client_id="orgbrain-reset",
        )
        live      = admin.list_topics()
        to_delete = [
            t for t in live
            if t.startswith("raw.") or t.startswith("clean.") or t == "kafka.pii_audit"
        ]
        if to_delete:
            try:
                admin.delete_topics(to_delete)
            except UnknownTopicOrPartitionError:
                pass
        admin.close()
        results["kafka"] = f"deleted {len(to_delete)} topics"
    except Exception as e:
        results["kafka"] = f"error: {e}"

    return results


@app.get("/api/platform/urls")
def platform_urls():
    """Return URLs to all web UIs.
    LOCAL:  plain HTTP with explicit ports (localhost).
    AWS:    HTTPS subdomains via Caddy + nip.io (PUBLIC_IP env var set).
    """
    public_ip = os.getenv("PUBLIC_IP", "")
    if public_ip:
        # AWS deployment — every service has its own HTTPS subdomain
        def _url(sub: str) -> str:
            return f"https://{sub}.{public_ip}.nip.io"
        return {
            "control_plane":   _url("orgbrain"),
            "kafka_ui":        _url("kafka"),
            "flink_ui":        _url("flink"),
            "grafana":         _url("grafana"),
            "airflow":         _url("airflow"),
            "neo4j_browser":   _url("neo4j"),
            "vault_ui":        _url("vault"),
            "minio_console":   _url("minio"),
            "schema_registry": f"http://{public_ip}:8081",   # no browser UI
            "portainer":       f"http://{public_ip}:9900",
        }
    # Local development
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

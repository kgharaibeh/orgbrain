"""
Bulk Ingest Router — load data directly into brain stores.
Supports file upload (JSON/CSV) and direct DB query (PostgreSQL).
Bypasses Debezium/Flink — writes directly to Neo4j, Qdrant, TimescaleDB.
"""

import csv
import io
import json
import logging
import os
import threading
import time
import uuid
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from neo4j import GraphDatabase
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams
from sqlalchemy.orm import Session

from database import get_db
from models import GovernanceRule, GovernanceSource

log = logging.getLogger(__name__)
router = APIRouter()

# ── Config ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",          "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",         "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD",     "orgbrain_neo4j")
QDRANT_HOST    = os.getenv("QDRANT_HOST",        "qdrant")
QDRANT_PORT    = int(os.getenv("QDRANT_PORT",    "6333"))
OLLAMA_HOST    = os.getenv("OLLAMA_HOST",        "http://ollama:11434")
TIMESCALE_CONN = {
    "host":     os.getenv("TIMESCALE_HOST",     "timescale"),
    "port":     int(os.getenv("TIMESCALE_PORT", "5432")),
    "dbname":   os.getenv("TIMESCALE_DB",       "orgbrain_metrics"),
    "user":     os.getenv("TIMESCALE_USER",     "orgbrain"),
    "password": os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret"),
}

QDRANT_COLLECTIONS = {"entity_vectors": 768, "event_vectors": 768}
EVENT_ENTITY_TYPES = {
    "transaction", "order", "event", "claim", "log",
    "activity", "entry", "record", "invoice", "payment",
    "reading", "alert", "incident", "interaction",
}

# ── In-memory job store ────────────────────────────────────────────────────────
_jobs_lock: threading.Lock = threading.Lock()
_jobs: dict = {}


# ── Data helpers ───────────────────────────────────────────────────────────────

def _singularize(word: str) -> str:
    w = word.lower()
    if w.endswith("ies"):
        return w[:-3] + "y"
    if w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


def _to_label(name: str) -> str:
    singular = _singularize(name)
    clean = "".join(c for c in singular if c.isalnum() or c == "_")
    return clean[0].upper() + clean[1:] if clean else "Entity"


def _find_primary_id(payload: dict) -> tuple:
    for k, v in payload.items():
        if k.endswith("_id") and v is not None:
            return k, str(v)
    if "id" in payload and payload["id"] is not None:
        return "id", str(payload["id"])
    return None, None


def _scalar_props(payload: dict) -> dict:
    return {k: v for k, v in payload.items()
            if v is not None and isinstance(v, (str, int, float, bool))}


def _embed_text(entity_type: str, payload: dict) -> str:
    parts = [entity_type]
    for k, v in payload.items():
        if v is None or k.endswith("_id") or k.endswith("_at"):
            continue
        parts.append(f"{k}={v}")
        if len(parts) > 20:
            break
    return " ".join(parts)


def _embed(text: str) -> list:
    try:
        r = requests.post(
            f"{OLLAMA_HOST}/api/embeddings",
            json={"model": "nomic-embed-text", "prompt": text},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()["embedding"]
    except Exception:
        return []


# ── PII rule application ───────────────────────────────────────────────────────

def _apply_rules(record: dict, rules: list) -> dict:
    """Apply governance rules. suppress→remove, keep→pass, others→[PROTECTED]."""
    result = dict(record)
    for rule in rules:
        if not getattr(rule, "is_active", True):
            continue
        field = rule.field_name
        if field not in result:
            continue
        method = rule.method
        if method == "suppress":
            del result[field]
        elif method == "keep":
            pass
        elif method == "generalize":
            gmap = rule.generalize_map or {}
            result[field] = gmap.get(str(result[field]), "[OTHER]")
        else:
            # fpe_*, hmac_sha256, nlp_scrub require Vault — mark as protected
            result[field] = "[PROTECTED]"
    return result


# ── Brain store writers ────────────────────────────────────────────────────────

def _write_neo4j(driver, entity_type: str, payload: dict) -> bool:
    label = _to_label(entity_type)
    id_field, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False
    props = _scalar_props(payload)
    try:
        with driver.session() as s:
            s.run(
                f"MERGE (n:`{label}` {{entity_id: $eid}}) "
                f"SET n += $props, n.entity_type = $etype, n.updated_at = datetime()",
                eid=entity_id, props=props, etype=entity_type,
            )
        with driver.session() as s:
            for field, value in payload.items():
                if not field.endswith("_id") or field == id_field or not value:
                    continue
                ref_label = _to_label(field[:-3])
                rel_type  = f"HAS_{label.upper()}"
                try:
                    s.run(
                        f"MATCH (ref:`{ref_label}` {{entity_id: $ref_id}}) "
                        f"MATCH (n:`{label}` {{entity_id: $eid}}) "
                        f"MERGE (ref)-[:`{rel_type}`]->(n)",
                        ref_id=str(value), eid=entity_id,
                    )
                except Exception:
                    pass
        return True
    except Exception as e:
        log.warning(f"Neo4j write failed ({entity_type}): {e}")
        return False


def _write_qdrant(qdrant: QdrantClient, entity_type: str, payload: dict) -> bool:
    _, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False
    collection = (
        "event_vectors"
        if _singularize(entity_type.lower()) in EVENT_ENTITY_TYPES
        else "entity_vectors"
    )
    vector = _embed(_embed_text(entity_type, payload))
    if not vector:
        return False
    doc_id = abs(hash(entity_id)) % (2 ** 63)
    pt_payload = {
        "entity_type": entity_type,
        "entity_id":   entity_id,
        **{k: v for k, v in payload.items()
           if not k.endswith("_at") and v is not None
           and isinstance(v, (str, int, float, bool))},
    }
    try:
        qdrant.upsert(
            collection_name=collection,
            points=[PointStruct(id=doc_id, vector=vector, payload=pt_payload)],
        )
        return True
    except Exception as e:
        log.warning(f"Qdrant write failed ({entity_type}): {e}")
        return False


def _write_timescale(ts_conn, entity_type: str, payload: dict, source_label: str) -> bool:
    _, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False
    try:
        cur = ts_conn.cursor()
        cur.execute(
            "INSERT INTO brain_events "
            "  (time, entity_type, entity_id, event_type, source_topic, payload) "
            "VALUES (NOW(), %s, %s, %s, %s, %s)",
            (entity_type, entity_id, _singularize(entity_type.lower()),
             source_label, json.dumps(payload)),
        )
        ts_conn.commit()
        cur.close()
        return True
    except Exception as e:
        ts_conn.rollback()
        log.warning(f"TimescaleDB write failed: {e}")
        return False


# ── Background ingest worker ───────────────────────────────────────────────────

def _run_ingest(job_id: str, records: list, entity_type: str,
                source_label: str, rules: list):
    def _upd(updates: dict):
        with _jobs_lock:
            _jobs[job_id].update(updates)

    _upd({"status": "running"})

    try:
        driver  = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        qdrant  = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        existing = {c.name for c in qdrant.get_collections().collections}
        for name, dim in QDRANT_COLLECTIONS.items():
            if name not in existing:
                qdrant.create_collection(
                    name, vectors_config=VectorParams(size=dim, distance=Distance.COSINE)
                )
        ts_conn = psycopg2.connect(**TIMESCALE_CONN)
    except Exception as e:
        _upd({"status": "failed", "error": str(e), "finished_at": time.time()})
        return

    n_cnt = q_cnt = t_cnt = err_cnt = 0
    try:
        for i, record in enumerate(records):
            processed = _apply_rules(record, rules) if rules else record
            n = _write_neo4j(driver, entity_type, processed)
            q = _write_qdrant(qdrant, entity_type, processed)
            t = _write_timescale(ts_conn, entity_type, processed, source_label)
            if n: n_cnt += 1
            if q: q_cnt += 1
            if t: t_cnt += 1
            if not (n or q or t): err_cnt += 1
            if (i + 1) % 10 == 0:
                _upd({"done": i + 1, "neo4j": n_cnt, "qdrant": q_cnt,
                      "timescale": t_cnt, "errors": err_cnt})
    except Exception as e:
        _upd({"status": "failed", "error": str(e), "finished_at": time.time()})
        return
    finally:
        try: driver.close()
        except Exception: pass
        try: ts_conn.close()
        except Exception: pass

    _upd({"status": "done", "done": len(records), "neo4j": n_cnt, "qdrant": q_cnt,
          "timescale": t_cnt, "errors": err_cnt, "finished_at": time.time()})


# ── File preview ───────────────────────────────────────────────────────────────

@router.post("/preview/file")
async def preview_file(file: UploadFile = File(...)):
    """Parse an uploaded file and return columns + first 20 rows."""
    content  = await file.read()
    filename = file.filename or ""
    try:
        if filename.endswith(".json"):
            data    = json.loads(content)
            records = data if isinstance(data, list) else [data]
        elif filename.endswith(".csv"):
            reader  = csv.DictReader(io.StringIO(content.decode("utf-8")))
            records = [dict(row) for row in reader]
        else:
            raise HTTPException(400, "Only .json and .csv files are supported")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Failed to parse file: {e}")

    columns = list(records[0].keys()) if records else []
    return {"columns": columns, "preview": records[:20], "total": len(records)}


# ── File ingest ────────────────────────────────────────────────────────────────

@router.post("/file")
async def ingest_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    entity_type: str = Form(...),
    source_name: Optional[str] = Form(None),
    apply_rules: bool = Form(False),
    db: Session = Depends(get_db),
):
    """Upload a JSON/CSV file and ingest records into all brain stores."""
    content  = await file.read()
    filename = file.filename or "unknown"
    try:
        if filename.endswith(".json"):
            data    = json.loads(content)
            records = data if isinstance(data, list) else [data]
        elif filename.endswith(".csv"):
            reader  = csv.DictReader(io.StringIO(content.decode("utf-8")))
            records = [dict(row) for row in reader]
        else:
            raise HTTPException(400, "Only .json and .csv files are supported")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Failed to parse file: {e}")

    rules = []
    if apply_rules and source_name:
        src = db.query(GovernanceSource).filter_by(source_name=source_name).first()
        if src:
            rules = db.query(GovernanceRule).filter_by(
                source_id=src.id, is_active=True
            ).all()

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id, "status": "queued",
            "source": f"file:{filename}", "entity_type": entity_type,
            "total": len(records), "done": 0,
            "neo4j": 0, "qdrant": 0, "timescale": 0, "errors": 0,
            "started_at": time.time(),
        }
    background_tasks.add_task(
        _run_ingest, job_id, records, entity_type, f"bulk-file:{filename}", rules
    )
    return {"job_id": job_id, "total": len(records), "status": "queued"}


# ── Query preview ──────────────────────────────────────────────────────────────

class DBQueryRequest(BaseModel):
    db_type:     str = "postgresql"
    host:        str
    port:        int = 5432
    database:    str
    username:    str
    password:    str
    query:       str
    entity_type: str
    source_name: Optional[str] = None
    apply_rules: bool = False


@router.post("/preview/query")
def preview_query(payload: DBQueryRequest):
    """Test DB connection + query, return first 20 rows and total count."""
    try:
        conn = psycopg2.connect(
            host=payload.host, port=payload.port, dbname=payload.database,
            user=payload.username, password=payload.password, connect_timeout=10,
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(payload.query)
        rows = [dict(r) for r in cur.fetchmany(20)]
        cur.execute(f"SELECT COUNT(*) FROM ({payload.query}) _q")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        raise HTTPException(400, f"Query failed: {e}")
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "preview": rows, "total": total}


# ── Query ingest ───────────────────────────────────────────────────────────────

@router.post("/query")
def ingest_query(
    payload: DBQueryRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Run a SELECT query on a PostgreSQL database and ingest into brain stores."""
    try:
        conn = psycopg2.connect(
            host=payload.host, port=payload.port, dbname=payload.database,
            user=payload.username, password=payload.password, connect_timeout=10,
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(payload.query)
        records = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        raise HTTPException(400, f"Query failed: {e}")

    rules = []
    if payload.apply_rules and payload.source_name:
        src = db.query(GovernanceSource).filter_by(source_name=payload.source_name).first()
        if src:
            rules = db.query(GovernanceRule).filter_by(
                source_id=src.id, is_active=True
            ).all()

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id, "status": "queued",
            "source": f"query:{payload.database}", "entity_type": payload.entity_type,
            "total": len(records), "done": 0,
            "neo4j": 0, "qdrant": 0, "timescale": 0, "errors": 0,
            "started_at": time.time(),
        }
    background_tasks.add_task(
        _run_ingest, job_id, records, payload.entity_type,
        f"bulk-query:{payload.database}", rules,
    )
    return {"job_id": job_id, "total": len(records), "status": "queued"}


# ── Job status ─────────────────────────────────────────────────────────────────

@router.get("/jobs")
def list_jobs():
    with _jobs_lock:
        return sorted(_jobs.values(), key=lambda j: j.get("started_at", 0), reverse=True)


@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return dict(job)

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

import openpyxl
import xlrd

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
    """
    Find the best primary-key field in a record.
    Tries progressively looser rules; falls back to first non-null value
    so records without explicit ID fields are never silently dropped.
    """
    def _norm(k: str) -> str:
        return k.lower().replace(" ", "_").replace("-", "_").strip("_")

    keys = list(payload.keys())

    # 1. Exact "id" (case-insensitive)
    for k in keys:
        if _norm(k) == "id" and payload[k] is not None:
            return k, str(payload[k])

    # 2. Ends with a recognised ID suffix
    ID_SUFFIXES = ("_id", "_no", "_num", "_number", "_code",
                   "_key", "_ref", "_uuid", "_guid", "_sn", "_seq")
    for k in keys:
        nk = _norm(k)
        if any(nk.endswith(s) for s in ID_SUFFIXES) and payload[k] is not None:
            return k, str(payload[k])

    # 3. Contains a recognised ID keyword anywhere
    ID_TOKENS = ("id", "number", "code", "reference", "ref", "key", "serial")
    for k in keys:
        nk = _norm(k)
        if any(tok in nk for tok in ID_TOKENS) and payload[k] is not None:
            return k, str(payload[k])

    # 4. Fall back to the first non-empty value in the record
    for k in keys:
        v = payload[k]
        if v is not None and str(v).strip():
            return k, str(v)

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
    first_err: str = ""
    try:
        for i, record in enumerate(records):
            processed = _apply_rules(record, rules) if rules else record
            n = _write_neo4j(driver, entity_type, processed)
            q = _write_qdrant(qdrant, entity_type, processed)
            t = _write_timescale(ts_conn, entity_type, processed, source_label)
            if n: n_cnt += 1
            if q: q_cnt += 1
            if t: t_cnt += 1
            if not (n or q or t):
                err_cnt += 1
                if not first_err:
                    _, eid = _find_primary_id(processed)
                    first_err = (
                        f"Record {i+1}: no primary ID found. "
                        f"Columns: {list(processed.keys())[:6]}"
                        if eid is None
                        else f"Record {i+1} (id={eid}): all store writes failed"
                    )
            if (i + 1) % 10 == 0:
                _upd({"done": i + 1, "neo4j": n_cnt, "qdrant": q_cnt,
                      "timescale": t_cnt, "errors": err_cnt,
                      **({"first_error": first_err} if first_err else {})})
    except Exception as e:
        _upd({"status": "failed", "error": str(e), "finished_at": time.time()})
        return
    finally:
        try: driver.close()
        except Exception: pass
        try: ts_conn.close()
        except Exception: pass

    _upd({"status": "done", "done": len(records), "neo4j": n_cnt, "qdrant": q_cnt,
          "timescale": t_cnt, "errors": err_cnt, "finished_at": time.time(),
          **({"first_error": first_err} if first_err else {})})


# ── File parser ────────────────────────────────────────────────────────────────

def _parse_file(content: bytes, filename: str) -> list[dict]:
    """Parse JSON, CSV, XLS, or XLSX bytes into a list of dicts."""
    name = (filename or "").lower()

    if name.endswith(".json"):
        data = json.loads(content)
        return data if isinstance(data, list) else [data]

    if name.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        return [dict(row) for row in reader]

    if name.endswith(".xlsx"):
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
        records = []
        for row in rows[1:]:
            rec = {}
            for h, v in zip(headers, row):
                if v is None:
                    continue          # skip empty cells
                rec[h] = str(v) if not isinstance(v, (int, float, bool)) else v
            if rec:
                records.append(rec)
        wb.close()
        return records

    if name.endswith(".xls"):
        wb = xlrd.open_workbook(file_contents=content)
        ws = wb.sheet_by_index(0)
        if ws.nrows == 0:
            return []
        headers = [str(ws.cell_value(0, c)) for c in range(ws.ncols)]
        records = []
        for r in range(1, ws.nrows):
            rec = {}
            for c, h in enumerate(headers):
                v = ws.cell_value(r, c)
                if v == "":
                    continue
                # xlrd returns floats for all numbers; convert integers
                if isinstance(v, float) and v == int(v):
                    v = int(v)
                rec[h] = v
            if rec:
                records.append(rec)
        return records

    raise HTTPException(400, "Unsupported file format. Accepted: .json, .csv, .xlsx, .xls")


# ── File preview ────────────────────────────────────────────────────────────────

@router.post("/preview/file")
async def preview_file(file: UploadFile = File(...)):
    """Parse an uploaded file and return columns + first 20 rows."""
    content  = await file.read()
    filename = file.filename or ""
    try:
        records = _parse_file(content, filename)
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
        records = _parse_file(content, filename)
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


# ── Unified DB fetch helper ────────────────────────────────────────────────────

def _db_fetch(db_type: str, host: str, port: int, database: str,
              username: str, password: str, query: str,
              preview: bool = False) -> tuple:
    """
    Connect to PostgreSQL, MySQL, or Oracle; run query; return (rows, total).
    preview=True fetches only the first 20 rows and estimates total with COUNT(*).
    """
    lower = db_type.lower()

    if lower == "postgresql":
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=database,
                user=username, password=password, connect_timeout=10,
            )
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query)
            rows = [dict(r) for r in (cur.fetchmany(20) if preview else cur.fetchall())]
            if preview:
                cur.execute(f"SELECT COUNT(*) FROM ({query}) _q")
                total = cur.fetchone()[0]
            else:
                total = len(rows)
            cur.close()
            conn.close()
        except psycopg2.Error as e:
            raise HTTPException(400, f"PostgreSQL error: {e}")

    elif lower == "mysql":
        try:
            import pymysql
            import pymysql.cursors
            conn = pymysql.connect(
                host=host, port=port, database=database,
                user=username, password=password,
                connect_timeout=10,
                cursorclass=pymysql.cursors.DictCursor,
            )
            with conn.cursor() as cur:
                cur.execute(query)
                rows = list(cur.fetchmany(20) if preview else cur.fetchall())
            if preview:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) AS cnt FROM ({query}) _q")
                    total = cur.fetchone()["cnt"]
            else:
                total = len(rows)
            conn.close()
        except Exception as e:
            raise HTTPException(400, f"MySQL error: {e}")

    elif lower == "oracle":
        try:
            import oracledb
            conn = oracledb.connect(
                user=username, password=password,
                dsn=f"{host}:{port}/{database}",
            )
            cur = conn.cursor()
            cur.execute(query)
            cols = [d[0].lower() for d in cur.description]
            rows = [dict(zip(cols, row))
                    for row in (cur.fetchmany(20) if preview else cur.fetchall())]
            if preview:
                cur.execute(f"SELECT COUNT(*) FROM ({query})")
                total = cur.fetchone()[0]
            else:
                total = len(rows)
            cur.close()
            conn.close()
        except Exception as e:
            raise HTTPException(400, f"Oracle error: {e}")

    else:
        raise HTTPException(400, f"Unsupported DB type '{db_type}'. Use postgresql, mysql, or oracle.")

    # Coerce all values to JSON-safe primitives (Oracle can return Decimal/Date types)
    safe_rows = []
    for row in rows:
        safe_row: dict = {}
        for k, v in row.items():
            if v is None or isinstance(v, (str, int, float, bool)):
                safe_row[k] = v
            else:
                safe_row[k] = str(v)
        safe_rows.append(safe_row)

    return safe_rows, total


# ── Query preview ──────────────────────────────────────────────────────────────

@router.post("/preview/query")
def preview_query(payload: DBQueryRequest):
    """Test connection + query for any supported DB; return first 20 rows and total count."""
    rows, total = _db_fetch(
        payload.db_type, payload.host, payload.port, payload.database,
        payload.username, payload.password, payload.query, preview=True,
    )
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "preview": rows, "total": total}


# ── Query ingest ───────────────────────────────────────────────────────────────

@router.post("/query")
def ingest_query(
    payload: DBQueryRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Run a SELECT query on PostgreSQL, MySQL, or Oracle and ingest into brain stores."""
    records, _ = _db_fetch(
        payload.db_type, payload.host, payload.port, payload.database,
        payload.username, payload.password, payload.query, preview=False,
    )

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

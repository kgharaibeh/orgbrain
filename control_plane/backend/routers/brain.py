"""
Brain Router — monitoring for Neo4j, Qdrant, TimescaleDB, and Ollama.
Generic / domain-agnostic: reads from brain_events and brain_signals.
"""

import logging
import os
import httpx
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

log = logging.getLogger(__name__)
router = APIRouter()

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")
QDRANT_HOST    = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT    = int(os.getenv("QDRANT_PORT", "6333"))
OLLAMA_HOST    = os.getenv("OLLAMA_HOST", "http://ollama:11434")

TIMESCALE_CONN = {
    "host":     os.getenv("TIMESCALE_HOST",     "timescale"),
    "port":     int(os.getenv("TIMESCALE_PORT", "5432")),
    "dbname":   os.getenv("TIMESCALE_DB",       "orgbrain_metrics"),
    "user":     os.getenv("TIMESCALE_USER",     "orgbrain"),
    "password": os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret"),
}


@router.get("/stats")
def brain_stats():
    """Aggregate health and size metrics from all brain stores."""
    return {
        "graph":      _neo4j_stats(),
        "vector":     _qdrant_stats(),
        "timeseries": _timescale_stats(),
        "llm":        _ollama_stats(),
    }


def _neo4j_stats():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as s:
            try:
                counts = s.run("""
                    CALL apoc.meta.stats()
                    YIELD labels, relTypesCount, nodeCount, relCount
                    RETURN labels, relTypesCount, nodeCount, relCount
                """).single()
                if counts:
                    return {
                        "status": "healthy",
                        "node_count": counts["nodeCount"],
                        "rel_count":  counts["relCount"],
                        "labels":     counts["labels"],
                    }
            except Exception:
                pass
            nc = s.run("MATCH (n) RETURN count(n) as c").single()["c"]
            rc = s.run("MATCH ()-[r]->() RETURN count(r) as c").single()["c"]
            return {"status": "healthy", "node_count": nc, "rel_count": rc, "labels": {}}
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        try:
            driver.close()
        except Exception:
            pass


def _qdrant_stats():
    try:
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        collections = client.get_collections().collections
        result = {"status": "healthy", "collections": []}
        for c in collections:
            info = client.get_collection(c.name)
            result["collections"].append({
                "name":          c.name,
                "vector_count":  getattr(info, "points_count", None) or getattr(info, "vectors_count", 0),
                "indexed_count": getattr(info, "indexed_vectors_count", 0),
                "status":        str(info.status),
            })
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _timescale_stats():
    try:
        conn = psycopg2.connect(**TIMESCALE_CONN)
        result = {"status": "healthy"}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                cur.execute("""
                    SELECT
                        COUNT(*)                    AS total_events,
                        COUNT(DISTINCT entity_id)   AS unique_entities,
                        COUNT(DISTINCT entity_type) AS entity_types,
                        MAX(time)                   AS latest_event
                    FROM brain_events
                """)
                row = cur.fetchone()
                if row:
                    result.update(dict(row))
            except Exception:
                conn.rollback()
                result["brain_events"] = "unavailable"

            try:
                cur.execute("""
                    SELECT
                        COUNT(*)                    AS total_signals,
                        COUNT(DISTINCT signal_type) AS signal_types,
                        COUNT(DISTINCT entity_type) AS entity_types_scored,
                        MAX(time)                   AS latest_signal
                    FROM brain_signals
                """)
                row = cur.fetchone()
                if row:
                    result["signals"] = dict(row)
            except Exception:
                conn.rollback()
                result["brain_signals"] = "unavailable"

        conn.close()
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _ollama_stats():
    try:
        resp = httpx.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        models = resp.json().get("models", [])
        return {
            "status": "healthy",
            "models": [{"name": m["name"], "size_gb": round(m.get("size", 0) / 1e9, 2)} for m in models],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/graph/entity-counts")
def graph_entity_counts():
    """Count of each entity type in the knowledge graph."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as s:
            result = s.run("""
                MATCH (n)
                RETURN labels(n)[0] AS label, count(n) AS count
                ORDER BY count DESC
            """)
            return [{"label": r["label"], "count": r["count"]} for r in result]
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            driver.close()
        except Exception:
            pass


@router.get("/graph/recent-activity")
def graph_recent_activity(limit: int = 20):
    """Most recently updated nodes in the knowledge graph (any entity type)."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as s:
            result = s.run("""
                MATCH (n)
                WHERE n.updated_at IS NOT NULL
                RETURN labels(n)[0] AS entity_type,
                       n.entity_id   AS entity_id,
                       n.updated_at  AS updated_at,
                       n.summary     AS summary
                ORDER BY n.updated_at DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(r) for r in result]
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        try:
            driver.close()
        except Exception:
            pass


@router.get("/timeseries/signals")
def signals_summary(signal_type: str = None, limit: int = 50):
    """Top scored entities from brain_signals (any domain, any signal type)."""
    try:
        conn = psycopg2.connect(**TIMESCALE_CONN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if signal_type:
                cur.execute("""
                    SELECT entity_type, entity_id, signal_type, score, metadata, time, source_dag
                    FROM brain_latest_signals
                    WHERE signal_type = %s
                    ORDER BY score DESC
                    LIMIT %s
                """, (signal_type, limit))
            else:
                cur.execute("""
                    SELECT entity_type, entity_id, signal_type, score, metadata, time, source_dag
                    FROM brain_latest_signals
                    ORDER BY score DESC
                    LIMIT %s
                """, (limit,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/timeseries/high-risk")
def high_risk_entities(limit: int = 50):
    """High-risk entities (score > 0.6) across all domains and signal types."""
    try:
        conn = psycopg2.connect(**TIMESCALE_CONN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT entity_type, entity_id, signal_type, score, metadata, time
                FROM brain_high_risk_entities
                LIMIT %s
            """, (limit,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/timeseries/activity")
def entity_activity():
    """Event activity summary per entity type (last 7 days)."""
    try:
        conn = psycopg2.connect(**TIMESCALE_CONN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM brain_entity_activity")
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/qdrant/init-collections")
def init_qdrant_collections():
    """Initialize generic Qdrant collections (entity_vectors, event_vectors)."""
    from qdrant_client.models import Distance, VectorParams
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    EMBEDDING_DIM = 768
    COLLECTIONS = ["entity_vectors", "event_vectors"]
    existing = {c.name for c in client.get_collections().collections}
    created = []
    for name in COLLECTIONS:
        if name not in existing:
            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
            )
            created.append(name)
    return {
        "created":        created,
        "already_existed": [n for n in COLLECTIONS if n not in created],
    }


@router.post("/neo4j/init-schema")
def init_neo4j_schema():
    """Apply the ontology schema (constraints, indexes) to Neo4j from a Cypher file."""
    schema_path = "/app/brain/graph/schema/ontology.cypher"
    if not os.path.exists(schema_path):
        # try alternative path
        schema_path = "/app/brain/graph/schema/banking_ontology.cypher"
    if not os.path.exists(schema_path):
        raise HTTPException(404, f"Schema file not found at {schema_path}")
    with open(schema_path) as f:
        statements = [s.strip() for s in f.read().split(";")
                      if s.strip() and not s.strip().startswith("//")]
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    results = []
    with driver.session() as s:
        for stmt in statements:
            try:
                s.run(stmt)
                results.append({"status": "ok", "stmt": stmt[:80]})
            except Exception as e:
                results.append({"status": "error", "stmt": stmt[:80], "error": str(e)})
    driver.close()
    return results

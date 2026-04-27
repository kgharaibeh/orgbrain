"""
Brain Ingest — Generic Domain-Agnostic Pipeline
Consumes records from clean.* Kafka topics (or reads from simulation/data/)
and loads them into:
  - Neo4j       (graph — dynamic entity nodes + foreign-key relationships)
  - Qdrant      (vectors — entity_vectors + event_vectors collections)
  - TimescaleDB (brain_events hypertable — generic time-series)

Works for any domain: banking, retail, manufacturing, insurance, etc.
Entity types and relationships are derived from the data, not hardcoded.

Run: python ingest_to_brain.py [--from-files] [--brokers localhost:9094]
"""

import argparse
import json
import os
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams

# ── Config ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",          "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",         "neo4j")
NEO4J_PASS     = os.getenv("NEO4J_PASSWORD",     "orgbrain_neo4j")
QDRANT_HOST    = os.getenv("QDRANT_HOST",        "localhost")
QDRANT_PORT    = int(os.getenv("QDRANT_PORT",    "6333"))
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST",     "localhost")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5433"))
TIMESCALE_DB   = os.getenv("TIMESCALE_DB",       "orgbrain_metrics")
TIMESCALE_USER = os.getenv("TIMESCALE_USER",     "orgbrain")
TIMESCALE_PASS = os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret")
OLLAMA_HOST    = os.getenv("OLLAMA_HOST",        "http://localhost:11434")

DATA_DIR = Path(__file__).parent / "data"

CLEAN_TOPICS = [
    "clean.core_banking.customers",
    "clean.core_banking.accounts",
    "clean.core_banking.transactions",
    "clean.core_banking.cards",
    "clean.core_banking.loans",
]

# Entity types whose records represent events/transactions → event_vectors
# Everything else → entity_vectors
EVENT_ENTITY_TYPES = {
    "transaction", "order", "event", "claim", "log",
    "activity", "entry", "record", "invoice", "payment",
    "reading", "alert", "incident", "interaction",
}

QDRANT_COLLECTIONS = {
    "entity_vectors": 768,
    "event_vectors":  768,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _singularize(word: str) -> str:
    w = word.lower()
    if w.endswith("ies"):
        return w[:-3] + "y"
    if w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


def _to_label(name: str) -> str:
    """Convert a plural name like 'customers' to a safe Neo4j label 'Customer'."""
    singular = _singularize(name)
    clean = "".join(c for c in singular if c.isalnum() or c == "_")
    return clean[0].upper() + clean[1:] if clean else "Entity"


def _find_primary_id(payload: dict) -> tuple:
    """Return (field_name, value) for the first *_id field — treated as primary key."""
    for k, v in payload.items():
        if k.endswith("_id") and v is not None:
            return k, str(v)
    if "id" in payload and payload["id"] is not None:
        return "id", str(payload["id"])
    return None, None


def _embed_text(entity_type: str, payload: dict) -> str:
    """Build a human-readable text string from a record for semantic embedding."""
    parts = [entity_type]
    for k, v in payload.items():
        if v is None or k.endswith("_id") or k.endswith("_at"):
            continue
        parts.append(f"{k}={v}")
        if len(parts) > 20:
            break
    return " ".join(parts)


def _scalar_props(payload: dict) -> dict:
    """Filter payload to only scalar types Neo4j can store as node properties."""
    return {
        k: v for k, v in payload.items()
        if v is not None and isinstance(v, (str, int, float, bool))
    }


# ── Embedding ──────────────────────────────────────────────────────────────────

def embed(text: str) -> list:
    try:
        r = requests.post(
            f"{OLLAMA_HOST}/api/embeddings",
            json={"model": "nomic-embed-text", "prompt": text},
            timeout=120,
        )
        r.raise_for_status()
        return r.json()["embedding"]
    except Exception as e:
        print(f"  Embedding failed: {e}")
        return []


# ── Qdrant bootstrap ───────────────────────────────────────────────────────────

def ensure_qdrant_collections(qdrant: QdrantClient):
    existing = {c.name for c in qdrant.get_collections().collections}
    for name, dim in QDRANT_COLLECTIONS.items():
        if name not in existing:
            qdrant.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )
            print(f"  Created Qdrant collection: {name} (dim={dim})")


# ── Neo4j generic ingest ───────────────────────────────────────────────────────

def ingest_neo4j(driver, entity_type: str, payload: dict) -> bool:
    label = _to_label(entity_type)
    id_field, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False

    props = _scalar_props(payload)
    cypher = (
        f"MERGE (n:`{label}` {{entity_id: $entity_id}}) "
        f"SET n += $props, n.entity_type = $etype, n.updated_at = datetime()"
    )
    try:
        with driver.session() as s:
            s.run(cypher, entity_id=entity_id, props=props, etype=entity_type)

        # Create edges for every foreign-key reference field (*_id != primary key)
        with driver.session() as s:
            for field, value in payload.items():
                if not field.endswith("_id") or field == id_field or not value:
                    continue
                ref_label = _to_label(field[:-3])   # account_id → Account
                rel_type  = f"HAS_{label.upper()}"  # Account → HAS_TRANSACTION
                try:
                    s.run(
                        f"MATCH (ref:`{ref_label}` {{entity_id: $ref_id}}) "
                        f"MATCH (n:`{label}` {{entity_id: $eid}}) "
                        f"MERGE (ref)-[:`{rel_type}`]->(n)",
                        ref_id=str(value), eid=entity_id,
                    )
                except Exception:
                    pass  # referenced node may not exist yet — ontology DAG fills gaps
        return True
    except Exception as e:
        print(f"  Neo4j write failed ({entity_type}): {e}")
        return False


# ── Qdrant generic ingest ──────────────────────────────────────────────────────

def ingest_qdrant(qdrant: QdrantClient, entity_type: str, payload: dict) -> bool:
    _, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False

    collection = (
        "event_vectors"
        if _singularize(entity_type.lower()) in EVENT_ENTITY_TYPES
        else "entity_vectors"
    )

    vector = embed(_embed_text(entity_type, payload))
    if not vector:
        return False

    doc_id = abs(hash(entity_id)) % (2 ** 63)
    point_payload = {
        "entity_type": entity_type,
        "entity_id":   entity_id,
        **{k: v for k, v in payload.items()
           if not k.endswith("_at") and v is not None
           and isinstance(v, (str, int, float, bool))},
    }
    try:
        qdrant.upsert(
            collection_name=collection,
            points=[PointStruct(id=doc_id, vector=vector, payload=point_payload)],
        )
        return True
    except Exception as e:
        print(f"  Qdrant upsert failed ({entity_type}): {e}")
        return False


# ── TimescaleDB generic ingest ─────────────────────────────────────────────────

def ingest_timescale(ts_conn, entity_type: str, payload: dict, source_topic: str = None) -> bool:
    _, entity_id = _find_primary_id(payload)
    if not entity_id:
        return False
    try:
        cur = ts_conn.cursor()
        cur.execute(
            """
            INSERT INTO brain_events
                (time, entity_type, entity_id, event_type, source_topic, payload)
            VALUES (NOW(), %s, %s, %s, %s, %s)
            """,
            (entity_type, entity_id, _singularize(entity_type.lower()),
             source_topic, json.dumps(payload)),
        )
        ts_conn.commit()
        cur.close()
        return True
    except Exception as e:
        ts_conn.rollback()
        print(f"  TimescaleDB write failed: {e}")
        return False


# ── Unified record ingest ──────────────────────────────────────────────────────

def ingest_record(driver, qdrant, ts_conn, entity_type: str, payload: dict,
                  source_topic: str = None) -> dict:
    return {
        "neo4j":     ingest_neo4j(driver, entity_type, payload),
        "qdrant":    ingest_qdrant(qdrant, entity_type, payload),
        "timescale": ingest_timescale(ts_conn, entity_type, payload, source_topic),
    }


# ── Ingest from files ──────────────────────────────────────────────────────────

def ingest_from_files(driver, qdrant, ts_conn):
    total = {"neo4j": 0, "qdrant": 0, "timescale": 0}
    for path in sorted(DATA_DIR.glob("*.json")):
        entity_type = _to_label(path.stem)   # customers.json → Customer
        records     = json.loads(path.read_text())
        print(f"  Ingesting {len(records):4d} {entity_type} records…")
        for record in records:
            counts = ingest_record(driver, qdrant, ts_conn, entity_type, record)
            for k in total:
                if counts[k]:
                    total[k] += 1
    return total


# ── Ingest from Kafka ──────────────────────────────────────────────────────────

def ingest_from_kafka(driver, qdrant, ts_conn, brokers: str, idle_timeout: int = 10):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        *CLEAN_TOPICS,
        bootstrap_servers=brokers.split(","),
        group_id="orgbrain-brain-ingest-sim",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=idle_timeout * 1000,
        api_version=(3, 6, 0),
    )

    total     = {"neo4j": 0, "qdrant": 0, "timescale": 0}
    processed = 0
    for msg in consumer:
        value       = msg.value
        payload     = value.get("payload", value)
        entity_type = _to_label(msg.topic.split(".")[-1])  # clean.*.customers → Customer
        counts = ingest_record(driver, qdrant, ts_conn, entity_type, payload,
                               source_topic=msg.topic)
        for k in total:
            if counts[k]:
                total[k] += 1
        processed += 1
        if processed % 50 == 0:
            print(f"  Processed {processed} messages…")

    consumer.close()
    return total


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest any domain data into the Brain stores")
    parser.add_argument("--from-files",   action="store_true", help="Read from simulation/data/")
    parser.add_argument("--brokers",      default="localhost:9094")
    parser.add_argument("--idle-timeout", type=int, default=10)
    args = parser.parse_args()

    print("Connecting to stores…")
    driver  = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    qdrant  = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    ensure_qdrant_collections(qdrant)
    ts_conn = psycopg2.connect(
        host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
        user=TIMESCALE_USER, password=TIMESCALE_PASS,
    )

    try:
        if args.from_files:
            print("Reading from simulation/data/ files…")
            counts = ingest_from_files(driver, qdrant, ts_conn)
        else:
            print(f"Consuming from Kafka clean topics (idle timeout={args.idle_timeout}s)…")
            counts = ingest_from_kafka(driver, qdrant, ts_conn, args.brokers, args.idle_timeout)
    finally:
        driver.close()
        ts_conn.close()

    print(
        f"\nBrain ingest complete:\n"
        f"  Neo4j nodes/edges written : {counts['neo4j']}\n"
        f"  Qdrant vectors upserted   : {counts['qdrant']}\n"
        f"  TimescaleDB rows inserted  : {counts['timescale']}\n"
        f"\nOpen the Control Plane at http://localhost:3001\n"
        f"  - Brain Monitor : entity counts, recent activity\n"
        f"  - Ontology      : live schema, trigger enrichment DAGs\n"
        f"  - AI Agent      : query the knowledge base\n"
    )


if __name__ == "__main__":
    main()

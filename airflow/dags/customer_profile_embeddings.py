"""
DAG: entity_embeddings
Weekly — re-generates Qdrant embeddings for ALL entity types in Neo4j.
Uses nomic-embed-text via Ollama. Upserts into entity_vectors collection.

Domain-agnostic: discovers all node labels dynamically from Neo4j.
Works for Customer, Product, Asset, Policy, Employee, Device, or any entity.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":             "orgbrain",
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="customer_profile_embeddings",
    description="Re-embed all entities from Neo4j into Qdrant entity_vectors (weekly)",
    schedule_interval="0 3 * * 6",  # Saturday 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["brain", "qdrant", "embeddings", "generic"],
) as dag:

    def rebuild_embeddings(**context):
        import requests
        from neo4j import GraphDatabase
        from qdrant_client import QdrantClient
        from qdrant_client.http.models import PointStruct

        NEO4J_URI   = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
        NEO4J_USER  = os.getenv("NEO4J_USER",     "neo4j")
        NEO4J_PASS  = os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")
        OLLAMA      = os.getenv("OLLAMA_HOST",    "http://ollama:11434")
        QDRANT_HOST = os.getenv("QDRANT_HOST",    "qdrant")
        QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

        def embed(text):
            r = requests.post(
                f"{OLLAMA}/api/embeddings",
                json={"model": "nomic-embed-text", "prompt": text},
                timeout=120,
            )
            r.raise_for_status()
            return r.json()["embedding"]

        def node_to_text(label, props):
            parts = [label]
            for k, v in props.items():
                if v is None or k in ("entity_id", "updated_at", "summary"):
                    continue
                parts.append(f"{k}={v}")
                if len(parts) > 20:
                    break
            return " ".join(parts)

        total_upserted = 0

        with driver.session() as s:
            # Discover all entity types dynamically
            labels = [r["label"] for r in s.run(
                "CALL db.labels() YIELD label RETURN label"
            ).data()]

        for label in labels:
            with driver.session() as s:
                nodes = s.run(
                    f"MATCH (n:`{label}`) RETURN n LIMIT 1000"
                ).data()

            label_count = 0
            for row in nodes:
                node  = row["n"]
                props = dict(node)
                eid   = props.get("entity_id")
                if not eid:
                    # fallback: find any *_id field
                    eid = next((str(v) for k, v in props.items()
                                if k.endswith("_id") and v), None)
                if not eid:
                    continue

                text   = node_to_text(label, props)
                doc_id = abs(hash(eid)) % (2 ** 63)

                try:
                    vector = embed(text)
                    qdrant.upsert(
                        collection_name="entity_vectors",
                        points=[PointStruct(
                            id=doc_id,
                            vector=vector,
                            payload={
                                "entity_type": label,
                                "entity_id":   eid,
                                **{k: v for k, v in props.items()
                                   if v is not None
                                   and isinstance(v, (str, int, float, bool))
                                   and k not in ("updated_at",)},
                            },
                        )],
                    )
                    label_count += 1
                except Exception as e:
                    print(f"  Embedding failed for {label} {eid}: {e}")

            print(f"  {label}: {label_count} vectors upserted")
            total_upserted += label_count

        driver.close()
        print(f"Re-embedding complete: {total_upserted} total vectors upserted")
        return total_upserted

    PythonOperator(
        task_id="rebuild_entity_embeddings",
        python_callable=rebuild_embeddings,
    )

"""
Relationship Inference Engine — Generic / Domain-Agnostic
Discovers all node labels in Neo4j dynamically, samples representative nodes
from each type, then asks Ollama to propose new relationship types or node
properties. Proposals are persisted to cp_schema_proposals for human review.

Works for any domain: banking, retail, manufacturing, insurance, SaaS, etc.
"""

import json
import logging
import os
import re

import psycopg2
import requests
from neo4j import GraphDatabase

log = logging.getLogger(__name__)

NEO4J_URI    = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER   = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASS   = os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",    "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL",   "llama3.1:8b")
DATABASE_URL = os.getenv("DATABASE_URL",   "postgresql://orgbrain:orgbrain_secret@postgres:5432/core_banking")

_PROMPT = """You are a knowledge graph schema expert.
Below is a sample of entities and relationships from a Neo4j knowledge graph.
The domain is unknown — reason purely from the data patterns you observe.

=== ENTITY TYPES AND SAMPLE NODES ===
{entity_sample}

=== EXISTING RELATIONSHIP TYPES ===
{existing_relationships}

Your task: propose NEW relationship types or node properties that would enrich
this schema based on the patterns visible in the data.

Rules:
- Only propose relationships or properties that do NOT already exist above.
- Each Cypher must be safe to run as a MERGE (idempotent).
- Use only node labels and property names that appear in the sample above.
- Confidence 0.0–1.0 based on how clearly the pattern appears in the data.
- Do NOT assume any particular domain — derive everything from the data.

Respond ONLY with a valid JSON array. No prose outside the JSON.

[
  {{
    "type": "NEW_RELATIONSHIP",
    "entity_type": "<source label from sample>",
    "relationship_type": "<SNAKE_CASE_REL_NAME>",
    "target_entity": "<target label from sample>",
    "cypher": "<idempotent MERGE Cypher using only labels/properties from the sample>",
    "rationale": "<one sentence: why this pattern is useful>",
    "confidence": 0.0
  }}
]

Proposals:"""


def _sample_graph(driver) -> dict:
    """Discover all node labels dynamically and sample representative nodes from each."""
    with driver.session() as s:
        labels = [r["label"] for r in s.run(
            "CALL db.labels() YIELD label RETURN label"
        ).data()]

        entity_samples = {}
        for label in labels:
            nodes = s.run(
                f"MATCH (n:`{label}`) RETURN n LIMIT 3"
            ).data()
            entity_samples[label] = [
                {k: v for k, v in dict(row["n"]).items()
                 if v is not None and isinstance(v, (str, int, float, bool))
                 and k not in ("summary", "updated_at")}
                for row in nodes
            ]

        rel_types = s.run(
            "MATCH ()-[r]->() RETURN DISTINCT type(r) AS rel_type, count(r) AS cnt "
            "ORDER BY cnt DESC LIMIT 30"
        ).data()

        try:
            stats = s.run(
                "CALL apoc.meta.stats() YIELD labels RETURN labels"
            ).single()
            label_counts = stats["labels"] if stats else {}
        except Exception:
            label_counts = {label: len(entity_samples.get(label, [])) for label in labels}

    return {
        "entity_samples":     entity_samples,
        "relationship_types": rel_types,
        "label_counts":       label_counts,
    }


def _call_ollama(prompt: str) -> str:
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json().get("response", "")


def _extract_json_array(text: str) -> list:
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return []


def _save_proposals(proposals: list, dag_run_id: str, source_dag: str, conn) -> int:
    cur = conn.cursor()
    saved = 0
    for p in proposals:
        cypher = (p.get("cypher") or "").strip()
        if not cypher:
            continue
        try:
            cur.execute(
                """
                INSERT INTO cp_schema_proposals
                    (proposal_type, entity_type, relationship_type, proposed_cypher,
                     rationale, evidence, confidence, dag_run_id, source_dag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    p.get("type", "NEW_RELATIONSHIP"),
                    p.get("entity_type"),
                    p.get("relationship_type") or p.get("property_name"),
                    cypher,
                    p.get("rationale"),
                    json.dumps(p),
                    float(p.get("confidence", 0.5)),
                    dag_run_id,
                    source_dag,
                ),
            )
            saved += 1
        except Exception as e:
            log.warning("Skipping proposal (save error): %s", e)
            conn.rollback()
    conn.commit()
    cur.close()
    log.info("Saved %d / %d proposals (dag_run=%s)", saved, len(proposals), dag_run_id)
    return saved


def run_inference(dag_run_id: str = "manual") -> int:
    """Discover graph structure → prompt Ollama → persist proposals. Returns count saved."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    conn   = psycopg2.connect(DATABASE_URL)
    try:
        sample   = _sample_graph(driver)
        existing = [r["rel_type"] for r in sample.get("relationship_types", [])]
        existing_str = ", ".join(existing) if existing else "(none yet)"

        # Truncate entity sample to keep prompt under ~3000 chars
        entity_str = json.dumps(sample["entity_samples"], indent=2)[:3000]

        prompt = _PROMPT.format(
            entity_sample=entity_str,
            existing_relationships=existing_str,
        )
        log.info("Calling Ollama for relationship inference (model=%s, labels=%s)...",
                 OLLAMA_MODEL, list(sample["entity_samples"].keys()))
        raw       = _call_ollama(prompt)
        proposals = _extract_json_array(raw)
        log.info("LLM returned %d proposals", len(proposals))
        return _save_proposals(proposals, dag_run_id, "relationship_inference", conn)
    finally:
        driver.close()
        conn.close()

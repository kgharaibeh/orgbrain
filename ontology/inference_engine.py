"""
Relationship Inference Engine
Samples the Neo4j graph, prompts Ollama to propose new schema extensions,
parses the structured JSON response, and persists proposals to cp_schema_proposals.
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

_PROMPT = """You are a banking data ontology expert.
Below is a live sample of entities and relationships from a bank's Neo4j knowledge graph.

=== ENTITY SAMPLE ===
{entity_sample}

=== EXISTING RELATIONSHIP TYPES ===
{existing_relationships}

Your task: propose NEW relationship types or entity properties that would enrich this schema
based on the data patterns you observe.

Rules:
- Only propose relationships/properties that do NOT already exist above.
- Each proposed Cypher must be safe to run as a MERGE (idempotent).
- Confidence 0.0-1.0 based on how clearly the pattern appears in the data.

Respond ONLY with a valid JSON array. No prose outside the JSON.

[
  {{
    "type": "NEW_RELATIONSHIP",
    "entity_type": "Customer",
    "relationship_type": "FREQUENTLY_SHOPS_AT",
    "target_entity": "Merchant",
    "cypher": "MATCH (c:Customer)-[:HAS_TX]->(:Transaction)-[:AT]->(m:Merchant) WITH c, m, count(*) AS freq WHERE freq >= 5 MERGE (c)-[r:FREQUENTLY_SHOPS_AT]->(m) SET r.frequency = freq",
    "rationale": "Identifies merchant loyalty patterns for targeted offers",
    "confidence": 0.85
  }}
]

Proposals:"""


def _sample_graph(driver: GraphDatabase.driver) -> dict:
    with driver.session() as s:
        customers = s.run(
            "MATCH (c:Customer) RETURN c.customer_id AS id, c.income_band AS income, "
            "c.risk_rating AS risk, c.occupation AS occ LIMIT 5"
        ).data()

        transactions = s.run(
            "MATCH (t:Transaction) RETURN t.amount AS amount, t.channel AS channel, "
            "t.status AS status, t.merchant_mcc AS mcc LIMIT 10"
        ).data()

        rel_types = s.run(
            "MATCH ()-[r]->() RETURN DISTINCT type(r) AS rel_type, count(r) AS cnt "
            "ORDER BY cnt DESC LIMIT 20"
        ).data()

        try:
            stats = s.run("CALL apoc.meta.stats() YIELD labels RETURN labels").data()
        except Exception:
            stats = s.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt "
                "GROUP BY labels(n)[0] ORDER BY cnt DESC"
            ).data()

    return {
        "customer_sample":   customers,
        "transaction_sample": transactions,
        "relationship_types": rel_types,
        "graph_stats":        stats,
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
    """Sample graph → prompt Ollama → persist proposals. Returns count saved."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    conn   = psycopg2.connect(DATABASE_URL)
    try:
        sample       = _sample_graph(driver)
        existing     = [r["rel_type"] for r in sample.get("relationship_types", [])]
        existing_str = ", ".join(existing) or "HOLDS, HAS_TX, AT, HAS_CARD, HAS_LOAN"
        prompt = _PROMPT.format(
            entity_sample=json.dumps(sample, indent=2)[:3000],
            existing_relationships=existing_str,
        )
        log.info("Calling Ollama for relationship inference (model=%s)…", OLLAMA_MODEL)
        raw      = _call_ollama(prompt)
        proposals = _extract_json_array(raw)
        log.info("LLM returned %d proposals", len(proposals))
        return _save_proposals(proposals, dag_run_id, "relationship_inference", conn)
    finally:
        driver.close()
        conn.close()

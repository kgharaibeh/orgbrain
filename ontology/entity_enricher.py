"""
Entity Enricher — Generic / Domain-Agnostic
Discovers all node labels in Neo4j dynamically, generates a human-readable
LLM summary for each node, and writes it back as a `summary` property.

Works for any domain: banking, retail, manufacturing, insurance, SaaS, etc.
"""

import json
import logging
import os

import requests
from neo4j import GraphDatabase

log = logging.getLogger(__name__)

NEO4J_URI    = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER   = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASS   = os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",    "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL",   "llama3.1:8b")

_ENTITY_PROMPT = """You are a business intelligence assistant.
Given the following entity data, write a concise 1-2 sentence factual summary
that captures the entity's key attributes and any notable characteristics.
Do NOT invent information — work only with what is provided.

Entity type: {entity_type}
Entity data: {entity_json}

Summary:"""


def _call_ollama(prompt: str) -> str:
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json().get("response", "").strip()


def _enrich_entity_type(driver, label: str, limit: int = 100) -> int:
    """Generate summaries for all un-summarised nodes of a given label."""
    enriched = 0
    with driver.session() as s:
        nodes = s.run(
            f"MATCH (n:`{label}`) WHERE n.summary IS NULL "
            f"RETURN n LIMIT $limit",
            limit=limit,
        ).data()

    for row in nodes:
        node  = row["n"]
        props = {k: v for k, v in dict(node).items()
                 if v is not None
                 and k not in ("summary", "updated_at", "entity_type")
                 and isinstance(v, (str, int, float, bool))}
        eid   = props.get("entity_id") or next(
            (str(v) for k, v in props.items() if k.endswith("_id")), None
        )
        if not eid:
            continue
        try:
            prompt  = _ENTITY_PROMPT.format(
                entity_type=label,
                entity_json=json.dumps(props),
            )
            summary = _call_ollama(prompt)
            with driver.session() as s:
                s.run(
                    f"MATCH (n:`{label}` {{entity_id: $eid}}) SET n.summary = $summary",
                    eid=eid, summary=summary,
                )
            enriched += 1
            log.debug("Enriched %s %s", label, eid)
        except Exception as e:
            log.warning("Could not enrich %s %s: %s", label, eid, e)

    return enriched


def run_enrichment(limit: int = 100) -> dict:
    """Discover all Neo4j labels and run enrichment for each. Returns counts per label."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    results = {}
    try:
        with driver.session() as s:
            labels = [r["label"] for r in s.run(
                "CALL db.labels() YIELD label RETURN label"
            ).data()]

        for label in labels:
            count = _enrich_entity_type(driver, label, limit)
            results[label] = count
            log.info("Enriched %d %s nodes", count, label)

        log.info("Entity enrichment complete: %s", results)
        return results
    finally:
        driver.close()

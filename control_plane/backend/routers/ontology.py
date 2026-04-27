"""
Ontology Router — Proposals CRUD + Neo4j schema application + Airflow DAG management
"""

import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from neo4j import GraphDatabase
from pydantic import BaseModel

from ontology.schema_proposals import (
    approve_proposal, get_proposal, list_proposals,
    mark_applied, proposal_stats, reject_proposal,
)

log = logging.getLogger(__name__)
router = APIRouter()

AIRFLOW_URL   = os.getenv("AIRFLOW_URL",    "http://airflow-webserver:8080")
AIRFLOW_USER  = os.getenv("AIRFLOW_USER",   "admin")
AIRFLOW_PASS  = os.getenv("AIRFLOW_PASS",   "orgbrain_airflow")
NEO4J_URI     = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER    = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")

ONTOLOGY_DAGS = [
    "relationship_inference",
    "entity_enrichment",
    "customer_profile_embeddings",
    "churn_risk",
    "product_adoption",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _airflow_get(path: str) -> dict:
    with httpx.Client(base_url=AIRFLOW_URL, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=15) as c:
        r = c.get(f"/api/v1/{path}")
        r.raise_for_status()
        return r.json()


def _airflow_post(path: str, body: dict = {}) -> dict:
    with httpx.Client(base_url=AIRFLOW_URL, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=15) as c:
        r = c.post(f"/api/v1/{path}", json=body)
        r.raise_for_status()
        return r.json()


def _neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ── Proposal endpoints ────────────────────────────────────────────────────────

@router.get("/proposals")
def list_all_proposals(status: Optional[str] = None, limit: int = 100):
    return list_proposals(status=status, limit=limit)


@router.get("/proposals/stats")
def get_proposal_stats():
    return proposal_stats()


@router.get("/proposals/{proposal_id}")
def get_one_proposal(proposal_id: int):
    p = get_proposal(proposal_id)
    if not p:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return p


class ReviewBody(BaseModel):
    reviewed_by: str = "admin"


@router.post("/proposals/{proposal_id}/approve")
def approve_and_apply(proposal_id: int, body: ReviewBody = ReviewBody()):
    """Approve a proposal and immediately execute its Cypher against Neo4j."""
    p = get_proposal(proposal_id)
    if not p:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if p["status"] != "PENDING":
        raise HTTPException(status_code=400, detail=f"Proposal is already {p['status']}")

    approve_proposal(proposal_id, reviewed_by=body.reviewed_by)

    # Execute the Cypher
    driver = _neo4j_driver()
    try:
        with driver.session() as s:
            s.run(p["proposed_cypher"])
        mark_applied(proposal_id)
        log.info("Applied proposal %d: %s", proposal_id, p.get("relationship_type"))
        return {"status": "APPLIED", "proposal_id": proposal_id}
    except Exception as e:
        mark_applied(proposal_id, error=str(e))
        log.error("Failed to apply proposal %d: %s", proposal_id, e)
        raise HTTPException(status_code=500, detail=f"Cypher execution failed: {e}")
    finally:
        driver.close()


@router.post("/proposals/{proposal_id}/reject")
def reject_one(proposal_id: int, body: ReviewBody = ReviewBody()):
    if not reject_proposal(proposal_id, reviewed_by=body.reviewed_by):
        raise HTTPException(status_code=404, detail="Proposal not found or already reviewed")
    return {"status": "REJECTED", "proposal_id": proposal_id}


# ── Airflow DAG endpoints ─────────────────────────────────────────────────────

@router.get("/dags")
def list_dags():
    """Return status for all ontology-related DAGs."""
    results = []
    for dag_id in ONTOLOGY_DAGS:
        try:
            dag_info = _airflow_get(f"dags/{dag_id}")
            runs     = _airflow_get(f"dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date")
            last_run = runs.get("dag_runs", [{}])[0]
            results.append({
                "dag_id":       dag_id,
                "is_paused":    dag_info.get("is_paused", False),
                "last_run_state": last_run.get("state"),
                "last_run_date":  last_run.get("execution_date"),
                "schedule":     dag_info.get("schedule_interval"),
                "description":  dag_info.get("description", ""),
            })
        except Exception as e:
            results.append({
                "dag_id":         dag_id,
                "is_paused":      None,
                "last_run_state": "unknown",
                "last_run_date":  None,
                "error":          str(e),
            })
    return results


@router.post("/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str):
    if dag_id not in ONTOLOGY_DAGS:
        raise HTTPException(status_code=400, detail=f"Unknown DAG: {dag_id}")
    try:
        result = _airflow_post(f"dags/{dag_id}/dagRuns", {"conf": {}})
        return {"dag_id": dag_id, "run_id": result.get("dag_run_id"), "state": result.get("state")}
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


@router.post("/dags/{dag_id}/pause")
def pause_dag(dag_id: str):
    if dag_id not in ONTOLOGY_DAGS:
        raise HTTPException(status_code=400, detail=f"Unknown DAG: {dag_id}")
    try:
        _airflow_post(f"dags/{dag_id}", {"is_paused": True})
        return {"dag_id": dag_id, "paused": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Schema stats ──────────────────────────────────────────────────────────────

@router.get("/schema/stats")
def schema_stats():
    """Return entity type counts and relationship type counts from Neo4j."""
    driver = _neo4j_driver()
    try:
        with driver.session() as s:
            try:
                meta = s.run("CALL apoc.meta.stats() YIELD labels, relTypesCount RETURN labels, relTypesCount").data()
                if meta:
                    return {"labels": meta[0]["labels"], "relationship_types": meta[0]["relTypesCount"]}
            except Exception:
                pass

            # Fallback without APOC
            labels = s.run(
                "MATCH (n) WITH labels(n)[0] AS lbl, count(n) AS cnt "
                "WHERE lbl IS NOT NULL RETURN lbl, cnt ORDER BY cnt DESC"
            ).data()
            rels = s.run(
                "MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS cnt ORDER BY cnt DESC"
            ).data()
            return {
                "labels":             {r["lbl"]: r["cnt"] for r in labels},
                "relationship_types": {r["rel_type"]: r["cnt"] for r in rels},
            }
    finally:
        driver.close()

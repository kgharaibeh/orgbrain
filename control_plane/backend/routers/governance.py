"""
Governance Router — PII rules management (DB-backed, no YAML files).
Full CRUD for governance sources and field-level anonymization rules.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import GovernanceSource, GovernanceRule, AnonymizationAudit

log = logging.getLogger(__name__)
router = APIRouter()

VALID_METHODS = {
    "fpe_numeric", "fpe_date", "fpe_email",
    "hmac_sha256", "generalize", "suppress", "nlp_scrub", "keep",
}


# ─── Pydantic schemas ─────────────────────────────────────────────────────────

class RuleCreate(BaseModel):
    field_name: str
    field_pattern: Optional[str] = None
    method: str
    vault_key: Optional[str] = None
    is_joinable: bool = False
    is_reversible: bool = False
    is_nullable: bool = False
    generalize_map: Optional[dict] = None
    presidio_entities: Optional[list[str]] = None
    notes: Optional[str] = None
    sort_order: int = 0

class RuleUpdate(BaseModel):
    method: Optional[str] = None
    vault_key: Optional[str] = None
    is_joinable: Optional[bool] = None
    is_reversible: Optional[bool] = None
    is_nullable: Optional[bool] = None
    is_active: Optional[bool] = None
    generalize_map: Optional[dict] = None
    presidio_entities: Optional[list[str]] = None
    notes: Optional[str] = None
    sort_order: Optional[int] = None

class SourceCreate(BaseModel):
    source_name: str
    raw_topic_prefix: str
    clean_topic_prefix: str
    entity_type: Optional[str] = None
    description: Optional[str] = None


# ─── Governance Sources ───────────────────────────────────────────────────────

@router.get("/sources")
def list_sources(db: Session = Depends(get_db)):
    sources = db.query(GovernanceSource).all()
    return [
        {
            "id": s.id, "source_name": s.source_name,
            "raw_topic_prefix": s.raw_topic_prefix,
            "clean_topic_prefix": s.clean_topic_prefix,
            "entity_type": s.entity_type, "description": s.description,
            "is_active": s.is_active,
            "rule_count": len(s.rules),
            "active_rule_count": sum(1 for r in s.rules if r.is_active),
        }
        for s in sources
    ]


@router.post("/sources", status_code=201)
def create_source(payload: SourceCreate, db: Session = Depends(get_db)):
    existing = db.query(GovernanceSource).filter_by(source_name=payload.source_name).first()
    if existing:
        raise HTTPException(400, f"Source '{payload.source_name}' already exists")
    source = GovernanceSource(**payload.model_dump())
    db.add(source)
    db.commit()
    db.refresh(source)
    return {"id": source.id, "source_name": source.source_name}


@router.delete("/sources/{source_id}")
def delete_source(source_id: int, db: Session = Depends(get_db)):
    s = db.query(GovernanceSource).get(source_id)
    if not s:
        raise HTTPException(404, "Source not found")
    db.delete(s)
    db.commit()
    return {"message": "Source and its rules deleted"}


# ─── Governance Rules ─────────────────────────────────────────────────────────

@router.get("/sources/{source_id}/rules")
def list_rules(source_id: int, db: Session = Depends(get_db)):
    source = db.query(GovernanceSource).get(source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    rules = db.query(GovernanceRule).filter_by(source_id=source_id).order_by(GovernanceRule.sort_order).all()
    return [
        {
            "id": r.id, "field_name": r.field_name, "field_pattern": r.field_pattern,
            "method": r.method, "vault_key": r.vault_key,
            "is_joinable": r.is_joinable, "is_reversible": r.is_reversible,
            "is_nullable": r.is_nullable, "is_active": r.is_active,
            "generalize_map": r.generalize_map,
            "presidio_entities": r.presidio_entities,
            "notes": r.notes, "sort_order": r.sort_order,
        }
        for r in rules
    ]


@router.post("/sources/{source_id}/rules", status_code=201)
def create_rule(source_id: int, payload: RuleCreate, db: Session = Depends(get_db)):
    source = db.query(GovernanceSource).get(source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    if payload.method not in VALID_METHODS:
        raise HTTPException(400, f"Invalid method '{payload.method}'. Valid: {sorted(VALID_METHODS)}")
    existing = db.query(GovernanceRule).filter_by(source_id=source_id, field_name=payload.field_name).first()
    if existing:
        raise HTTPException(400, f"Rule for field '{payload.field_name}' already exists on this source")
    rule = GovernanceRule(source_id=source_id, **payload.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return {"id": rule.id, "field_name": rule.field_name, "method": rule.method}


@router.put("/rules/{rule_id}")
def update_rule(rule_id: int, payload: RuleUpdate, db: Session = Depends(get_db)):
    rule = db.query(GovernanceRule).get(rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    if payload.method and payload.method not in VALID_METHODS:
        raise HTTPException(400, f"Invalid method '{payload.method}'")
    for field, val in payload.model_dump(exclude_none=True).items():
        setattr(rule, field, val)
    db.commit()
    return {"message": "Rule updated", "id": rule.id}


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(GovernanceRule).get(rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    db.delete(rule)
    db.commit()
    return {"message": "Rule deleted"}


@router.get("/rules/export/{source_id}")
def export_rules_as_config(source_id: int, db: Session = Depends(get_db)):
    """
    Export governance rules for a source as a structured config (used by Flink anonymizer).
    This endpoint replaces the old YAML files.
    """
    source = db.query(GovernanceSource).get(source_id)
    if not source:
        raise HTTPException(404, "Source not found")
    rules = db.query(GovernanceRule).filter_by(source_id=source_id, is_active=True).order_by(GovernanceRule.sort_order).all()
    return {
        "source_name":         source.source_name,
        "raw_topic_prefix":    source.raw_topic_prefix,
        "clean_topic_prefix":  source.clean_topic_prefix,
        "entity_type":         source.entity_type,
        "rules": [
            {
                "field": r.field_name,
                "field_pattern": r.field_pattern,
                "method": r.method,
                "vault_key": r.vault_key,
                "joinable": r.is_joinable,
                "reversible": r.is_reversible,
                "nullable": r.is_nullable,
                "mapping": r.generalize_map,
                "presidio_entities": r.presidio_entities,
            }
            for r in rules
        ],
    }


@router.get("/rules/export-all")
def export_all_rules(db: Session = Depends(get_db)):
    """Export all active governance rules — called by Flink anonymizer at startup."""
    sources = db.query(GovernanceSource).filter_by(is_active=True).all()
    result = []
    for source in sources:
        rules = db.query(GovernanceRule).filter_by(source_id=source.id, is_active=True).order_by(GovernanceRule.sort_order).all()
        result.append({
            "source_name":        source.source_name,
            "raw_topic_prefix":   source.raw_topic_prefix,
            "clean_topic_prefix": source.clean_topic_prefix,
            "entity_type":        source.entity_type,
            "rules": [
                {
                    "field": r.field_name,
                    "field_pattern": r.field_pattern,
                    "method": r.method,
                    "vault_key": r.vault_key,
                    "joinable": r.is_joinable,
                    "reversible": r.is_reversible,
                    "nullable": r.is_nullable,
                    "mapping": r.generalize_map,
                    "presidio_entities": r.presidio_entities,
                }
                for r in rules
            ],
        })
    return result


# ─── Audit Log ────────────────────────────────────────────────────────────────

@router.get("/audit")
def get_audit_log(
    limit: int = 100,
    source_topic: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(AnonymizationAudit).order_by(AnonymizationAudit.event_time.desc())
    if source_topic:
        q = q.filter(AnonymizationAudit.source_topic == source_topic)
    rows = q.limit(limit).all()
    return [
        {
            "id": r.id, "event_time": r.event_time, "source_topic": r.source_topic,
            "clean_topic": r.clean_topic, "entity_type": r.entity_type,
            "entity_id": r.entity_id, "fields_count": r.fields_count,
            "op": r.op, "processed_by": r.processed_by,
        }
        for r in rows
    ]


@router.get("/audit/summary")
def audit_summary(db: Session = Depends(get_db)):
    from sqlalchemy import func, text
    rows = db.execute(text("""
        SELECT source_topic, entity_type, COUNT(*) as total, MAX(event_time) as last_event
        FROM cp_anonymization_audit
        GROUP BY source_topic, entity_type
        ORDER BY last_event DESC
    """)).fetchall()
    return [dict(r._mapping) for r in rows]

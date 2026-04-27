"""
Schema Proposals CRUD
Thin data-access layer for cp_schema_proposals in PostgreSQL.
Used by both the FastAPI router and Airflow DAGs.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orgbrain:orgbrain_secret@postgres:5432/core_banking")


def _conn():
    return psycopg2.connect(DATABASE_URL)


def list_proposals(status: Optional[str] = None, limit: int = 100) -> list[dict]:
    conn = _conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if status:
        cur.execute(
            "SELECT * FROM cp_schema_proposals WHERE status = %s ORDER BY created_at DESC LIMIT %s",
            (status, limit),
        )
    else:
        cur.execute(
            "SELECT * FROM cp_schema_proposals ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def get_proposal(proposal_id: int) -> Optional[dict]:
    conn = _conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM cp_schema_proposals WHERE id = %s", (proposal_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def approve_proposal(proposal_id: int, reviewed_by: str = "admin") -> bool:
    conn = _conn()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE cp_schema_proposals SET status = 'APPROVED', reviewed_by = %s, reviewed_at = %s WHERE id = %s AND status = 'PENDING'",
        (reviewed_by, datetime.now(timezone.utc), proposal_id),
    )
    updated = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return updated


def reject_proposal(proposal_id: int, reviewed_by: str = "admin") -> bool:
    conn = _conn()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE cp_schema_proposals SET status = 'REJECTED', reviewed_by = %s, reviewed_at = %s WHERE id = %s AND status = 'PENDING'",
        (reviewed_by, datetime.now(timezone.utc), proposal_id),
    )
    updated = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return updated


def mark_applied(proposal_id: int, error: Optional[str] = None) -> None:
    conn = _conn()
    cur  = conn.cursor()
    status = "APPLIED" if not error else "FAILED"
    cur.execute(
        "UPDATE cp_schema_proposals SET status = %s, applied_at = %s, apply_error = %s WHERE id = %s",
        (status, datetime.now(timezone.utc), error, proposal_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def proposal_stats() -> dict:
    conn = _conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT status, count(*) AS cnt FROM cp_schema_proposals GROUP BY status"
    )
    rows   = {r["status"]: r["cnt"] for r in cur.fetchall()}
    cur.close()
    conn.close()
    return {
        "pending":  rows.get("PENDING", 0),
        "approved": rows.get("APPROVED", 0),
        "rejected": rows.get("REJECTED", 0),
        "applied":  rows.get("APPLIED", 0),
        "failed":   rows.get("FAILED", 0),
    }

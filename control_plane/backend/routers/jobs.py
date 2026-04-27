"""
Jobs Router — Flink job submission and lifecycle management via Flink REST API.
Also manages brain ingest pipeline and risk scorer as regular processes.
"""

import logging
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import FlinkJob
from datetime import datetime, timezone

log = logging.getLogger(__name__)
router = APIRouter()

FLINK_URL      = os.getenv("FLINK_URL", "http://flink-jobmanager:8081")
CONNECT_URL    = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
CP_BACKEND_URL = "http://cp-backend:8088"


def _flink(method: str, path: str, **kwargs):
    try:
        resp = httpx.request(method, f"{FLINK_URL}{path}", timeout=30, **kwargs)
        if resp.status_code >= 400:
            raise HTTPException(resp.status_code, resp.text)
        return resp.json() if resp.content else {}
    except httpx.RequestError as e:
        raise HTTPException(503, f"Flink unavailable: {e}")


class JobConfig(BaseModel):
    parallelism: int = 2
    checkpoint_interval_ms: int = 30000


@router.get("")
def list_jobs(db: Session = Depends(get_db)):
    """List all registered jobs with their live Flink status."""
    jobs = db.query(FlinkJob).all()
    # Also fetch live Flink overview
    try:
        flink_jobs = {j["jid"]: j for j in _flink("GET", "/jobs/overview").get("jobs", [])}
    except Exception:
        flink_jobs = {}

    result = []
    for job in jobs:
        live = flink_jobs.get(job.flink_job_id, {})
        result.append({
            "id": job.id,
            "job_name": job.job_name,
            "job_type": job.job_type,
            "flink_job_id": job.flink_job_id,
            "status": live.get("state", job.status),
            "script_path": job.script_path,
            "config_json": job.config_json,
            "last_submitted": job.last_submitted,
            "start_time": live.get("start-time"),
            "duration": live.get("duration"),
            "tasks": {
                "running": live.get("tasks", {}).get("RUNNING", 0),
                "total":   live.get("tasks", {}).get("TOTAL", 0),
            },
        })
    return result


@router.post("/{job_id}/submit")
def submit_job(job_id: int, config: JobConfig = JobConfig(), db: Session = Depends(get_db)):
    """
    Submit a Flink job to the cluster.
    The anonymizer job fetches rules from the Control Plane API — no YAML files.
    """
    job = db.query(FlinkJob).get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    # Upload the Python script as a JAR-less job via Flink Python API
    script_path = job.script_path
    if not os.path.exists(script_path):
        raise HTTPException(404, f"Script not found: {script_path}")

    try:
        # Upload script
        with open(script_path, "rb") as f:
            upload_resp = httpx.post(
                f"{FLINK_URL}/jars/upload",
                files={"jarfile": (os.path.basename(script_path), f, "application/octet-stream")},
                timeout=60,
            )
        if upload_resp.status_code >= 400:
            raise HTTPException(upload_resp.status_code, upload_resp.text)

        jar_id = upload_resp.json().get("filename", "").split("/")[-1]

        # Build job run args
        env_args = [
            f"--rules-api-url {CP_BACKEND_URL}/api/governance/rules/export-all",
            f"--kafka-brokers {os.getenv('KAFKA_BROKERS', 'kafka:9092')}",
            f"--vault-addr {os.getenv('VAULT_ADDR', 'http://vault:8200')}",
            f"--vault-token {os.getenv('VAULT_TOKEN', 'orgbrain-vault-root')}",
            f"--parallelism {config.parallelism}",
        ]

        run_resp = httpx.post(
            f"{FLINK_URL}/jars/{jar_id}/run",
            json={
                "programArgsList": " ".join(env_args).split(),
                "parallelism": config.parallelism,
                "allowNonRestoredState": True,
            },
            timeout=30,
        )
        if run_resp.status_code >= 400:
            raise HTTPException(run_resp.status_code, run_resp.text)

        flink_job_id = run_resp.json().get("jobid")
        job.flink_job_id = flink_job_id
        job.status = "RUNNING"
        job.last_submitted = datetime.now(timezone.utc)
        db.commit()
        return {"message": "Job submitted", "flink_job_id": flink_job_id}

    except HTTPException:
        raise
    except Exception as e:
        job.status = "ERROR"
        db.commit()
        raise HTTPException(500, f"Job submission failed: {e}")


@router.post("/{job_id}/cancel")
def cancel_job(job_id: int, db: Session = Depends(get_db)):
    """Cancel a running Flink job."""
    job = db.query(FlinkJob).get(job_id)
    if not job or not job.flink_job_id:
        raise HTTPException(404, "Job not found or not running")
    _flink("PATCH", f"/jobs/{job.flink_job_id}", json={"status": "CANCELED"})
    job.status = "STOPPED"
    job.flink_job_id = None
    db.commit()
    return {"message": "Job cancelled"}


@router.get("/{job_id}/metrics")
def job_metrics(job_id: int, db: Session = Depends(get_db)):
    """Get Flink job metrics (throughput, latency, checkpoints)."""
    job = db.query(FlinkJob).get(job_id)
    if not job or not job.flink_job_id:
        raise HTTPException(404, "Job not running")
    try:
        details = _flink("GET", f"/jobs/{job.flink_job_id}")
        checkpoints = _flink("GET", f"/jobs/{job.flink_job_id}/checkpoints")
        return {
            "job_id": job.flink_job_id,
            "status": details.get("state"),
            "vertices": [
                {"name": v["name"], "status": v["status"], "parallelism": v["parallelism"]}
                for v in details.get("vertices", [])
            ],
            "checkpoints": checkpoints.get("latest", {}),
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/flink/cluster")
def flink_cluster_overview():
    """Get Flink cluster overview (task managers, slots, running jobs)."""
    try:
        overview = _flink("GET", "/overview")
        tm = _flink("GET", "/taskmanagers")
        return {
            "jobs_running": overview.get("jobs-running", 0),
            "jobs_finished": overview.get("jobs-finished", 0),
            "jobs_cancelled": overview.get("jobs-cancelled", 0),
            "jobs_failed": overview.get("jobs-failed", 0),
            "slots_total": overview.get("slots-total", 0),
            "slots_available": overview.get("slots-available", 0),
            "task_managers": len(tm.get("taskmanagers", [])),
            "flink_version": overview.get("flink-version"),
        }
    except Exception as e:
        return {"error": str(e)}

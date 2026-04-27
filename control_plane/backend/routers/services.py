"""
Services Router — Docker container lifecycle management.
Start, stop, restart, get logs for any OrgBrain service.
"""

import logging
from typing import Optional
import docker
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

log = logging.getLogger(__name__)
router = APIRouter()

MANAGED_SERVICES = {
    "kafka":             "orgbrain-kafka",
    "schema-registry":   "orgbrain-schema-registry",
    "kafka-connect":     "orgbrain-kafka-connect",
    "kafka-ui":          "orgbrain-kafka-ui",
    "postgres":          "orgbrain-postgres",
    "minio":             "orgbrain-minio",
    "vault":             "orgbrain-vault",
    "neo4j":             "orgbrain-neo4j",
    "qdrant":            "orgbrain-qdrant",
    "timescale":         "orgbrain-timescale",
    "ollama":            "orgbrain-ollama",
    "flink-jobmanager":  "orgbrain-flink-jobmanager",
    "flink-taskmanager": "orgbrain-flink-taskmanager",
    "grafana":           "orgbrain-grafana",
    "portainer":         "orgbrain-portainer",
    "cp-backend":        "orgbrain-cp-backend",
    "cp-frontend":       "orgbrain-cp-frontend",
}

SERVICE_DISPLAY = {
    "kafka":            {"label": "Apache Kafka",          "group": "streaming",    "ui": None},
    "schema-registry":  {"label": "Apicurio Schema Registry", "group": "streaming", "ui": "http://localhost:8081"},
    "kafka-connect":    {"label": "Kafka Connect",         "group": "streaming",    "ui": "http://localhost:8083"},
    "kafka-ui":         {"label": "Kafka UI",              "group": "streaming",    "ui": "http://localhost:8080"},
    "postgres":         {"label": "PostgreSQL (Core Banking)", "group": "storage",  "ui": None},
    "minio":            {"label": "MinIO (Data Lake)",     "group": "storage",      "ui": "http://localhost:9001"},
    "vault":            {"label": "HashiCorp Vault",       "group": "security",     "ui": "http://localhost:8200"},
    "neo4j":            {"label": "Neo4j Graph Store",     "group": "brain",        "ui": "http://localhost:7474"},
    "qdrant":           {"label": "Qdrant Vector Store",   "group": "brain",        "ui": "http://localhost:6333/dashboard"},
    "timescale":        {"label": "TimescaleDB",           "group": "brain",        "ui": None},
    "ollama":           {"label": "Ollama LLM",            "group": "ai",           "ui": None},
    "flink-jobmanager": {"label": "Flink Job Manager",     "group": "processing",   "ui": "http://localhost:8082"},
    "flink-taskmanager":{"label": "Flink Task Manager",    "group": "processing",   "ui": None},
    "grafana":          {"label": "Grafana",               "group": "observability","ui": "http://localhost:3000"},
    "portainer":        {"label": "Portainer",             "group": "operations",   "ui": "http://localhost:9900"},
    "cp-backend":       {"label": "Control Plane API",     "group": "operations",   "ui": "http://localhost:8088/api/docs"},
    "cp-frontend":      {"label": "Control Plane UI",      "group": "operations",   "ui": "http://localhost:3001"},
}


def _get_docker():
    try:
        return docker.from_env()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Docker socket unavailable: {e}")


def _get_container(service_key: str):
    client = _get_docker()
    container_name = MANAGED_SERVICES.get(service_key)
    if not container_name:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service_key}")
    try:
        return client.containers.get(container_name)
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail=f"Container '{container_name}' not found")


@router.get("")
def list_services():
    """List all managed services with their current status and resource usage."""
    client = _get_docker()
    result = []
    for key, container_name in MANAGED_SERVICES.items():
        info = SERVICE_DISPLAY.get(key, {})
        try:
            c = client.containers.get(container_name)
            c.reload()
            status = c.status          # running, exited, paused, etc.
            health = "unknown"
            if c.attrs.get("State", {}).get("Health"):
                health = c.attrs["State"]["Health"].get("Status", "unknown")
            # Lightweight stats (no streaming)
            stats = c.stats(stream=False) if status == "running" else {}
            cpu_pct = 0.0
            mem_mb  = 0.0
            if stats:
                cpu_delta   = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
                sys_delta   = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"].get("system_cpu_usage", 0)
                num_cpus    = stats["cpu_stats"].get("online_cpus", 1)
                cpu_pct     = (cpu_delta / sys_delta * num_cpus * 100.0) if sys_delta > 0 else 0
                mem_mb      = stats["memory_stats"].get("usage", 0) / (1024 * 1024)
        except docker.errors.NotFound:
            status = "not_found"
            health = "not_found"
            cpu_pct = 0.0
            mem_mb  = 0.0

        result.append({
            "key":         key,
            "label":       info.get("label", key),
            "group":       info.get("group", "other"),
            "container":   container_name,
            "status":      status,
            "health":      health,
            "ui_url":      info.get("ui"),
            "cpu_percent": round(cpu_pct, 1),
            "mem_mb":      round(mem_mb, 1),
        })
    return result


@router.post("/{service_key}/start")
def start_service(service_key: str):
    """Start a stopped service container."""
    c = _get_container(service_key)
    if c.status == "running":
        return {"message": f"{service_key} is already running", "status": "running"}
    c.start()
    c.reload()
    return {"message": f"{service_key} started", "status": c.status}


@router.post("/{service_key}/stop")
def stop_service(service_key: str):
    """Gracefully stop a running service container."""
    c = _get_container(service_key)
    if c.status != "running":
        return {"message": f"{service_key} is not running", "status": c.status}
    c.stop(timeout=30)
    c.reload()
    return {"message": f"{service_key} stopped", "status": c.status}


@router.post("/{service_key}/restart")
def restart_service(service_key: str):
    """Restart a service container."""
    c = _get_container(service_key)
    c.restart(timeout=30)
    c.reload()
    return {"message": f"{service_key} restarted", "status": c.status}


@router.get("/{service_key}/logs")
def get_logs(
    service_key: str,
    tail: int = Query(default=200, ge=1, le=5000),
    since: Optional[str] = Query(default=None, description="ISO timestamp or relative like '1h'"),
):
    """Retrieve recent logs from a service container."""
    c = _get_container(service_key)
    logs = c.logs(tail=tail, timestamps=True).decode("utf-8", errors="replace")
    return {"service": service_key, "logs": logs}


@router.get("/{service_key}/status")
def service_status(service_key: str):
    """Get detailed status of a single service."""
    c = _get_container(service_key)
    c.reload()
    state = c.attrs.get("State", {})
    return {
        "key":       service_key,
        "container": c.name,
        "status":    c.status,
        "started_at":state.get("StartedAt"),
        "finished_at":state.get("FinishedAt"),
        "exit_code": state.get("ExitCode"),
        "health":    state.get("Health", {}).get("Status", "none"),
        "image":     c.image.tags[0] if c.image.tags else str(c.image.id)[:12],
    }

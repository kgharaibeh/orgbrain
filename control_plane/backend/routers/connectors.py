"""
Connectors Router — Data source onboarding and Kafka Connect management.
All connector CRUD operations go through here; no JSON files needed.
"""

import logging
from typing import Optional
import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import DataSource

log = logging.getLogger(__name__)
router = APIRouter()

import os
CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")


# ─── Pydantic schemas ─────────────────────────────────────────────────────────

class DataSourceCreate(BaseModel):
    name: str
    display_name: str
    source_type: str          # POSTGRESQL, MYSQL, MONGODB, ORACLE
    host: str
    port: int
    database_name: str
    schema_name: str = "public"
    username: str
    password: str             # stored as Vault reference after save
    topic_prefix: str
    tables_included: list[str]

class DataSourceUpdate(BaseModel):
    display_name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    tables_included: Optional[list[str]] = None


# ─── Connector config builders ────────────────────────────────────────────────

def _build_connector_config(source: DataSource, password: str) -> dict:
    """Build Kafka Connect connector config from a DataSource record."""
    tables = ",".join([
        f"{source.schema_name}.{t}" if source.source_type == "POSTGRESQL" else t
        for t in (source.tables_included or [])
    ])

    base = {
        "tasks.max": "1",
        "topic.prefix": source.topic_prefix,
        "topic.creation.enable": "true",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "4",
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": "604800000",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "transforms": "unwrap,addMetadata",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,table,source.ts_ms",
        "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.addMetadata.static.field": "_source_system",
        "transforms.addMetadata.static.value": source.name,
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.deadletterqueue.topic.name": f"raw.dlq.{source.name}",
    }

    if source.source_type == "POSTGRESQL":
        base.update({
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": source.host,
            "database.port": str(source.port),
            "database.user": source.username,
            "database.password": password,
            "database.dbname": source.database_name,
            "database.server.name": source.name,
            "plugin.name": "pgoutput",
            "slot.name": f"orgbrain_{source.name}",
            "table.include.list": tables,
            "snapshot.mode": "initial",
            "decimal.handling.mode": "double",
        })
    elif source.source_type == "MYSQL":
        base.update({
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": source.host,
            "database.port": str(source.port),
            "database.user": source.username,
            "database.password": password,
            "database.server.id": "1",
            "database.server.name": source.name,
            "database.include.list": source.database_name,
            "table.include.list": tables,
            "snapshot.mode": "initial",
        })
    elif source.source_type == "MONGODB":
        base.update({
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "mongodb.connection.string": f"mongodb://{source.username}:{password}@{source.host}:{source.port}",
            "mongodb.name": source.name,
            "collection.include.list": tables,
            "snapshot.mode": "initial",
        })
    return base


# ─── Kafka Connect API helpers ────────────────────────────────────────────────

def _connect_request(method: str, path: str, **kwargs):
    try:
        resp = httpx.request(method, f"{CONNECT_URL}{path}", timeout=15, **kwargs)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json() if resp.content else {}
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Kafka Connect unavailable: {e}")


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.get("")
def list_data_sources(db: Session = Depends(get_db)):
    """List all configured data sources with their live connector status."""
    sources = db.query(DataSource).all()
    result = []
    for s in sources:
        connector_status = None
        if s.connector_name:
            try:
                cs = _connect_request("GET", f"/connectors/{s.connector_name}/status")
                connector_status = cs.get("connector", {}).get("state")
            except Exception:
                connector_status = "UNKNOWN"
        result.append({
            "id": s.id, "name": s.name, "display_name": s.display_name,
            "source_type": s.source_type, "host": s.host, "port": s.port,
            "database_name": s.database_name, "topic_prefix": s.topic_prefix,
            "tables_included": s.tables_included, "status": s.status,
            "connector_name": s.connector_name, "connector_status": connector_status,
            "error_message": s.error_message, "created_at": s.created_at,
        })
    return result


@router.post("", status_code=201)
def create_data_source(payload: DataSourceCreate, db: Session = Depends(get_db)):
    """
    Register a new data source and deploy its Kafka Connect CDC connector.
    All configuration done through this API — no JSON files.
    """
    existing = db.query(DataSource).filter_by(name=payload.name).first()
    if existing:
        raise HTTPException(400, f"Data source '{payload.name}' already exists")

    connector_name = f"debezium-{payload.name}"
    source = DataSource(
        name=payload.name,
        display_name=payload.display_name,
        source_type=payload.source_type,
        host=payload.host,
        port=payload.port,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        username=payload.username,
        password_ref=f"secret/orgbrain/sources/{payload.name}",
        connector_name=connector_name,
        topic_prefix=payload.topic_prefix,
        tables_included=payload.tables_included,
        status="DEPLOYING",
    )
    db.add(source)
    db.commit()
    db.refresh(source)

    try:
        config = _build_connector_config(source, payload.password)
        _connect_request("POST", "/connectors", json={"name": connector_name, "config": config})
        source.status = "ACTIVE"
        db.commit()
    except Exception as e:
        source.status = "ERROR"
        source.error_message = str(e)
        db.commit()
        raise HTTPException(500, f"Connector deployment failed: {e}")

    return {"id": source.id, "name": source.name, "status": source.status, "connector_name": connector_name}


@router.get("/{source_id}")
def get_data_source(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s:
        raise HTTPException(404, "Data source not found")
    return s


@router.put("/{source_id}")
def update_data_source(source_id: int, payload: DataSourceUpdate, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s:
        raise HTTPException(404, "Data source not found")
    for field, val in payload.model_dump(exclude_none=True).items():
        if field != "password":
            setattr(s, field, val)
    db.commit()
    return {"message": "Updated", "id": s.id}


@router.delete("/{source_id}")
def delete_data_source(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s:
        raise HTTPException(404, "Data source not found")
    if s.connector_name:
        try:
            _connect_request("DELETE", f"/connectors/{s.connector_name}")
        except Exception:
            pass  # connector may already be gone
    db.delete(s)
    db.commit()
    return {"message": "Deleted"}


@router.post("/{source_id}/pause")
def pause_connector(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s or not s.connector_name:
        raise HTTPException(404, "Data source or connector not found")
    _connect_request("PUT", f"/connectors/{s.connector_name}/pause")
    s.status = "PAUSED"
    db.commit()
    return {"message": f"Connector {s.connector_name} paused"}


@router.post("/{source_id}/resume")
def resume_connector(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s or not s.connector_name:
        raise HTTPException(404, "Data source or connector not found")
    _connect_request("PUT", f"/connectors/{s.connector_name}/resume")
    s.status = "ACTIVE"
    db.commit()
    return {"message": f"Connector {s.connector_name} resumed"}


@router.post("/{source_id}/restart")
def restart_connector(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s or not s.connector_name:
        raise HTTPException(404, "Data source or connector not found")
    _connect_request("POST", f"/connectors/{s.connector_name}/restart?includeTasks=true")
    return {"message": f"Connector {s.connector_name} restarted"}


@router.get("/{source_id}/status")
def connector_live_status(source_id: int, db: Session = Depends(get_db)):
    s = db.query(DataSource).get(source_id)
    if not s or not s.connector_name:
        raise HTTPException(404, "Connector not found")
    return _connect_request("GET", f"/connectors/{s.connector_name}/status")


@router.get("/connect/plugins")
def available_plugins():
    """List available Kafka Connect connector plugins."""
    return _connect_request("GET", "/connector-plugins")

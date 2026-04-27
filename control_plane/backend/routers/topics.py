"""
Topics Router — Kafka topic management via Admin API.
Create, delete, update topics and inspect consumer groups.
"""

import logging
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db
from models import KafkaTopic

log = logging.getLogger(__name__)
router = APIRouter()

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")


def _admin():
    return KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS, client_id="orgbrain-cp")


class TopicCreate(BaseModel):
    topic_name: str
    topic_type: str = "CUSTOM"
    partitions: int = 4
    replication_factor: int = 1
    retention_ms: int = 604800000   # 7 days; -1 = infinite
    compression: str = "lz4"
    description: Optional[str] = None


class TopicUpdate(BaseModel):
    retention_ms: Optional[int] = None
    description: Optional[str] = None


@router.get("")
def list_topics(db: Session = Depends(get_db)):
    """List all Kafka topics with their configuration and DB registration."""
    admin = _admin()
    try:
        live_topics = admin.list_topics()
        topic_configs: dict[str, dict] = {}
        for t in live_topics:
            try:
                cr = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, t)])
                if cr:
                    topic_configs[t] = {e.name: e.value for e in cr[0].resources[0].config_entries}
            except Exception:
                topic_configs[t] = {}
    finally:
        admin.close()

    db_topics = {t.topic_name: t for t in db.query(KafkaTopic).all()}
    result = []
    for name in sorted(live_topics):
        if name.startswith("_"):
            continue
        cfg = topic_configs.get(name, {})
        db_rec = db_topics.get(name)
        result.append({
            "topic_name": name,
            "topic_type": db_rec.topic_type if db_rec else "EXTERNAL",
            "partitions":  cfg.get("num.partitions", "?"),
            "retention_ms": int(cfg.get("retention.ms", -1)),
            "compression":  cfg.get("compression.type", "?"),
            "description":  db_rec.description if db_rec else None,
            "is_provisioned": True,
            "registered": db_rec is not None,
        })
    return result


@router.post("", status_code=201)
def create_topic(payload: TopicCreate, db: Session = Depends(get_db)):
    """Create a new Kafka topic and register it in the control plane."""
    admin = _admin()
    try:
        configs = {
            "retention.ms": str(payload.retention_ms),
            "compression.type": payload.compression,
        }
        admin.create_topics([
            NewTopic(
                name=payload.topic_name,
                num_partitions=payload.partitions,
                replication_factor=payload.replication_factor,
                topic_configs=configs,
            )
        ])
    except TopicAlreadyExistsError:
        pass  # idempotent
    except Exception as e:
        raise HTTPException(500, f"Failed to create topic: {e}")
    finally:
        admin.close()

    existing = db.query(KafkaTopic).filter_by(topic_name=payload.topic_name).first()
    if not existing:
        db.add(KafkaTopic(
            topic_name=payload.topic_name,
            topic_type=payload.topic_type,
            partitions=payload.partitions,
            replication_factor=payload.replication_factor,
            retention_ms=payload.retention_ms,
            compression=payload.compression,
            description=payload.description,
            is_provisioned=True,
        ))
        db.commit()
    return {"message": f"Topic '{payload.topic_name}' created"}


@router.delete("/{topic_name:path}")
def delete_topic(topic_name: str, db: Session = Depends(get_db)):
    """Delete a Kafka topic (irreversible)."""
    admin = _admin()
    try:
        admin.delete_topics([topic_name])
    except UnknownTopicOrPartitionError:
        pass
    except Exception as e:
        raise HTTPException(500, f"Failed to delete topic: {e}")
    finally:
        admin.close()

    db.query(KafkaTopic).filter_by(topic_name=topic_name).delete()
    db.commit()
    return {"message": f"Topic '{topic_name}' deleted"}


@router.get("/consumer-groups")
def list_consumer_groups():
    """List all consumer groups and their lag."""
    admin = _admin()
    try:
        groups = admin.list_consumer_groups()
        return [{"group_id": g[0], "protocol_type": g[1]} for g in groups]
    finally:
        admin.close()


@router.post("/provision-defaults")
def provision_default_topics(db: Session = Depends(get_db)):
    """Provision all registered-but-not-yet-created topics from the control plane DB."""
    admin = _admin()
    pending = db.query(KafkaTopic).filter_by(is_provisioned=False).all()
    created = []
    errors = []
    for topic in pending:
        try:
            configs = {"retention.ms": str(topic.retention_ms), "compression.type": topic.compression or "lz4"}
            admin.create_topics([NewTopic(
                name=topic.topic_name,
                num_partitions=topic.partitions,
                replication_factor=topic.replication_factor,
                topic_configs=configs,
            )])
            topic.is_provisioned = True
            created.append(topic.topic_name)
        except TopicAlreadyExistsError:
            topic.is_provisioned = True
            created.append(topic.topic_name)
        except Exception as e:
            errors.append({"topic": topic.topic_name, "error": str(e)})
    db.commit()
    admin.close()
    return {"created": created, "errors": errors}

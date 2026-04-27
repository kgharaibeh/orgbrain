from datetime import datetime
from typing import Optional
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Integer, String, Text, func, ARRAY,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from database import Base


class DataSource(Base):
    __tablename__ = "cp_data_sources"
    id            = Column(Integer, primary_key=True)
    name          = Column(String(100), nullable=False, unique=True)
    display_name  = Column(String(200))
    source_type   = Column(String(30), nullable=False)
    host          = Column(String(200))
    port          = Column(Integer)
    database_name = Column(String(100))
    schema_name   = Column(String(100), default="public")
    username      = Column(String(100))
    password_ref  = Column(String(200))
    connector_name= Column(String(100))
    topic_prefix  = Column(String(100))
    tables_included = Column(ARRAY(String))
    config_json   = Column(JSONB)
    status        = Column(String(20), default="DRAFT")
    error_message = Column(Text)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class GovernanceSource(Base):
    __tablename__ = "cp_governance_sources"
    id                = Column(Integer, primary_key=True)
    source_name       = Column(String(100), nullable=False, unique=True)
    raw_topic_prefix  = Column(String(200), nullable=False)
    clean_topic_prefix= Column(String(200), nullable=False)
    entity_type       = Column(String(50))
    description       = Column(Text)
    is_active         = Column(Boolean, default=True)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    rules             = relationship("GovernanceRule", back_populates="source", cascade="all, delete-orphan")


class GovernanceRule(Base):
    __tablename__ = "cp_governance_rules"
    id                = Column(Integer, primary_key=True)
    source_id         = Column(Integer, ForeignKey("cp_governance_sources.id", ondelete="CASCADE"))
    field_name        = Column(String(100), nullable=False)
    field_pattern     = Column(String(200))
    method            = Column(String(30), nullable=False)
    vault_key         = Column(String(100))
    is_joinable       = Column(Boolean, default=False)
    is_reversible     = Column(Boolean, default=False)
    is_nullable       = Column(Boolean, default=False)
    generalize_map    = Column(JSONB)
    presidio_entities = Column(JSONB)
    notes             = Column(Text)
    is_active         = Column(Boolean, default=True)
    sort_order        = Column(Integer, default=0)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    updated_at        = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    source            = relationship("GovernanceSource", back_populates="rules")


class AnonymizationAudit(Base):
    __tablename__ = "cp_anonymization_audit"
    id           = Column(BigInteger, primary_key=True)
    event_time   = Column(DateTime(timezone=True), server_default=func.now())
    source_topic = Column(String(200))
    clean_topic  = Column(String(200))
    entity_type  = Column(String(50))
    entity_id    = Column(String(100))
    fields_count = Column(Integer)
    op           = Column(String(5))
    processed_by = Column(String(50), default="flink_anonymizer")


class KafkaTopic(Base):
    __tablename__ = "cp_kafka_topics"
    id                 = Column(Integer, primary_key=True)
    topic_name         = Column(String(200), nullable=False, unique=True)
    topic_type         = Column(String(20), nullable=False)
    partitions         = Column(Integer, default=4)
    replication_factor = Column(Integer, default=1)
    retention_ms       = Column(BigInteger, default=604800000)
    compression        = Column(String(20), default="lz4")
    description        = Column(Text)
    source_id          = Column(Integer, ForeignKey("cp_data_sources.id"))
    is_provisioned     = Column(Boolean, default=False)
    created_at         = Column(DateTime(timezone=True), server_default=func.now())


class FlinkJob(Base):
    __tablename__ = "cp_flink_jobs"
    id             = Column(Integer, primary_key=True)
    job_name       = Column(String(100), nullable=False)
    job_type       = Column(String(30), nullable=False)
    script_path    = Column(String(300))
    flink_job_id   = Column(String(50))
    status         = Column(String(20), default="STOPPED")
    config_json    = Column(JSONB)
    last_submitted = Column(DateTime(timezone=True))
    created_at     = Column(DateTime(timezone=True), server_default=func.now())


class PipelineRun(Base):
    __tablename__ = "cp_pipeline_runs"
    id            = Column(BigInteger, primary_key=True)
    run_time      = Column(DateTime(timezone=True), server_default=func.now())
    pipeline_name = Column(String(100))
    status        = Column(String(20))
    records_in    = Column(BigInteger, default=0)
    records_out   = Column(BigInteger, default=0)
    errors        = Column(Integer, default=0)
    duration_ms   = Column(Integer)
    details       = Column(JSONB)

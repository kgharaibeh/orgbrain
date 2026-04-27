"""
Flink Anonymizer Job — OrgBrain Governance Membrane
Consumes raw.* Kafka topics, applies PII rules, publishes to clean.* topics.
Also emits an audit event for every anonymized record.

Run:
  flink run -py flink_anonymizer_job.py \
    --vault-addr http://vault:8200 \
    --vault-token orgbrain-vault-root \
    --rules-dir /opt/flink/jobs/../rules \
    --kafka-brokers kafka:9092
"""

import json
import logging
import os
import time
from typing import Any

import requests
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)
from pyflink.datastream.functions import MapFunction, RichMapFunction

from pii_engine import PIIEngine, AnonymizationRule

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def load_rules_from_api(rules_api_url: str, raw_topic_prefix: str) -> tuple[str, list[AnonymizationRule]]:
    """
    Fetch governance rules from Control Plane API.
    Replaces file-based YAML loading — rules are now stored in PostgreSQL
    and edited through the web UI.
    """
    try:
        resp = requests.get(rules_api_url, timeout=10)
        resp.raise_for_status()
        all_sources = resp.json()
        for source in all_sources:
            if source["raw_topic_prefix"] == raw_topic_prefix:
                rules = [
                    AnonymizationRule(
                        field=r["field"],
                        method=r["method"],
                        vault_key=r.get("vault_key"),
                        joinable=r.get("joinable", False),
                        reversible=r.get("reversible", False),
                        nullable=r.get("nullable", False),
                        mapping=r.get("mapping"),
                        presidio_entities=r.get("presidio_entities"),
                    )
                    for r in source.get("rules", [])
                ]
                clean_topic = source["clean_topic_prefix"]
                log.info("Loaded %d rules for %s from Control Plane API", len(rules), raw_topic_prefix)
                return clean_topic, rules
        log.warning("No rules found for topic prefix: %s", raw_topic_prefix)
        return raw_topic_prefix.replace("raw.", "clean."), []
    except Exception as e:
        log.error("Failed to load rules from API %s: %s", rules_api_url, e)
        raise


def discover_topic_pairs(rules_api_url: str) -> list[tuple[str, str]]:
    """
    Dynamically discover all active source/topic pairs from the Control Plane API.
    No hardcoded TOPIC_PAIRS list — everything comes from the web UI configuration.
    """
    resp = requests.get(rules_api_url, timeout=10)
    resp.raise_for_status()
    return [
        (src["raw_topic_prefix"], src["clean_topic_prefix"])
        for src in resp.json()
        if src.get("rules")
    ]


class PIIAnonymizeFunction(RichMapFunction):
    """
    Flink RichMapFunction that anonymizes a single record.
    Fetches rules from Control Plane API in open() — once per task.
    Rules are configured entirely through the web UI; no YAML files.
    """

    def __init__(self, vault_addr: str, vault_token: str, rules_api_url: str, raw_topic_prefix: str):
        self._vault_addr = vault_addr
        self._vault_token = vault_token
        self._rules_api_url = rules_api_url
        self._raw_topic_prefix = raw_topic_prefix
        self._engine: PIIEngine | None = None

    def open(self, runtime_context: RuntimeContext):
        _, rules = load_rules_from_api(self._rules_api_url, self._raw_topic_prefix)
        self._engine = PIIEngine(
            vault_addr=self._vault_addr,
            vault_token=self._vault_token,
            rules=rules,
        )
        log.info("PIIEngine initialized for %s with %d rules (from API)", self._raw_topic_prefix, len(rules))

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
        except json.JSONDecodeError:
            log.warning("Skipping non-JSON record")
            return value

        # Extract the payload (Debezium wraps records in {schema, payload})
        payload = record.get("payload", record)
        anonymized = self._engine.anonymize_record(payload)

        # Wrap back with governance metadata
        output = {
            "payload": anonymized,
            "_governance": {
                "anonymized": True,
                "source_topic": self._raw_topic_prefix,
                "rules_version": "1.0",
                "processed_at": int(time.time() * 1000),
            },
        }
        return json.dumps(output)


class AuditEmitFunction(RichMapFunction):
    """Emits a lightweight audit record for each anonymized event."""

    def __init__(self, raw_topic: str):
        self._raw_topic = raw_topic

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            payload = record.get("payload", {})
            governance = record.get("_governance", {})
            audit = {
                "source_topic": self._raw_topic,
                "entity_id": payload.get("customer_id") or payload.get("tx_id") or payload.get("loan_id"),
                "op": payload.get("__op", "r"),
                "anonymized": governance.get("anonymized", False),
                "processed_at": governance.get("processed_at"),
                "field_count": len(payload),
            }
            return json.dumps(audit)
        except Exception as e:
            log.warning("Audit emit failed: %s", e)
            return json.dumps({"error": str(e)})


def build_pipeline(
    env: StreamExecutionEnvironment,
    raw_topic: str,
    clean_topic: str,
    rules_api_url: str,
    kafka_brokers: str,
    vault_addr: str,
    vault_token: str,
):
    kafka_props = {
        "bootstrap.servers": kafka_brokers,
        "group.id": f"orgbrain-anonymizer-{raw_topic}",
        "auto.offset.reset": "earliest",
    }

    consumer = FlinkKafkaConsumer(
        topics=raw_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )

    stream = env.add_source(consumer, source_name=f"kafka-source-{raw_topic}")

    # Anonymize
    anonymized_stream = stream.map(
        PIIAnonymizeFunction(vault_addr, vault_token, rules_api_url, raw_topic),
        output_type=Types.STRING(),
    ).name(f"anonymize-{raw_topic}")

    # Publish to clean topic
    clean_producer = FlinkKafkaProducer(
        topic=clean_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": kafka_brokers},
    )
    anonymized_stream.add_sink(clean_producer).name(f"kafka-sink-{clean_topic}")

    # Publish audit events
    audit_stream = anonymized_stream.map(
        AuditEmitFunction(raw_topic),
        output_type=Types.STRING(),
    ).name(f"audit-{raw_topic}")

    audit_producer = FlinkKafkaProducer(
        topic="kafka.pii_audit",
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": kafka_brokers},
    )
    audit_stream.add_sink(audit_producer).name(f"audit-sink-{raw_topic}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="OrgBrain PII Anonymizer Flink Job")
    parser.add_argument("--vault-addr",      default=os.getenv("VAULT_ADDR",       "http://vault:8200"))
    parser.add_argument("--vault-token",     default=os.getenv("VAULT_TOKEN",      "orgbrain-vault-root"))
    parser.add_argument("--rules-api-url",   default=os.getenv("RULES_API_URL",    "http://cp-backend:8088/api/governance/rules/export-all"))
    parser.add_argument("--kafka-brokers",   default=os.getenv("KAFKA_BROKERS",    "kafka:9092"))
    parser.add_argument("--parallelism",     type=int, default=2)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(30_000)

    # Discover topic pairs dynamically from Control Plane API (no hardcoded list)
    topic_pairs = discover_topic_pairs(args.rules_api_url)
    log.info("Discovered %d topic pipelines from Control Plane API", len(topic_pairs))

    for raw_topic, clean_topic in topic_pairs:
        build_pipeline(
            env=env,
            raw_topic=raw_topic,
            clean_topic=clean_topic,
            rules_api_url=args.rules_api_url,
            kafka_brokers=args.kafka_brokers,
            vault_addr=args.vault_addr,
            vault_token=args.vault_token,
        )
        log.info("Pipeline configured: %s → %s", raw_topic, clean_topic)

    log.info("Submitting Flink job: OrgBrain PII Anonymizer")
    env.execute("orgbrain-pii-anonymizer")


if __name__ == "__main__":
    main()

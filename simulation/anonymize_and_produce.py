"""
Standalone Anonymizer + Clean-Topic Producer — Domain-Agnostic
Reads data files from simulation/data/, applies PII rules fetched from the
Control Plane API, and publishes anonymized records to clean.* Kafka topics.

This is the offline equivalent of the Flink streaming job — useful for testing
PII rules without a running Flink cluster, and for any domain.

Topic mapping is loaded from topic_map.json (or --topic-config). Replace that
file to simulate any domain without changing this script.

Run: python anonymize_and_produce.py [--brokers localhost:9094]
                                      [--rules-api http://localhost:8088/api/governance/rules/export-all]
                                      [--topic-config simulation/topic_map.json]
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "governance" / "anonymizer"))

import requests
from kafka import KafkaProducer

DATA_DIR         = Path(__file__).parent / "data"
DEFAULT_CFG_PATH = Path(__file__).parent / "topic_map.json"


def _find_primary_id(record: dict):
    """Return value of the first *_id field — works for any domain."""
    for k, v in record.items():
        if k.endswith("_id") and v is not None:
            return str(v)
    return str(record.get("id", ""))


def load_topic_map(cfg_path: Path) -> dict:
    """Load filename -> (raw_topic, clean_topic) from a topic_map.json config."""
    with open(cfg_path) as f:
        cfg = json.load(f)
    return {
        fname: (entry["raw"], entry["clean"])
        for fname, entry in cfg["topics"].items()
    }


def load_rules(rules_api_url: str) -> dict:
    """Fetch all anonymization rule-sets from Control Plane API.
    Returns {raw_topic_prefix: [AnonymizationRule, ...]}."""
    from pii_engine import AnonymizationRule
    try:
        resp = requests.get(rules_api_url, timeout=10)
        resp.raise_for_status()
        sources  = resp.json()
        rule_map = {}
        for src in sources:
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
                for r in src.get("rules", [])
            ]
            rule_map[src["raw_topic_prefix"]] = rules
        print(f"Loaded rules for {len(rule_map)} topics from Control Plane API")
        return rule_map
    except Exception as e:
        print(f"WARNING: Could not load rules from API ({e}) — no anonymization applied")
        return {}


def main():
    parser = argparse.ArgumentParser(description="Offline PII anonymizer + clean-topic producer")
    parser.add_argument("--brokers",      default="localhost:9094")
    parser.add_argument("--rules-api",    default="http://localhost:8088/api/governance/rules/export-all")
    parser.add_argument("--vault-addr",   default="http://localhost:8200")
    parser.add_argument("--vault-token",  default="orgbrain-vault-root")
    parser.add_argument("--delay",        type=float, default=0.01)
    parser.add_argument("--topic-config", default=str(DEFAULT_CFG_PATH),
                        help="Path to topic_map.json (default: simulation/topic_map.json)")
    args = parser.parse_args()

    from pii_engine import PIIEngine

    cfg_path = Path(args.topic_config)
    if not cfg_path.exists():
        print(f"ERROR: topic config not found at {cfg_path}")
        raise SystemExit(1)

    topic_map = load_topic_map(cfg_path)
    rule_map  = load_rules(args.rules_api)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(3, 6, 0),
    )
    audit_producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        api_version=(3, 6, 0),
    )

    total_anon  = 0
    total_audit = 0

    for filename, (raw_topic, clean_topic) in topic_map.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"  SKIP {filename} (not found in {DATA_DIR})")
            continue

        records = json.loads(path.read_text())
        rules   = rule_map.get(raw_topic, [])
        engine  = PIIEngine(
            vault_addr=args.vault_addr,
            vault_token=args.vault_token,
            rules=rules,
        ) if rules else None

        print(f"  Anonymizing {len(records):4d} records ({len(rules)} PII rules)  {filename} -> {clean_topic}")

        for record in records:
            payload    = engine.anonymize_record(record) if engine else record
            entity_id  = _find_primary_id(payload)
            anonymized = {
                "payload": payload,
                "_governance": {
                    "anonymized":    engine is not None,
                    "source_topic":  raw_topic,
                    "rules_version": "1.0",
                    "processed_at":  int(time.time() * 1000),
                },
            }
            producer.send(clean_topic, value=anonymized, key=entity_id or None)

            audit_producer.send("kafka.pii_audit", value={
                "source_topic": raw_topic,
                "clean_topic":  clean_topic,
                "entity_id":    entity_id,
                "op":           "c",
                "anonymized":   engine is not None,
                "processed_at": int(time.time() * 1000),
                "field_count":  len(payload),
            })
            total_anon  += 1
            total_audit += 1

            if args.delay:
                time.sleep(args.delay)

        producer.flush()
        print(f"    ok  -> {clean_topic}")

    audit_producer.flush()
    producer.close()
    audit_producer.close()

    print(f"\nAnonymized {total_anon} records -> clean topics")
    print(f"Emitted   {total_audit} audit events -> kafka.pii_audit")
    print("\nNext: run ingest_to_brain.py to load the knowledge base")


if __name__ == "__main__":
    main()

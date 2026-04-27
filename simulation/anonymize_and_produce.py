"""
Standalone Anonymizer + Clean-Topic Producer
Reads raw simulation files → applies PII rules from Control Plane API →
publishes anonymized records to clean.* Kafka topics.

This is the offline equivalent of the Flink job, useful for testing
PII rules without a running Flink cluster.

Run: python anonymize_and_produce.py [--brokers localhost:9094] [--rules-api http://localhost:8088/api/governance/rules/export-all]
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "governance" / "anonymizer"))

from kafka import KafkaProducer
import requests

DATA_DIR = Path(__file__).parent / "data"

TOPIC_MAP = {
    "customers.json":    ("raw.core_banking.public.customers",    "clean.core_banking.customers"),
    "accounts.json":     ("raw.core_banking.public.accounts",     "clean.core_banking.accounts"),
    "transactions.json": ("raw.core_banking.public.transactions",  "clean.core_banking.transactions"),
    "cards.json":        ("raw.core_banking.public.cards",        "clean.core_banking.cards"),
    "loans.json":        ("raw.core_banking.public.loans",        "clean.core_banking.loans"),
}


def load_rules(rules_api_url: str) -> dict:
    """Fetch all anonymization rule-sets from Control Plane API.
    Returns a dict: raw_topic_prefix → list[AnonymizationRule]"""
    from pii_engine import PIIEngine, AnonymizationRule
    try:
        resp = requests.get(rules_api_url, timeout=10)
        resp.raise_for_status()
        sources = resp.json()
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
        print(f"WARNING: Could not load rules from API ({e}) — using empty rules (no anonymization)")
        return {}


def main():
    parser = argparse.ArgumentParser(description="Offline PII anonymizer for simulation data")
    parser.add_argument("--brokers",   default="localhost:9094")
    parser.add_argument("--rules-api", default="http://localhost:8088/api/governance/rules/export-all")
    parser.add_argument("--vault-addr",  default="http://localhost:8200")
    parser.add_argument("--vault-token", default="orgbrain-vault-root")
    parser.add_argument("--delay",     type=float, default=0.01)
    args = parser.parse_args()

    from pii_engine import PIIEngine, AnonymizationRule

    rule_map = load_rules(args.rules_api)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(3, 6, 0),
    )

    # Audit producer
    audit_producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        api_version=(3, 6, 0),
    )

    total_anon  = 0
    total_audit = 0

    for filename, (raw_topic, clean_topic) in TOPIC_MAP.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"  SKIP {filename} (not found)")
            continue

        records  = json.loads(path.read_text())
        rules    = rule_map.get(raw_topic, [])

        engine = PIIEngine(
            vault_addr=args.vault_addr,
            vault_token=args.vault_token,
            rules=rules,
        ) if rules else None

        print(f"  Anonymizing {len(records):4d} records ({len(rules)} PII rules) → {clean_topic}")

        key_field = next(
            (k for k in ["tx_id", "loan_id", "card_id", "account_id", "customer_id"]
             if k in (records[0] if records else {})),
            None,
        )

        for record in records:
            payload    = engine.anonymize_record(record) if engine else record
            anonymized = {
                "payload": payload,
                "_governance": {
                    "anonymized":  engine is not None,
                    "source_topic": raw_topic,
                    "rules_version": "1.0",
                    "processed_at":  int(time.time() * 1000),
                },
            }
            key = str(record.get(key_field, "")) if key_field else None
            producer.send(clean_topic, value=anonymized, key=key)

            # Emit audit event
            audit = {
                "source_topic": raw_topic,
                "clean_topic":  clean_topic,
                "entity_id":    payload.get("customer_id") or payload.get("tx_id") or payload.get("loan_id"),
                "op":           "c",
                "anonymized":   engine is not None,
                "processed_at": int(time.time() * 1000),
                "field_count":  len(payload),
            }
            audit_producer.send("kafka.pii_audit", value=audit)
            total_anon  += 1
            total_audit += 1

            if args.delay:
                time.sleep(args.delay)

        producer.flush()
        print(f"    ✓ → {clean_topic}")

    audit_producer.flush()
    producer.close()
    audit_producer.close()

    print(f"\nAnonymized {total_anon} records → clean topics")
    print(f"Emitted   {total_audit} audit events → kafka.pii_audit")
    print("\nNext: run ingest_to_brain.py to load the knowledge base")


if __name__ == "__main__":
    main()

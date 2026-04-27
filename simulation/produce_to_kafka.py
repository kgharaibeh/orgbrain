"""
Kafka Producer — Simulation
Reads JSON files from simulation/data/ and publishes them to raw.* Kafka topics
wrapped in Debezium CDC envelope format, simulating what Debezium would produce.

Run: python produce_to_kafka.py [--brokers localhost:9094]
"""

import argparse
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

DATA_DIR = Path(__file__).parent / "data"

TOPIC_MAP = {
    "customers.json":    "raw.core_banking.public.customers",
    "accounts.json":     "raw.core_banking.public.accounts",
    "transactions.json": "raw.core_banking.public.transactions",
    "cards.json":        "raw.core_banking.public.cards",
    "loans.json":        "raw.core_banking.public.loans",
}


def debezium_envelope(record: dict, table: str) -> dict:
    """Wrap a record in a Debezium CDC create-event envelope."""
    return {
        "schema": {
            "type": "struct",
            "name": f"core_banking.public.{table}.Envelope",
            "fields": [{"field": "payload", "type": "struct"}],
        },
        "payload": {
            **record,
            "__op":      "c",
            "__source":  {"connector": "simulation", "table": table, "ts_ms": int(time.time() * 1000)},
            "__ts_ms":   int(time.time() * 1000),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Produce simulation data to Kafka")
    parser.add_argument("--brokers", default="localhost:9094", help="Kafka bootstrap servers")
    parser.add_argument("--delay",   type=float, default=0.02, help="Seconds between messages")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(3, 6, 0),
    )

    total = 0
    for filename, topic in TOPIC_MAP.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"  SKIP {filename} (not found — run generate_banking_data.py first)")
            continue

        records  = json.loads(path.read_text())
        table    = filename.replace(".json", "")
        key_field = next(
            (k for k in ["tx_id", "loan_id", "card_id", "account_id", "customer_id"] if k in (records[0] if records else {})),
            None,
        )

        print(f"  Producing {len(records):4d} records → {topic}")
        for record in records:
            envelope = debezium_envelope(record, table)
            key      = str(record.get(key_field, "")) if key_field else None
            future   = producer.send(topic, value=envelope, key=key)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"    WARNING: send failed for key={key}: {e}")
            total += 1
            if args.delay:
                time.sleep(args.delay)

        producer.flush()
        print(f"    ✓ {filename} → {topic}")

    producer.close()
    print(f"\nProduced {total} total messages to Kafka.")
    print("Raw topics are now populated. The Flink anonymizer will process them.")
    print("Or run anonymize_and_produce.py for a standalone anonymization pass.")


if __name__ == "__main__":
    main()

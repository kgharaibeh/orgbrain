"""
Kafka Producer — Simulation
Reads JSON files from simulation/data/ and publishes them to raw.* Kafka topics
wrapped in Debezium CDC envelope format, simulating what Debezium would produce.

Domain-agnostic: topic mapping is loaded from topic_map.json (or a file you pass
via --topic-config). To simulate any domain, replace topic_map.json and put the
corresponding *.json data files in simulation/data/.

Run: python produce_to_kafka.py [--brokers localhost:9094] [--topic-config path/to/map.json]
"""

import argparse
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

DATA_DIR         = Path(__file__).parent / "data"
DEFAULT_CFG_PATH = Path(__file__).parent / "topic_map.json"


def load_topic_map(cfg_path: Path) -> dict:
    """Load filename -> raw_topic map from a topic_map.json config file."""
    with open(cfg_path) as f:
        cfg = json.load(f)
    return {fname: entry["raw"] for fname, entry in cfg["topics"].items()}


def _find_primary_id_field(record: dict):
    """Return the first *_id key found — works for any domain."""
    for k in record:
        if k.endswith("_id"):
            return k
    return None


def debezium_envelope(record: dict, source_name: str, table: str) -> dict:
    """Wrap a record in a Debezium CDC create-event envelope."""
    return {
        "schema": {
            "type":   "struct",
            "name":   f"{source_name}.{table}.Envelope",
            "fields": [{"field": "payload", "type": "struct"}],
        },
        "payload": {
            **record,
            "__op":     "c",
            "__source": {"connector": "simulation", "table": table,
                         "ts_ms": int(time.time() * 1000)},
            "__ts_ms":  int(time.time() * 1000),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Produce simulation data to Kafka raw topics")
    parser.add_argument("--brokers",      default="localhost:9094")
    parser.add_argument("--delay",        type=float, default=0.02)
    parser.add_argument("--topic-config", default=str(DEFAULT_CFG_PATH),
                        help="Path to topic_map.json (default: simulation/topic_map.json)")
    args = parser.parse_args()

    cfg_path = Path(args.topic_config)
    if not cfg_path.exists():
        print(f"ERROR: topic config not found at {cfg_path}")
        raise SystemExit(1)

    with open(cfg_path) as f:
        cfg = json.load(f)
    source_name = cfg.get("source_name", "simulation")
    topic_map   = {fname: entry["raw"] for fname, entry in cfg["topics"].items()}

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        api_version=(3, 6, 0),
    )

    total = 0
    for filename, raw_topic in topic_map.items():
        path = DATA_DIR / filename
        if not path.exists():
            print(f"  SKIP {filename} (not found in {DATA_DIR})")
            continue

        records   = json.loads(path.read_text())
        table     = filename.replace(".json", "")
        key_field = _find_primary_id_field(records[0]) if records else None

        print(f"  Producing {len(records):4d} records  {filename} -> {raw_topic}")
        for record in records:
            envelope = debezium_envelope(record, source_name, table)
            key      = str(record.get(key_field, "")) if key_field else None
            future   = producer.send(raw_topic, value=envelope, key=key)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"    WARNING: send failed for key={key}: {e}")
            total += 1
            if args.delay:
                time.sleep(args.delay)

        producer.flush()
        print(f"    ok  {filename} -> {raw_topic}")

    producer.close()
    print(f"\nProduced {total} total messages to Kafka.")
    print("Raw topics populated. Next: Flink anonymizer picks up from raw topics,")
    print("or run anonymize_and_produce.py for an offline anonymization pass.")


if __name__ == "__main__":
    main()

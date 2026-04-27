"""
Full Simulation Runner — End-to-End Pipeline
Orchestrates all steps in order:
  1. Generate synthetic banking data with PII
  2. Produce raw records to Kafka raw.* topics
  3. Anonymize PII and produce to clean.* topics (calls Control Plane API for rules)
  4. Ingest anonymized data into Neo4j + Qdrant + TimescaleDB

Run: python run_simulation.py [--skip-kafka] [--from-files-only]
  --skip-kafka     : skip steps 2 & 3 (data already in Kafka)
  --from-files     : step 4 reads from simulation/data/ instead of Kafka clean topics
"""

import argparse
import subprocess
import sys
from pathlib import Path

import requests

BASE = Path(__file__).parent
CP_API = "http://localhost:8088/api"


def _ensure_topics(cp_api: str = CP_API):
    """Provision default topics via the Control Plane API (avoids kafka-python admin client KRaft incompatibility)."""
    try:
        r = requests.post(f"{cp_api}/topics/provision-defaults", timeout=30)
        r.raise_for_status()
        print(f"  Topics provisioned via Control Plane API")
    except requests.exceptions.ConnectionError:
        print("  WARNING: Control Plane API not reachable at localhost:8088 — topics may need manual provisioning")
    except Exception as e:
        print(f"  WARNING: Topic provisioning returned: {e} — continuing anyway")


def run(label: str, script: str, extra_args: list = []):
    print(f"\n{'='*60}")
    print(f"  STEP: {label}")
    print(f"{'='*60}")
    result = subprocess.run(
        [sys.executable, str(BASE / script)] + extra_args,
        cwd=str(BASE),
    )
    if result.returncode != 0:
        print(f"\nERROR: {script} exited with code {result.returncode}")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="OrgBrain full simulation pipeline")
    parser.add_argument("--brokers",     default="localhost:9094")
    parser.add_argument("--rules-api",   default="http://localhost:8088/api/governance/rules/export-all")
    parser.add_argument("--vault-addr",  default="http://localhost:8200")
    parser.add_argument("--vault-token", default="orgbrain-vault-root")
    parser.add_argument("--skip-kafka",  action="store_true", help="Skip producing to Kafka (run generation + brain ingest only)")
    parser.add_argument("--from-files",  action="store_true", help="Brain ingest reads simulation/data/ instead of Kafka")
    args = parser.parse_args()

    print("""
╔═══════════════════════════════════════════════════════════╗
║         OrgBrain Banking Simulation Pipeline              ║
║  Generates → Anonymizes → Produces → Ingests to Brain     ║
╚═══════════════════════════════════════════════════════════╝
""")

    # Step 1: Generate
    run("Generate synthetic banking data", "generate_banking_data.py")

    if not args.skip_kafka:
        # Step 1b: Ensure Kafka topics exist via Control Plane API
        print("\n" + "="*60)
        print("  STEP: Create Kafka topics (if not already present)")
        print("="*60)
        _ensure_topics()
        # Step 2: Produce raw to Kafka
        run("Produce raw records to Kafka",
            "produce_to_kafka.py",
            ["--brokers", args.brokers])

        # Step 3: Anonymize + produce to clean topics
        run("Anonymize PII and produce to clean topics",
            "anonymize_and_produce.py",
            ["--brokers", args.brokers,
             "--rules-api", args.rules_api,
             "--vault-addr", args.vault_addr,
             "--vault-token", args.vault_token])

    # Step 4: Ingest to brain
    ingest_args = ["--brokers", args.brokers]
    if args.from_files or args.skip_kafka:
        ingest_args.append("--from-files")
    run("Ingest to Neo4j + Qdrant + TimescaleDB", "ingest_to_brain.py", ingest_args)

    print("""
╔═══════════════════════════════════════════════════════════╗
║  Simulation complete! Open the Control Plane to explore:  ║
║                                                           ║
║  Control Plane UI  → http://localhost:3001                ║
║    Brain Monitor   → /brain                               ║
║    Ontology        → /ontology                            ║
║    AI Agent        → /agent                               ║
║                                                           ║
║  Supporting UIs:                                          ║
║  Kafka UI          → http://localhost:8080                ║
║  Flink UI          → http://localhost:8082                ║
║  Neo4j Browser     → http://localhost:7474                ║
║  Airflow           → http://localhost:8090                ║
║  Grafana           → http://localhost:3000                ║
║  Vault             → http://localhost:8200                ║
╚═══════════════════════════════════════════════════════════╝
""")


if __name__ == "__main__":
    main()

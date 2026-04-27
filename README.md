# OrgBrain — Banking Intelligence Platform

A fully open-source organizational intelligence platform for retail banks.
Streams events from core banking systems, anonymizes PII at the Kafka layer,
builds a living knowledge graph + vector brain, and exposes a natural-language
insights agent.

## Architecture Overview

```
Source Systems → Kafka (Debezium CDC) → Governance Membrane (Flink + Vault)
                                              ↓
                                   [clean anonymized events]
                                    ↙         ↓         ↘
                               Neo4j    TimescaleDB    Qdrant
                             (graph)    (trends)    (vectors)
                                    ↘         ↓         ↙
                                   LangGraph Insights Agent
                                              ↓
                                    FastAPI / Superset / Grafana
```

## Quick Start

### Prerequisites
- Docker Desktop (8GB RAM minimum, 16GB recommended for Ollama)
- Docker Compose v2
- Make
- 20GB free disk space (Ollama models: ~8GB)

### 1. Start the full stack

```bash
make bootstrap
```

This starts all infrastructure, creates Kafka topics, initializes Vault keys,
and registers the Debezium CDC connector.

### 2. Start the processing layers

```bash
make submit-anonymizer   # Flink PII governance job
make submit-brain        # Neo4j + Qdrant ingest pipeline
make start-agent         # LangGraph insights agent API
```

### 3. Verify everything is running

```bash
make status
```

### 4. Ask the brain a question

```bash
make query Q="Which customers are at risk of churning this month?"
make query Q="What products are underperforming and should be reviewed?"
make query Q="Which customer segments should we target for a credit card upsell?"
make query Q="Summarize the bank's transaction patterns for the last 30 days"
```

Or open the interactive API: http://localhost:8000/docs

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Flink UI | http://localhost:8082 | — |
| Schema Registry | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | orgbrain_minio / orgbrain_minio_secret |
| Vault UI | http://localhost:8200 | Token: orgbrain-vault-root |
| Neo4j Browser | http://localhost:7474 | neo4j / orgbrain_neo4j |
| Grafana | http://localhost:3000 | admin / orgbrain_grafana |
| OrgBrain Agent | http://localhost:8000/docs | — |

---

## Project Structure

```
orgbrain/
├── docs/
│   └── banking-architecture.md      ← Detailed banking diagram + PII table
│
├── infra/
│   ├── docker-compose.yml           ← Full stack (Kafka, MinIO, Vault, Neo4j, Qdrant, Flink...)
│   ├── postgres/
│   │   ├── init.sql                 ← Core banking schema (CDC-enabled)
│   │   └── seed.sql                 ← Sample customers, accounts, transactions
│   └── vault/
│       └── init.sh                  ← FPE keys + access policies
│
├── connectors/
│   └── debezium-core-banking.json   ← Debezium CDC connector config
│
├── governance/
│   ├── rules/
│   │   └── core_banking.yaml        ← PII field rules (method, vault key, flags)
│   └── anonymizer/
│       ├── pii_engine.py            ← FPE + HMAC + Presidio NLP engine
│       └── flink_anonymizer_job.py  ← Flink job: raw → clean topics
│
├── brain/
│   ├── graph/
│   │   └── schema/
│   │       └── banking_ontology.cypher ← Neo4j constraints + indexes + seed
│   ├── timeseries/
│   │   └── init.sql                 ← TimescaleDB hypertables + continuous aggregates
│   └── vector/
│       ├── qdrant_init.py           ← Collection creation + payload indexes
│       └── embedding_pipeline.py   ← Kafka → Neo4j + Qdrant ingest with Ollama
│
├── agent/
│   ├── banking_agent.py             ← LangGraph agent + FastAPI wrapper
│   └── requirements.txt
│
├── scripts/
│   ├── create_topics.sh             ← Kafka topic creation script
│   └── churn_risk_scorer.py         ← Statistical churn/upsell scorer (runs every 6h)
│
└── Makefile                         ← All operational commands
```

---

## Implementation Phases

| Phase | Status | Description |
|---|---|---|
| **Phase 0** | ✅ Built | Kafka, MinIO, Vault, PostgreSQL core banking, Debezium CDC |
| **Phase 1** | ✅ Built | Flink PII anonymizer, governance rules, audit log |
| **Phase 2** | ✅ Built | Neo4j ontology, TimescaleDB trends, Qdrant embeddings |
| **Phase 3** | 🔄 Next | Airflow ontology enrichment, LLM-assisted relationship inference |
| **Phase 4** | ✅ Built | LangGraph insights agent, FastAPI, Superset dashboards |
| **Phase 5** | 📋 Planned | RBAC hardening, ML churn model, replay from Iceberg |

---

## Adding a New Source System

1. Add a Debezium connector config in `connectors/`
2. Add PII rules in `governance/rules/<system>.yaml`
3. Add topic pairs to `TOPIC_PAIRS` in `governance/anonymizer/flink_anonymizer_job.py`
4. Add narrator function in `brain/vector/embedding_pipeline.py`
5. Register the connector: `curl -X POST http://localhost:8083/connectors -d @connectors/<file>.json`

---

## PII Anonymization Methods

| Method | Description | Reversible | Joinable |
|---|---|---|---|
| `fpe_numeric` | Format-Preserving Encryption — digit-for-digit | Yes (Vault) | Yes |
| `fpe_date` | FPE on dates — preserves YYYY-MM-DD format | Yes (Vault) | No |
| `fpe_email` | FPE on email local part — preserves domain | Yes (Vault) | Yes |
| `hmac_sha256` | HMAC one-way hash — consistent token | No | Yes |
| `generalize` | Replaces with category (M/F → PERSON) | No | No |
| `suppress` | Removes field entirely | No | No |
| `nlp_scrub` | Presidio NER scan + redact on free text | No | No |

---

## Security Notes

- **Production deployment**: Replace `VAULT_DEV_ROOT_TOKEN_ID` with a proper Vault HA setup
- **FPE keys**: Rotate quarterly via `vault write -f transit/keys/<name>/rotate`
- **Deanonymization**: Requires explicit `deanonymizer-policy` Vault token — all calls are audit-logged
- **No PII in brain**: The brain never receives raw PII; the governance membrane is the only system that processes raw events, and it discards them after 7 days

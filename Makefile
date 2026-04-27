# =============================================================================
# OrgBrain — Banking Intelligence Platform
# =============================================================================

COMPOSE     = docker compose -f infra/docker-compose.yml
KAFKA_CLI   = docker exec orgbrain-kafka kafka-topics.sh --bootstrap-server localhost:9092
CONNECT_URL = http://localhost:8083/connectors

.PHONY: help up down logs status \
        topics register-connector \
        init-vault init-qdrant \
        submit-anonymizer submit-brain \
        query smoke-test clean

# ─── Default target ───────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "OrgBrain — Banking Intelligence Platform"
	@echo "─────────────────────────────────────────"
	@echo "  make up                  Start full stack"
	@echo "  make down                Stop and remove containers"
	@echo "  make logs                Tail all logs"
	@echo "  make status              Show service health"
	@echo ""
	@echo "  make topics              Create all Kafka topics"
	@echo "  make register-connector  Register Debezium CDC connector"
	@echo "  make init-vault          Bootstrap Vault keys and policies"
	@echo "  make init-qdrant         Create Qdrant collections"
	@echo ""
	@echo "  make submit-anonymizer   Deploy Flink PII anonymizer job"
	@echo "  make submit-brain        Start brain ingest pipeline"
	@echo ""
	@echo "  make query Q='...'       Ask the banking agent a question"
	@echo "  make smoke-test          Run end-to-end smoke test"
	@echo ""

# ─── Lifecycle ────────────────────────────────────────────────────────────────
up:
	$(COMPOSE) up -d --build
	@echo "Waiting for services..."
	@sleep 15
	@$(MAKE) _wait-kafka
	@$(MAKE) topics
	@echo ""
	@echo "✓ Stack is up. Run 'make status' to verify all services."

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f

status:
	@echo ""
	@echo "─── Service Health ──────────────────────────────────────────────────"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep orgbrain || true
	@echo ""
	@echo "─── UI Endpoints ────────────────────────────────────────────────────"
	@echo "  Kafka UI         →  http://localhost:8080"
	@echo "  Flink UI         →  http://localhost:8082"
	@echo "  Schema Registry  →  http://localhost:8081"
	@echo "  MinIO Console    →  http://localhost:9001  (orgbrain_minio / orgbrain_minio_secret)"
	@echo "  Vault UI         →  http://localhost:8200  (token: orgbrain-vault-root)"
	@echo "  Neo4j Browser    →  http://localhost:7474  (neo4j / orgbrain_neo4j)"
	@echo "  Grafana          →  http://localhost:3000  (admin / orgbrain_grafana)"
	@echo "  OrgBrain Agent   →  http://localhost:8000/docs"
	@echo ""

# ─── Kafka Topics ─────────────────────────────────────────────────────────────
topics:
	@echo "Creating Kafka topics..."
	$(KAFKA_CLI) --create --if-not-exists --topic raw.core_banking.public.customers    --partitions 4 --replication-factor 1 --config retention.ms=604800000
	$(KAFKA_CLI) --create --if-not-exists --topic raw.core_banking.public.accounts     --partitions 4 --replication-factor 1 --config retention.ms=604800000
	$(KAFKA_CLI) --create --if-not-exists --topic raw.core_banking.public.transactions --partitions 8 --replication-factor 1 --config retention.ms=604800000
	$(KAFKA_CLI) --create --if-not-exists --topic raw.core_banking.public.cards        --partitions 4 --replication-factor 1 --config retention.ms=604800000
	$(KAFKA_CLI) --create --if-not-exists --topic raw.core_banking.public.loans        --partitions 4 --replication-factor 1 --config retention.ms=604800000
	$(KAFKA_CLI) --create --if-not-exists --topic clean.core_banking.customers         --partitions 4 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic clean.core_banking.accounts          --partitions 4 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic clean.core_banking.transactions      --partitions 8 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic clean.core_banking.cards             --partitions 4 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic clean.core_banking.loans             --partitions 4 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic kafka.pii_audit                      --partitions 2 --replication-factor 1 --config retention.ms=-1
	$(KAFKA_CLI) --create --if-not-exists --topic raw.dlq.core_banking                 --partitions 2 --replication-factor 1 --config retention.ms=-1
	@echo "✓ Topics created."

# ─── Debezium Connector ───────────────────────────────────────────────────────
register-connector:
	@echo "Registering Debezium core banking CDC connector..."
	@until curl -sf $(CONNECT_URL) > /dev/null; do echo "  Waiting for Kafka Connect..."; sleep 5; done
	curl -sf -X POST $(CONNECT_URL) \
	  -H "Content-Type: application/json" \
	  -d @connectors/debezium-core-banking.json | python3 -m json.tool
	@echo "✓ Connector registered."

check-connector:
	@curl -sf $(CONNECT_URL)/debezium-core-banking/status | python3 -m json.tool

# ─── Vault ────────────────────────────────────────────────────────────────────
init-vault:
	@echo "Initializing Vault..."
	@until curl -sf http://localhost:8200/v1/sys/health > /dev/null; do echo "  Waiting for Vault..."; sleep 3; done
	docker exec orgbrain-vault-init /bin/sh /vault/init.sh || docker exec -e VAULT_ADDR=http://vault:8200 -e VAULT_TOKEN=orgbrain-vault-root orgbrain-vault /bin/sh /vault/init.sh
	@echo "✓ Vault initialized."

# ─── Qdrant Collections ───────────────────────────────────────────────────────
init-qdrant:
	@echo "Initializing Qdrant collections..."
	@until curl -sf http://localhost:6333/readyz > /dev/null; do echo "  Waiting for Qdrant..."; sleep 3; done
	cd brain/vector && pip install -q qdrant-client && python3 qdrant_init.py
	@echo "✓ Qdrant collections created."

# ─── Flink Jobs ───────────────────────────────────────────────────────────────
submit-anonymizer:
	@echo "Submitting PII anonymizer Flink job..."
	docker exec orgbrain-flink-jobmanager flink run \
	  -py /opt/flink/jobs/flink_anonymizer_job.py \
	  --vault-addr http://vault:8200 \
	  --vault-token orgbrain-vault-root \
	  --rules-dir /opt/flink/jobs/../rules \
	  --kafka-brokers kafka:9092
	@echo "✓ Anonymizer job submitted. Check http://localhost:8082"

# ─── Brain Ingest ─────────────────────────────────────────────────────────────
submit-brain:
	@echo "Starting brain ingest pipeline..."
	docker run -d --name orgbrain-brain-ingest \
	  --network orgbrain_orgbrain \
	  -e KAFKA_BROKERS=kafka:9092 \
	  -e NEO4J_URI=bolt://neo4j:7687 \
	  -e NEO4J_USER=neo4j \
	  -e NEO4J_PASSWORD=orgbrain_neo4j \
	  -e QDRANT_HOST=qdrant \
	  -e OLLAMA_HOST=http://ollama:11434 \
	  -v $(PWD)/brain:/app \
	  -w /app/vector \
	  python:3.11-slim \
	  sh -c "pip install -q confluent-kafka neo4j qdrant-client requests && python3 embedding_pipeline.py"
	@echo "✓ Brain ingest pipeline started."

# ─── Agent ────────────────────────────────────────────────────────────────────
start-agent:
	@echo "Starting OrgBrain banking agent..."
	docker run -d --name orgbrain-agent \
	  --network orgbrain_orgbrain \
	  -p 8000:8000 \
	  -e NEO4J_URI=bolt://neo4j:7687 \
	  -e NEO4J_USER=neo4j \
	  -e NEO4J_PASSWORD=orgbrain_neo4j \
	  -e TIMESCALE_HOST=timescale \
	  -e QDRANT_HOST=qdrant \
	  -e OLLAMA_HOST=http://ollama:11434 \
	  -v $(PWD)/agent:/app \
	  -w /app \
	  python:3.11-slim \
	  sh -c "pip install -q -r requirements.txt && python3 banking_agent.py"
	@echo "✓ Agent running at http://localhost:8000/docs"

query:
	@test -n "$(Q)" || (echo "Usage: make query Q='your question here'" && exit 1)
	curl -sf -X POST http://localhost:8000/query \
	  -H "Content-Type: application/json" \
	  -d '{"question": "$(Q)"}' | python3 -m json.tool

# ─── Full bootstrap sequence ──────────────────────────────────────────────────
bootstrap: up
	@sleep 30
	$(MAKE) init-vault
	$(MAKE) register-connector
	$(MAKE) init-qdrant
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  OrgBrain platform is ready. Next steps:"
	@echo "  1. make submit-anonymizer   → start PII governance membrane"
	@echo "  2. make submit-brain        → start brain ingest pipeline"
	@echo "  3. make start-agent         → start insights agent API"
	@echo "  4. make query Q='...'       → ask a question"
	@echo "═══════════════════════════════════════════════════════════════"

# ─── Smoke Test ───────────────────────────────────────────────────────────────
smoke-test:
	@echo "Running end-to-end smoke test..."
	@echo "1. Producing test transaction to raw topic..."
	docker exec orgbrain-kafka kafka-console-producer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic raw.core_banking.public.transactions \
	  --property "parse.key=true" \
	  --property "key.separator=:" <<< 'TX001:{"tx_id":"TX001","customer_id":"CUST001","account_number":"AE070330000010000001234","amount":150.00,"direction":"DEBIT","channel":"MOBILE_APP","merchant_name":"Test Merchant","merchant_mcc":"5999","tx_timestamp":"2026-04-25T10:00:00Z","status":"COMPLETED"}'
	@echo "2. Checking Kafka topics for clean output..."
	@sleep 5
	docker exec orgbrain-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic clean.core_banking.transactions \
	  --from-beginning --max-messages 1 --timeout-ms 10000
	@echo "✓ Smoke test complete."

# ─── Internal helpers ─────────────────────────────────────────────────────────
_wait-kafka:
	@echo "Waiting for Kafka to be ready..."
	@for i in $$(seq 1 30); do \
	  docker exec orgbrain-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && echo "✓ Kafka ready" && exit 0; \
	  echo "  Attempt $$i/30..."; sleep 5; \
	done; echo "✗ Kafka did not start in time" && exit 1

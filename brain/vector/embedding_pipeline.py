"""
Embedding Pipeline — OrgBrain Banking Brain
Consumes clean.* Kafka topics, narrates events, embeds via Ollama,
and upserts into Qdrant + Neo4j.
"""

import json
import logging
import time
import uuid
from typing import Optional

import requests
from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, UpdateStatus

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────────────────────────────────────────
# Event narration templates — turn structured events into natural language
# ─────────────────────────────────────────────────────────────────────────────

def narrate_transaction(event: dict) -> str:
    direction = event.get("direction", "")
    amount = event.get("amount", 0)
    currency = event.get("currency", "AED")
    channel = event.get("channel", "unknown channel")
    merchant = event.get("merchant_name") or "an unknown merchant"
    mcc = event.get("merchant_mcc", "")
    tx_type = event.get("tx_type", "transaction")
    customer_id = event.get("customer_id", "unknown")
    account = event.get("account_number", "unknown")
    ts = event.get("tx_timestamp", "")
    balance = event.get("balance_after")

    balance_note = f" Balance after: {balance:,.2f} {currency}." if balance else ""
    merchant_note = f" at {merchant} (MCC: {mcc})" if merchant != "an unknown merchant" else ""

    return (
        f"Customer {customer_id} made a {direction.lower()} {tx_type.lower()} "
        f"of {amount:,.2f} {currency}{merchant_note} via {channel} on {ts}."
        f" Account: {account}.{balance_note}"
    )


def narrate_customer(event: dict) -> str:
    cid = event.get("customer_id", "unknown")
    income = event.get("income_band", "unknown income")
    occ = event.get("occupation", "unknown occupation")
    city = event.get("city", "unknown city")
    risk = event.get("risk_rating", "LOW")
    kyc = event.get("kyc_status", "VERIFIED")
    op = event.get("__op", "c")
    action = "updated" if op == "u" else "onboarded"
    return (
        f"Customer {cid} was {action}. "
        f"Profile: {occ} in {city}, income band {income}, risk rating {risk}, KYC {kyc}."
    )


def narrate_loan(event: dict) -> str:
    cid = event.get("customer_id", "unknown")
    loan_type = event.get("loan_type", "loan")
    principal = event.get("principal", 0)
    currency = "AED"
    rate = event.get("interest_rate", 0)
    tenor = event.get("tenor_months", 0)
    status = event.get("status", "ACTIVE")
    dpd = event.get("dpd", 0)
    dpd_note = f" Currently {dpd} days past due." if dpd > 0 else ""
    return (
        f"Customer {cid} has a {loan_type} loan of {principal:,.2f} {currency} "
        f"at {rate*100:.2f}% interest over {tenor} months. Status: {status}.{dpd_note}"
    )


NARRATORS = {
    "clean.core_banking.transactions": narrate_transaction,
    "clean.core_banking.customers":    narrate_customer,
    "clean.core_banking.loans":        narrate_loan,
}


# ─────────────────────────────────────────────────────────────────────────────
# Ollama embedding client
# ─────────────────────────────────────────────────────────────────────────────

class OllamaEmbedder:
    def __init__(self, host: str = "http://ollama:11434", model: str = "nomic-embed-text"):
        self._host = host
        self._model = model

    def embed(self, text: str) -> list[float]:
        resp = requests.post(
            f"{self._host}/api/embeddings",
            json={"model": self._model, "prompt": text},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["embedding"]


# ─────────────────────────────────────────────────────────────────────────────
# Neo4j upsert helpers
# ─────────────────────────────────────────────────────────────────────────────

CYPHER_UPSERT_CUSTOMER = """
MERGE (c:Customer {id: $customer_id})
SET c += {
  income_band: $income_band,
  occupation:  $occupation,
  city:        $city,
  risk_rating: $risk_rating,
  kyc_status:  $kyc_status,
  is_active:   $is_active,
  updated_at:  $updated_at
}
"""

CYPHER_UPSERT_TRANSACTION = """
MERGE (t:Transaction {id: $tx_id})
SET t += {
  amount:       $amount,
  direction:    $direction,
  channel:      $channel,
  tx_type:      $tx_type,
  tx_timestamp: datetime($tx_timestamp),
  status:       $status,
  merchant_mcc: $merchant_mcc
}
WITH t
MERGE (c:Customer {id: $customer_id})
MERGE (c)-[:MADE]->(t)
WITH t
MERGE (a:Account {id: $account_id})
MERGE (t)-[:FROM_ACCOUNT]->(a)
WITH t
FOREACH (mid IN CASE WHEN $merchant_id IS NOT NULL THEN [$merchant_id] ELSE [] END |
  MERGE (m:Merchant {id: mid})
  SET m.name = $merchant_name, m.mcc = $merchant_mcc
  MERGE (t)-[:AT]->(m)
  MERGE (mc:MerchantCategory {mcc: $merchant_mcc})
  MERGE (m)-[:IN_CATEGORY]->(mc)
)
"""

CYPHER_UPSERT_ACCOUNT = """
MERGE (a:Account {id: $account_id})
SET a += {
  account_type: $account_type,
  currency:     $currency,
  status:       $status,
  balance:      $balance,
  product_code: $product_code,
  updated_at:   $updated_at
}
WITH a
MERGE (c:Customer {id: $customer_id})
MERGE (c)-[r:HOLDS]->(a)
ON CREATE SET r.since = $opened_date
WITH a
MERGE (p:Product {code: $product_code})
MERGE (a)-[:IS_PRODUCT]->(p)
"""

CYPHER_UPSERT_LOAN = """
MERGE (l:Loan {id: $loan_id})
SET l += {
  loan_type:      $loan_type,
  principal:      $principal,
  outstanding:    $outstanding,
  interest_rate:  $interest_rate,
  tenor_months:   $tenor_months,
  status:         $status,
  dpd:            $dpd,
  product_code:   $product_code
}
WITH l
MERGE (c:Customer {id: $customer_id})
MERGE (c)-[:HAS_LOAN]->(l)
WITH l
MERGE (p:Product {code: $product_code})
MERGE (l)-[:IS_PRODUCT]->(p)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

class BrainIngestPipeline:
    def __init__(
        self,
        kafka_brokers: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        qdrant_host: str,
        qdrant_port: int,
        ollama_host: str,
    ):
        self._consumer = Consumer({
            "bootstrap.servers": kafka_brokers,
            "group.id": "orgbrain-brain-ingest",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._neo4j = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._qdrant = QdrantClient(host=qdrant_host, port=qdrant_port)
        self._embedder = OllamaEmbedder(host=ollama_host)

    def _upsert_neo4j(self, topic: str, payload: dict):
        with self._neo4j.session() as session:
            if "transactions" in topic:
                session.run(CYPHER_UPSERT_TRANSACTION, **{
                    "tx_id":        payload.get("tx_id"),
                    "customer_id":  payload.get("customer_id"),
                    "account_id":   payload.get("account_id"),
                    "amount":       float(payload.get("amount", 0)),
                    "direction":    payload.get("direction"),
                    "channel":      payload.get("channel"),
                    "tx_type":      payload.get("tx_type"),
                    "tx_timestamp": str(payload.get("tx_timestamp", "")),
                    "status":       payload.get("status"),
                    "merchant_id":  payload.get("merchant_id"),
                    "merchant_name":payload.get("merchant_name"),
                    "merchant_mcc": payload.get("merchant_mcc"),
                })
            elif "customers" in topic:
                session.run(CYPHER_UPSERT_CUSTOMER, **{
                    "customer_id": payload.get("customer_id"),
                    "income_band": payload.get("income_band"),
                    "occupation":  payload.get("occupation"),
                    "city":        payload.get("city"),
                    "risk_rating": payload.get("risk_rating"),
                    "kyc_status":  payload.get("kyc_status"),
                    "is_active":   payload.get("is_active", True),
                    "updated_at":  str(payload.get("updated_at", "")),
                })
            elif "accounts" in topic:
                session.run(CYPHER_UPSERT_ACCOUNT, **{
                    "account_id":   payload.get("account_id"),
                    "customer_id":  payload.get("customer_id"),
                    "account_type": payload.get("account_type"),
                    "currency":     payload.get("currency", "AED"),
                    "status":       payload.get("status"),
                    "balance":      float(payload.get("balance", 0)),
                    "product_code": payload.get("product_code"),
                    "opened_date":  str(payload.get("opened_date", "")),
                    "updated_at":   str(payload.get("updated_at", "")),
                })
            elif "loans" in topic:
                session.run(CYPHER_UPSERT_LOAN, **{
                    "loan_id":       payload.get("loan_id"),
                    "customer_id":   payload.get("customer_id"),
                    "loan_type":     payload.get("loan_type"),
                    "principal":     float(payload.get("principal", 0)),
                    "outstanding":   float(payload.get("outstanding", 0)),
                    "interest_rate": float(payload.get("interest_rate", 0)),
                    "tenor_months":  payload.get("tenor_months"),
                    "status":        payload.get("status"),
                    "dpd":           payload.get("dpd", 0),
                    "product_code":  payload.get("product_code"),
                })

    def _upsert_qdrant(self, topic: str, payload: dict, narration: str):
        narrator = NARRATORS.get(topic)
        if narrator is None:
            return
        try:
            vector = self._embedder.embed(narration)
            collection = "banking_events"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, payload.get("tx_id") or narration[:64]))
            self._qdrant.upsert(
                collection_name=collection,
                points=[PointStruct(
                    id=point_id,
                    vector=vector,
                    payload={
                        "customer_id":  payload.get("customer_id"),
                        "event_type":   topic.split(".")[-1],
                        "entity_type":  topic.split(".")[-1].rstrip("s").capitalize(),
                        "amount":       float(payload.get("amount", 0)),
                        "merchant_mcc": payload.get("merchant_mcc"),
                        "channel":      payload.get("channel"),
                        "tx_timestamp": payload.get("tx_timestamp", ""),
                        "narration":    narration,
                    },
                )],
            )
        except Exception as e:
            log.warning("Qdrant upsert failed: %s", e)

    def run(self, topics: list[str]):
        self._consumer.subscribe(topics)
        log.info("Brain ingest pipeline started. Subscribed to: %s", topics)

        batch: list = []
        BATCH_SIZE = 50

        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    self._flush_batch(batch)
                    batch = []
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                payload = raw.get("payload", raw)
                topic = msg.topic()
                narrator = NARRATORS.get(topic)
                narration = narrator(payload) if narrator else json.dumps(payload)[:300]
                batch.append((topic, payload, narration, msg))
            except Exception as e:
                log.warning("Failed to parse message: %s", e)

            if len(batch) >= BATCH_SIZE:
                self._flush_batch(batch)
                batch = []

    def _flush_batch(self, batch: list):
        for topic, payload, narration, msg in batch:
            try:
                self._upsert_neo4j(topic, payload)
                self._upsert_qdrant(topic, payload, narration)
                self._consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                log.error("Flush error for topic %s: %s", topic, e)
        log.info("Flushed %d events to brain stores", len(batch))


if __name__ == "__main__":
    import os
    pipeline = BrainIngestPipeline(
        kafka_brokers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j"),
        qdrant_host=os.getenv("QDRANT_HOST", "qdrant"),
        qdrant_port=int(os.getenv("QDRANT_PORT", "6333")),
        ollama_host=os.getenv("OLLAMA_HOST", "http://ollama:11434"),
    )
    pipeline.run(topics=list(NARRATORS.keys()))

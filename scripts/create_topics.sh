#!/bin/bash
# Creates all Kafka topics with correct retention and partition settings.
# Called by Makefile 'topics' target, also runnable standalone.

set -e

BOOTSTRAP="localhost:9094"  # external port for host access

KAFKA_CLI="docker exec orgbrain-kafka kafka-topics.sh --bootstrap-server localhost:9092"

echo "Creating raw topics (7-day retention)..."
declare -A RAW_TOPICS=(
  ["raw.core_banking.public.customers"]=4
  ["raw.core_banking.public.accounts"]=4
  ["raw.core_banking.public.transactions"]=8
  ["raw.core_banking.public.cards"]=4
  ["raw.core_banking.public.loans"]=4
  ["raw.core_banking.public.loan_repayments"]=4
)
for topic in "${!RAW_TOPICS[@]}"; do
  partitions="${RAW_TOPICS[$topic]}"
  $KAFKA_CLI --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=lz4
  echo "  ✓ $topic"
done

echo ""
echo "Creating clean topics (infinite retention, compliance archive)..."
declare -A CLEAN_TOPICS=(
  ["clean.core_banking.customers"]=4
  ["clean.core_banking.accounts"]=4
  ["clean.core_banking.transactions"]=8
  ["clean.core_banking.cards"]=4
  ["clean.core_banking.loans"]=4
)
for topic in "${!CLEAN_TOPICS[@]}"; do
  partitions="${CLEAN_TOPICS[$topic]}"
  $KAFKA_CLI --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=-1 \
    --config compression.type=lz4
  echo "  ✓ $topic"
done

echo ""
echo "Creating system topics..."
$KAFKA_CLI --create --if-not-exists --topic kafka.pii_audit     --partitions 2 --replication-factor 1 --config retention.ms=-1
$KAFKA_CLI --create --if-not-exists --topic raw.dlq.core_banking --partitions 2 --replication-factor 1 --config retention.ms=2592000000  # 30 days

echo ""
echo "All Kafka topics created."
$KAFKA_CLI --list

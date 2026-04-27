-- =============================================================================
-- TimescaleDB Schema — OrgBrain Brain Store (Generic / Domain-Agnostic)
-- Works for banking, retail, e-commerce, manufacturing, insurance, or any domain.
-- Entity types and signal types are data-driven, not schema-driven.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────────────────────────────────────
-- brain_events: generic time-series event stream
--   Stores any event for any entity from any domain.
--   entity_type examples: Customer, Order, Asset, Policy, SKU, Employee, Device
--   event_type  examples: transaction, order_placed, claim_filed, sensor_reading
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS brain_events (
    time         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    entity_type  TEXT         NOT NULL,
    entity_id    TEXT         NOT NULL,
    event_type   TEXT,
    source_topic TEXT,
    payload      JSONB
);

SELECT create_hypertable('brain_events', 'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day');

CREATE INDEX IF NOT EXISTS idx_brain_events_entity
    ON brain_events (entity_type, entity_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_brain_events_event_type
    ON brain_events (event_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_brain_events_payload
    ON brain_events USING GIN (payload);

-- ─────────────────────────────────────────────────────────────────────────────
-- brain_signals: AI-inferred scores and insights
--   Written by Airflow DAGs after analysing graph + event patterns.
--   signal_type examples: churn_risk, fraud_risk, upsell_score, anomaly_score,
--                         sentiment, demand_forecast, maintenance_risk
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS brain_signals (
    time         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    entity_type  TEXT         NOT NULL,
    entity_id    TEXT         NOT NULL,
    signal_type  TEXT         NOT NULL,
    score        NUMERIC(6,4) DEFAULT 0,
    metadata     JSONB,
    source_dag   TEXT
);

SELECT create_hypertable('brain_signals', 'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '7 days');

CREATE INDEX IF NOT EXISTS idx_brain_signals_entity
    ON brain_signals (entity_type, entity_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_brain_signals_type_score
    ON brain_signals (signal_type, score DESC, time DESC);
CREATE INDEX IF NOT EXISTS idx_brain_signals_metadata
    ON brain_signals USING GIN (metadata);

-- ─────────────────────────────────────────────────────────────────────────────
-- Views
-- ─────────────────────────────────────────────────────────────────────────────

-- Latest signal per entity per signal type
CREATE OR REPLACE VIEW brain_latest_signals AS
SELECT DISTINCT ON (entity_type, entity_id, signal_type)
    entity_type, entity_id, signal_type, score, metadata, time, source_dag
FROM brain_signals
ORDER BY entity_type, entity_id, signal_type, time DESC;

-- High-risk entities across all domains and signal types
CREATE OR REPLACE VIEW brain_high_risk_entities AS
SELECT entity_type, entity_id, signal_type, score, metadata, time
FROM brain_latest_signals
WHERE score > 0.6
ORDER BY score DESC;

-- Event activity summary per entity type (last 7 days)
CREATE OR REPLACE VIEW brain_entity_activity AS
SELECT
    entity_type,
    COUNT(*)                        AS event_count,
    COUNT(DISTINCT entity_id)       AS unique_entities,
    MAX(time)                       AS latest_event,
    MIN(time)                       AS earliest_event
FROM brain_events
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY entity_type
ORDER BY event_count DESC;

"""
DAG: product_adoption
Daily — aggregates entity-type activity metrics from brain_events and writes
adoption signals to brain_signals (generic / domain-agnostic).

In banking:  entity types = Account, Card, Loan → product adoption signals
In retail:   entity types = Order, Subscription  → engagement signals
In SaaS:     entity types = Feature, Module      → feature adoption signals

No domain-specific table queries — everything reads from brain_events.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":             "orgbrain",
    "retries":           1,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="product_adoption",
    description="Generic entity adoption metrics — writes to brain_signals daily",
    schedule_interval="30 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["product", "timescale", "adoption", "generic"],
) as dag:

    def compute_adoption(**context):
        import json
        import psycopg2
        import psycopg2.extras

        TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "timescale")
        TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
        TIMESCALE_DB   = os.getenv("TIMESCALE_DB",   "orgbrain_metrics")
        TIMESCALE_USER = os.getenv("TIMESCALE_USER", "orgbrain")
        TIMESCALE_PASS = os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret")

        ts_conn = psycopg2.connect(
            host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
            user=TIMESCALE_USER, password=TIMESCALE_PASS,
        )

        try:
            cur = ts_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Per entity type: activity volume in 30d vs 7d (adoption trend)
            cur.execute("""
                SELECT
                    entity_type,
                    COUNT(DISTINCT entity_id)                                                AS unique_entities,
                    COUNT(*)                                                                 AS total_events,
                    COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '7 days')                AS events_7d,
                    COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '30 days')               AS events_30d,
                    COUNT(DISTINCT entity_id) FILTER (WHERE time > NOW() - INTERVAL '7 days')  AS active_7d,
                    COUNT(DISTINCT entity_id) FILTER (WHERE time > NOW() - INTERVAL '30 days') AS active_30d
                FROM brain_events
                WHERE time > NOW() - INTERVAL '30 days'
                GROUP BY entity_type
            """)
            rows = cur.fetchall()

            ts_cur = ts_conn.cursor()
            inserted = 0

            for r in rows:
                etype     = r["entity_type"]
                active_30 = r["active_30d"] or 0
                active_7  = r["active_7d"]  or 0
                total     = r["unique_entities"] or 1

                # Adoption score: fraction of entities active in last 7d
                adoption_score = round(active_7 / max(active_30, 1), 4)
                # Growth score: 7d activity rate vs 30d average
                daily_avg_30 = (r["events_30d"] or 0) / 30.0
                daily_avg_7  = (r["events_7d"]  or 0) / 7.0
                growth_score = round(min(daily_avg_7 / max(daily_avg_30, 1), 2.0) / 2.0, 4)

                meta = json.dumps({
                    "unique_entities": total,
                    "active_30d":      active_30,
                    "active_7d":       active_7,
                    "events_30d":      r["events_30d"],
                    "events_7d":       r["events_7d"],
                })

                ts_cur.execute(
                    """
                    INSERT INTO brain_signals
                        (time, entity_type, entity_id, signal_type, score, metadata, source_dag)
                    VALUES
                        (NOW(), %s, '__aggregate__', 'adoption_score', %s, %s::jsonb, 'product_adoption'),
                        (NOW(), %s, '__aggregate__', 'growth_score',   %s, %s::jsonb, 'product_adoption')
                    """,
                    (etype, adoption_score, meta,
                     etype, growth_score,   meta),
                )
                inserted += 1

            ts_conn.commit()
            ts_cur.close()
            cur.close()
            print(f"Adoption metrics: {inserted} entity types scored")
            return inserted
        finally:
            ts_conn.close()

    PythonOperator(
        task_id="compute_product_adoption",
        python_callable=compute_adoption,
    )

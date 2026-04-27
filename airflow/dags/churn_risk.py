"""
DAG: churn_risk
Every 6 hours — computes churn + upsell scores for any entity type that has
activity data in brain_events. Writes results to brain_signals (generic).

Works for any domain:
  - Banking  : entity_type=Customer, signal_types: churn_risk, upsell_score
  - Retail   : entity_type=Customer or Subscriber
  - SaaS     : entity_type=Account or User
Scoring derives from event velocity in brain_events — no domain assumptions.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":             "orgbrain",
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="churn_risk",
    description="Generic churn + upsell scoring — writes to brain_signals every 6 hours",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["risk", "timescale", "scoring", "generic"],
) as dag:

    def score_churn(**context):
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

            # Discover all entity types that have events — fully domain-agnostic
            cur.execute("""
                SELECT DISTINCT entity_type FROM brain_events
                WHERE time > NOW() - INTERVAL '90 days'
            """)
            entity_types = [r["entity_type"] for r in cur.fetchall()]

            ts_cur = ts_conn.cursor()
            scored = 0

            for etype in entity_types:
                cur.execute("""
                    SELECT
                        entity_id,
                        COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '30 days')  AS events_30d,
                        COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '90 days')  AS events_90d,
                        MAX(time)                                                   AS last_event
                    FROM brain_events
                    WHERE entity_type = %s
                    GROUP BY entity_id
                """, (etype,))
                rows = cur.fetchall()

                for r in rows:
                    events_30d    = r["events_30d"] or 0
                    events_90d    = r["events_90d"] or 0
                    last_event    = r["last_event"]
                    days_inactive = (
                        (datetime.utcnow() - last_event.replace(tzinfo=None)).days
                        if last_event else 180
                    )

                    avg_30d_rate  = events_90d / 3.0
                    velocity_drop = max(0.0, min(1.0, 1.0 - (events_30d / max(avg_30d_rate, 1))))
                    churn_score   = round((velocity_drop * 0.6) + (min(days_inactive, 90) / 90.0 * 0.4), 4)
                    upsell_score  = round(max(0.0, 1.0 - churn_score), 4)

                    meta = json.dumps({
                        "events_30d":    events_30d,
                        "events_90d":    events_90d,
                        "days_inactive": days_inactive,
                        "velocity_drop": round(velocity_drop, 4),
                    })

                    ts_cur.execute(
                        """
                        INSERT INTO brain_signals
                            (time, entity_type, entity_id, signal_type, score, metadata, source_dag)
                        VALUES
                            (NOW(), %s, %s, 'churn_risk',   %s, %s::jsonb, 'churn_risk'),
                            (NOW(), %s, %s, 'upsell_score', %s, %s::jsonb, 'churn_risk')
                        """,
                        (etype, r["entity_id"], churn_score,  meta,
                         etype, r["entity_id"], upsell_score, meta),
                    )
                    scored += 1

            ts_conn.commit()
            ts_cur.close()
            cur.close()
            print(f"Scoring complete: {scored} entities across {len(entity_types)} entity types")
            return scored
        finally:
            ts_conn.close()

    PythonOperator(
        task_id="score_churn_risk",
        python_callable=score_churn,
    )

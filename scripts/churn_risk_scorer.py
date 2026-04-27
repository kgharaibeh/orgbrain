"""
Churn Risk Scorer — runs as a periodic job (every 6 hours via Airflow or cron).
Reads customer transaction patterns from TimescaleDB and writes churn/upsell scores
back to the customer_risk_signals hypertable.

This is a simple statistical model (Phase 1). ML model replaces it in Phase 5.
"""

import logging
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def score_customers(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as read_cur:
        # Compute behavioral signals per customer over the last 90 days
        read_cur.execute("""
            WITH base AS (
                SELECT
                    customer_id,
                    COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '30 days')  AS tx_count_30d,
                    COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '90 days')  AS tx_count_90d,
                    SUM(amount) FILTER (WHERE direction='DEBIT' AND time > NOW() - INTERVAL '30 days') AS spend_30d,
                    SUM(amount) FILTER (WHERE direction='DEBIT' AND time > NOW() - INTERVAL '90 days') AS spend_90d,
                    MAX(time) AS last_tx_time,
                    COUNT(DISTINCT merchant_mcc) FILTER (WHERE time > NOW() - INTERVAL '30 days') AS category_diversity
                FROM tx_events
                WHERE time > NOW() - INTERVAL '90 days'
                GROUP BY customer_id
            )
            SELECT
                customer_id,
                tx_count_30d,
                tx_count_90d,
                spend_30d,
                spend_90d,
                last_tx_time,
                category_diversity,
                EXTRACT(EPOCH FROM (NOW() - last_tx_time)) / 86400 AS days_since_last_tx,
                -- Velocity drop: compare last 30d vs 30d before that
                CASE
                    WHEN tx_count_90d > 0 THEN
                        1.0 - (tx_count_30d::float / GREATEST((tx_count_90d - tx_count_30d), 1))
                    ELSE 0.5
                END AS velocity_drop_ratio
            FROM base
        """)
        rows = read_cur.fetchall()

    if not rows:
        log.info("No customers to score.")
        return

    scored = []
    now = datetime.now(timezone.utc)

    for row in rows:
        days_inactive = float(row["days_since_last_tx"] or 0)
        tx_30d = int(row["tx_count_30d"] or 0)
        velocity_drop = float(row["velocity_drop_ratio"] or 0)

        # ── Churn score (0-1): higher = more likely to churn ──────────────────
        # Factors: inactivity, velocity drop, low tx count
        churn_score = min(1.0, (
            0.4 * min(days_inactive / 60.0, 1.0) +   # 60+ days inactive = max
            0.4 * max(velocity_drop, 0.0) +           # severe velocity drop
            0.2 * max(1.0 - tx_30d / 10.0, 0.0)      # fewer than 10 tx/month
        ))

        # ── Upsell score (0-1): higher = more likely to accept an offer ───────
        # High engagement + high spend diversity = good upsell candidate
        upsell_score = min(1.0, (
            0.5 * min(tx_30d / 20.0, 1.0) +
            0.3 * min(float(row["category_diversity"] or 0) / 8.0, 1.0) +
            0.2 * max(1.0 - churn_score, 0.0)
        ))

        scored.append({
            "time": now,
            "customer_id": row["customer_id"],
            "churn_score": round(churn_score, 4),
            "upsell_score": round(upsell_score, 4),
            "fraud_score": 0.0,      # placeholder — fraud model in Phase 5
            "credit_stress_score": 0.0,
            "days_since_last_tx": int(days_inactive),
            "tx_count_30d": tx_30d,
            "tx_count_90d": int(row["tx_count_90d"] or 0),
            "avg_monthly_spend": float(row["spend_30d"] or 0),
            "product_count": 0,      # joined from graph in Phase 3
            "loan_dpd": 0,
            "signal_source": "statistical_v1",
        })

    with conn.cursor() as write_cur:
        psycopg2.extras.execute_batch(
            write_cur,
            """
            INSERT INTO customer_risk_signals
              (time, customer_id, churn_score, upsell_score, fraud_score,
               credit_stress_score, days_since_last_tx, tx_count_30d, tx_count_90d,
               avg_monthly_spend, product_count, loan_dpd, signal_source)
            VALUES
              (%(time)s, %(customer_id)s, %(churn_score)s, %(upsell_score)s,
               %(fraud_score)s, %(credit_stress_score)s, %(days_since_last_tx)s,
               %(tx_count_30d)s, %(tx_count_90d)s, %(avg_monthly_spend)s,
               %(product_count)s, %(loan_dpd)s, %(signal_source)s)
            """,
            scored,
            page_size=500,
        )
        conn.commit()

    log.info("Scored %d customers, written to customer_risk_signals", len(scored))


if __name__ == "__main__":
    conn = psycopg2.connect(
        host=os.getenv("TIMESCALE_HOST", "localhost"),
        port=int(os.getenv("TIMESCALE_PORT", "5433")),
        dbname=os.getenv("TIMESCALE_DB", "orgbrain_metrics"),
        user=os.getenv("TIMESCALE_USER", "orgbrain"),
        password=os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret"),
    )
    score_customers(conn)
    conn.close()

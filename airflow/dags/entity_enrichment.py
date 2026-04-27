"""
DAG: entity_enrichment
Daily — pulls entities without summaries from Neo4j, generates LLM summaries,
writes the summary back as a node property.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/dags")

default_args = {
    "owner":             "orgbrain",
    "retries":           1,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="entity_enrichment",
    description="Daily LLM enrichment of Customer and Merchant nodes in Neo4j",
    schedule_interval="0 1 * * *",  # 01:00 daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ontology", "neo4j", "enrichment"],
) as dag:

    def enrich_entities(**context):
        from ontology.entity_enricher import run_enrichment
        result = run_enrichment(limit=200)
        print(f"Enrichment complete: {result}")
        return result

    PythonOperator(
        task_id="enrich_entities",
        python_callable=enrich_entities,
    )

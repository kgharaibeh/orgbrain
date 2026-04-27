"""
DAG: relationship_inference
Weekly — samples the Neo4j graph, asks Ollama to propose new relationship types/properties,
persists proposals to cp_schema_proposals for human review in the Control Plane UI.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys, os
sys.path.insert(0, "/opt/airflow/dags")

default_args = {
    "owner":            "orgbrain",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="relationship_inference",
    description="LLM-assisted graph relationship inference — proposes new Neo4j schema extensions",
    schedule_interval="0 2 * * 0",  # Sunday 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ontology", "neo4j", "llm"],
) as dag:

    def run_inference(**context):
        from ontology.inference_engine import run_inference
        dag_run_id = context["run_id"]
        count = run_inference(dag_run_id=dag_run_id)
        print(f"Inference complete — {count} proposals saved")
        return count

    PythonOperator(
        task_id="infer_relationships",
        python_callable=run_inference,
    )

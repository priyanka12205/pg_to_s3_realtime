# airflow/dag_postgres_to_s3.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Allow importing our etl module if DAGs are placed one level up from the repo root for example.
import sys
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(os.path.join(repo_root, "etl"))

import etl_s3  # type: ignore

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_s3_parquet",
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["postgres", "s3", "parquet"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl_once",
        python_callable=etl_s3.main,
    )

    run_etl
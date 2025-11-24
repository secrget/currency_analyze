from airflow import DAG
from  airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from scripts.nbu_loader import load_nbu_data

with DAG(
    "nbu_etl_to_postgres",
    start_date= datetime(2025, 1, 1),
    schedule_interval= "@daily",
    catchup= False
) as dag:
    load = PythonOperator(
        task_id="load_nbu",
        python_callable=load_nbu_data
    )
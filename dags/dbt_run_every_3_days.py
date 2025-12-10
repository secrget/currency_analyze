from airflow import DAG
from  airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


DBT_PROJECT_PATH = "*"
with DAG(
    dag_id="dbt_run_every_3_days",
    description="Запуск dbt кожні 3 дні",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 */3 * *",
    catchup=False
) as dag:
    run_dbt=BashOperator(
        task_id = "run_dbt_models",
        bash_command=f"""cd {DBT_PROJECT_PATH} && \
        dbt build
        """

    )

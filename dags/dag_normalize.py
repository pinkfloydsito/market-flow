from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="dbt_normalize_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="A DAG to run DBT models sequentially",
) as dag:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed",
    )

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="dbt run --select core",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
    )

    dbt_seed >> dbt_run_core >> dbt_test

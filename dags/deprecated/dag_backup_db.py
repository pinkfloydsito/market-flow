from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "start_date": datetime(2023, 1, 1),
}

backup_dag = DAG(
    "backup_db",
    default_args=default_args,
    description="DAG to back up PostgreSQL database",
    schedule=None,
)

backup_task = BashOperator(
    task_id="backup_postgres",
    bash_command="""
    pg_dump -U airflow -h postgres market_flow > /opt/backups/market_flow_{{ ds }}.sql
    """,
    env={"PGPASSWORD": "airflow"},
    dag=backup_dag,
)

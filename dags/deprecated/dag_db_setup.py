import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text
from datetime import datetime

from sqlalchemy.engine import URL
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2

DATABASE_NAME = "market_flow"

SERVER_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/postgres"
DATABASE_URI = f"postgresql+psycopg2://airflow:airflow@postgres:5432/{DATABASE_NAME}"


def create_database():
    """
    Creates the target database if it does not exist.
    """
    # Parse connection URL
    url = URL.create(
        drivername="postgresql+psycopg2",
        username="airflow",
        password="airflow",
        host="postgres",
        port=5432,
        database="postgres",
    )

    conn_params = url.translate_connect_args()
    conn_params["user"] = conn_params.pop("username")

    connection = psycopg2.connect(**conn_params)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
        print(f"Database '{DATABASE_NAME}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{DATABASE_NAME}' already exists.")
    finally:
        cursor.close()
        connection.close()


def execute_schema_file(schema_file):
    engine = create_engine(DATABASE_URI)

    with engine.connect() as connection:
        with open(schema_file, "r") as file:
            sql_statements = file.read()
            for statement in sql_statements.split(";"):
                if statement.strip():
                    connection.execute(text(statement.strip()))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG(
    "load_schema_from_file",
    default_args=default_args,
    description="A DAG to load table schema from a file",
    schedule_interval=None,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
schema_file_path = os.path.join(current_dir, "./sql/tables.sql")

create_database_task = PythonOperator(
    task_id="create_database",
    python_callable=create_database,
    dag=dag,
)

execute_schema_task = PythonOperator(
    task_id="execute_schema_file",
    python_callable=execute_schema_file,
    op_args=[schema_file_path],
    dag=dag,
)

create_database_task >> execute_schema_task

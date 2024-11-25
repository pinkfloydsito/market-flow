from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import duckdb

import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("airflow.task")

DUCKDB_PATH = "/opt/airflow/db/analytics.duckdb"
RAW_SCHEMA_NAME = "raw"
RAW_TABLE_NAMES = ["raw_hdi", "raw_wfp"]
CLEANED_TABLE_NAMES = ["cleaned_hdi", "cleaned_wfp"]


def clean_columns_in_table(conn, table_name, cleaned_table_name):
    column_names = conn.execute(f"PRAGMA table_info(public.{table_name})").fetchall()

    def clean_column_name(col_name):
        """
        Cleans a column name by:
        - Replacing spaces with underscores.
        - Removing special characters.
        - Normalizing to lowercase.
        - Escaping reserved keywords with double quotes.
        """

        # Normalize column name
        col_name = col_name.strip().lower().replace(" ", "_")
        col_name = "".join(e for e in col_name if e.isalnum() or e == "_")

        # Escape reserved keywords
        if col_name.upper() in [
            "GROUPS",
            "SELECT",
            "WHERE",
            "FROM",
        ]:  # Add more as needed
            col_name = f'"{col_name}"'

        # Handle column names starting with digits
        if col_name[0].isdigit():
            col_name = f"col_{col_name}"

        return col_name

    renamed_columns = ", ".join(
        f' "{col[1]}" AS {clean_column_name(col[1])}' for col in column_names
    )

    query = f"""
            CREATE TABLE IF NOT EXISTS public.{cleaned_table_name} AS
            SELECT {renamed_columns} FROM public.{table_name};
        """

    logger.info(f"Cleaning columns in {table_name}..{cleaned_table_name}")
    conn.execute(query)


def clean_columns_in_duckdb():
    conn = duckdb.connect(DUCKDB_PATH)

    for raw_table, cleaned_table in zip(RAW_TABLE_NAMES, CLEANED_TABLE_NAMES):
        clean_columns_in_table(conn, raw_table, cleaned_table)

    conn.commit()
    conn.close()

    logging.info("Columns cleaned successfully.")


default_args = {
    "start_date": datetime(2024, 11, 1),
    "catchup": False,
}

with DAG(
    "duckdb_dbt_clean_pipeline",
    default_args=default_args,
    schedule_interval=None,
    description="Check schema, create raw table with dbt, and clean columns in DuckDB",
    tags=["duckdb", "dbt", "setup"],
) as dag:
    # Task 1: Creating raw tables
    create_raw_table_hdi = BashOperator(
        task_id="create_raw_table_hdi",
        bash_command="dbt run --select raw_hdi",
    )

    create_raw_table_wfp = BashOperator(
        task_id="create_raw_table_wfp",
        bash_command="dbt run --select raw_wfp",
    )

    # Task 2: Clean columns in DuckDB
    clean_columns = PythonOperator(
        task_id="clean_columns",
        python_callable=clean_columns_in_duckdb,
    )

    create_core_models = BashOperator(
        task_id="create_core",
        bash_command="dbt run --select core",
    )

    create_raw_table_hdi >> create_raw_table_wfp >> clean_columns >> create_core_models

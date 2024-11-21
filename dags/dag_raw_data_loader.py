import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/market_flow"

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_dir_path = os.path.join(current_dir, "./data")


def clean_column_name(col_name):
    """
    Cleans a column name by:
    - Replacing spaces with underscores.
    - Removing special characters.
    - Normalizing to lowercase.
    - Adding a prefix if the name starts with a number.
    """
    col_name = col_name.strip().lower()
    col_name = col_name.replace(" ", "_")
    col_name = "".join(e for e in col_name if e.isalnum() or e == "_")
    if col_name[0].isdigit():
        col_name = f"col_{col_name}"
    return col_name


CSV_TABLE_MAP = {
    "raw_hdi": f"{csv_dir_path}/raw_hdi.csv",
    "raw_wfp": f"{csv_dir_path}/raw_wfp.csv",
}


def parse_and_insert_csv(table_name, csv_file_path, chunksize=1000):
    """
    Parses a CSV and inserts its data into the specified table in chunks.
    """
    engine = create_engine(DATABASE_URI)

    df_iterator = pd.read_csv(csv_file_path, chunksize=chunksize, encoding="ISO-8859-1")

    with engine.connect() as connection:
        for chunk in df_iterator:
            chunk.columns = [clean_column_name(col) for col in chunk.columns]

            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
            )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG(
    "csv_to_multiple_tables",
    default_args=default_args,
    description="A DAG to parse multiple CSVs and load them into different tables",
    schedule_interval=None,
)

for table_name, csv_file_path in CSV_TABLE_MAP.items():
    task = PythonOperator(
        task_id=f"parse_and_insert_{table_name}",
        python_callable=parse_and_insert_csv,
        op_args=[table_name, csv_file_path],
        dag=dag,
    )

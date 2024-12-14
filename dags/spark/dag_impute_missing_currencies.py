from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
from sklearn.impute import SimpleImputer
import numpy as np

import os

from dotenv import load_dotenv
import logging

logger = logging.getLogger("airflow.task")

load_dotenv()
duckdb_path = os.getenv("DUCKDB_FILE", "duckdb.db")


def load_and_impute_data():
    conn = duckdb.connect(duckdb_path)

    df = conn.execute("""
        SELECT currency_code, year, month, value, created_at
        FROM raw.currencies_historical
        ORDER BY currency_code, year, month
    """).df()

    # pivot using yeear, month
    pivoted_df = df.pivot_table(
        index=["year", "month"], columns="currency_code", values="value"
    )

    # easy imputation
    imputed_pivot = pivoted_df.fillna(method="ffill").fillna(method="bfill")

    # Convert back to the original format
    imputed_df = imputed_pivot.reset_index().melt(
        id_vars=["year", "month"], var_name="currency_code", value_name="value"
    )

    # Add back created_at timestamp
    imputed_df["created_at"] = datetime.now()

    # Sort the data
    imputed_df = imputed_df.sort_values(["currency_code", "year", "month"])

    # Write back to DuckDB
    conn.execute("""
        -- Backup original table
        CREATE TABLE IF NOT EXISTS raw.currencies_historical_backup AS 
        SELECT * FROM raw.currencies_historical;
        
        -- Replace original data
        DELETE FROM raw.currencies_historical;
    """)

    # Insert new data
    conn.execute("""
        INSERT INTO raw.currencies_historical 
        (currency_code, year, month, value, created_at)
        SELECT currency_code,year, month, value, created_at FROM imputed_df
    """)

    print(f"Processed {len(imputed_df)} rows")
    print("\nSample of imputed data:")
    print(imputed_df.head())

    # Check for any remaining nulls
    null_count = len(imputed_df[imputed_df["value"].isna()])
    print(f"\nRemaining null values: {null_count}")

    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "currency_imputation",
    default_args=default_args,
    description="Impute missing values in currency historical data",
    schedule_interval="@daily",
    catchup=False,
    tags=["currency", "imputation"],
)

impute_task = PythonOperator(
    task_id="impute_currencies", python_callable=load_and_impute_data, dag=dag
)

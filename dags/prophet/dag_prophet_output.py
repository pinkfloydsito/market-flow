from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import os

from dotenv import load_dotenv
import logging

logger = logging.getLogger("airflow.task")

load_dotenv()
duckdb_path = os.getenv("DUCKDB_FILE")


def sql():
    return """
    SELECT 
    fact.price, 
    fact.price_per_kg,
    fact.price/dim_currency_value.value as price_usd, 
    fact.price_per_kg/dim_currency_value.value as price_per_kg_usd, 
    dim_date.year, 
    dim_date.month, 
    1 AS day, 
    CAST(dim_date.year || '-' || LPAD(dim_date.month::VARCHAR, 2, '0') || '-' || LPAD(1::VARCHAR, 2, '0') AS DATE) AS constructed_date,
    dim_market.name as market,
    dim_locality.name as locality,
    dim_locality.latitude as latitude,
    dim_locality.longitude as longitude,
    dim_country.name as country,
    dim_weather.temperature,
    dim_weather.precipitation,
    dim_commodity.name as product_name,
    dim_currency.name as currency_name
    FROM raw.fact_transaction fact
    INNER JOIN raw.dim_market dim_market ON fact.dim_market_id = dim_market.id
    INNER JOIN raw.dim_date dim_date ON fact.dim_date_id = dim_date.id
    INNER JOIN raw.dim_locality dim_locality ON fact.dim_locality_id = dim_locality.id
    INNER JOIN raw.dim_country dim_country ON dim_locality.country_id = dim_country.id
    INNER JOIN raw.dim_weather dim_weather ON fact.dim_weather_id = dim_weather.id
    INNER JOIN raw.dim_commodity dim_commodity ON fact.dim_commodity_id = dim_commodity.id
    INNER JOIN raw.dim_currency dim_currency ON fact.dim_currency_id = dim_currency.id
    INNER JOIN raw.dim_currency_historical dim_currency_value ON fact.dim_currency_historical_id = dim_currency_value.id
    ORDER BY dim_date.year, dim_date.month, dim_country.name, dim_locality.name, dim_commodity.name
    """


def extract_and_save_csv():
    db = duckdb.connect(str(duckdb_path), read_only=True)

    df = db.execute(sql()).df()

    db.close()

    df.to_csv("/opt/airflow/db/ml_data.csv", index=False)
    logger.info(df.head())
    logger.info("CSV file saved")


# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="create_csv_for_prophet_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ml", "prophet", "output"],
) as dag:
    create_csv = PythonOperator(
        task_id="extract_and_save_csv", python_callable=extract_and_save_csv
    )

    create_csv

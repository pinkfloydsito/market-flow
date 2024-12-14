from airflow import DAG
import requests
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import duckdb
import os
from dotenv import load_dotenv
import currencyapicom
import logging
import pandas as pd
from sqlalchemy.orm.base import CALLABLES_OK

from ratelimit import limits, sleep_and_retry
import backoff

logger = logging.getLogger("airflow.task")

duckdb_path: str = os.getenv("DUCKDB_FILE", "duckdb.db")

CURRENCY_CALLS_PER_MINUTE = 10


def create_raw_table(**context):
    """Create raw_currencies table if it doesn't exist"""
    db = duckdb.connect(duckdb_path)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw.currencies_historical (
        currency_code VARCHAR,
        year INTEGER,
        month INTEGER,
        value DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    db.execute(create_table_query)
    db.commit()
    db.close()


def get_missing_currency_data(**context):
    """Extract missing currency combinations from WFP data"""
    db = duckdb.connect(duckdb_path)
    query = """
    WITH needed_currencies AS (
        SELECT DISTINCT 
            CAST(mp_year AS INT) as year,
            CAST(mp_month AS INT) as month
        FROM raw.wfp
        WHERE mp_year IS NOT NULL 
            AND mp_month IS NOT NULL
    ),
    existing_currencies AS (
        SELECT DISTINCT year, month 
        -- SELECT currency_code
        FROM raw.currencies_historical
    )
    SELECT DISTINCT nc.year, nc.month
    FROM needed_currencies nc
    LEFT JOIN existing_currencies ec 
        ON nc.year = ec.year 
        AND nc.month = ec.month
    WHERE ec.year is NULL
    ORDER BY nc.year DESC, nc.month DESC -- XXX: Might need to limit this
    """
    df = db.execute(query).df()
    db.close()

    context["ti"].xcom_push(
        key="missing_currency_data", value=df.to_dict(orient="records")
    )


@sleep_and_retry
@limits(calls=CURRENCY_CALLS_PER_MINUTE, period=60)
@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException), max_tries=3)
def fetch_missing_currencies(**context):
    """Fetch missing currency data from API"""
    load_dotenv()
    currency_api_key = os.getenv("CURRENCY_API_KEY")
    client = currencyapicom.Client(currency_api_key)

    rows = context["ti"].xcom_pull(
        key="missing_currency_data", task_ids="get_missing_currency_data"
    )
    if not rows:
        logger.info("No missing currency data to fetch")
        return

    processed_dates = set()

    logger.info(f"Fetching data for {len(rows)} missing currency combinations")
    for r in rows:
        orig_year = r["year"]
        month = r["month"]
        # currency_code = r["currency_code"] XXX: historical fetches all currencies

        fetch_year = orig_year if orig_year >= 2000 else 2000
        date_key = (fetch_year, month)

        if date_key not in processed_dates:
            date = f"{fetch_year}-{month:02d}-01"
            try:
                time.sleep(1)
                result = client.historical(date)
                logger.info(f"Fetched data for {date}")
                logger.info(f"result: {result}")

                currency_data = result["data"]

                data_in_tuples = []

                for code, payload in currency_data.items():
                    value = payload["value"]

                    item = (
                        code,
                        orig_year,
                        month,
                        value,
                    )
                    data_in_tuples.append(item)

                def store_data_in_duckdb(data):
                    for item in data:
                        db = duckdb.connect(duckdb_path)
                        try:
                            db.execute(
                                """
                                INSERT INTO raw.currencies_historical (currency_code, year, month, value)
                                VALUES (?, ?, ?, ?)
                            """,
                                item,
                            )
                            db.commit()

                        except Exception as e:
                            print(f"Error inserting data: {str(e)}")
                            continue

                        finally:
                            db.close()

                logger.info(f"Storing data {data_in_tuples} in DuckDB")
                store_data_in_duckdb(data_in_tuples)

                processed_dates.add(date_key)
            except Exception as e:
                logger.error(f"Error fetching data for {date}: {str(e)}")
                continue


default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    "raw_fetch_currencies",
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily",
    tags=["duckdb", "dbt", "currency", "raw"],
) as dag:
    create_table = PythonOperator(
        task_id="create_raw_table", python_callable=create_raw_table
    )

    get_missing_data = PythonOperator(
        task_id="get_missing_currency_data", python_callable=get_missing_currency_data
    )

    fetch_data = PythonOperator(
        task_id="fetch_missing_currencies",
        python_callable=fetch_missing_currencies,
        provide_context=True,
    )

    create_table >> get_missing_data >> fetch_data

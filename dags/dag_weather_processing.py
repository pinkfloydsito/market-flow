import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import logging
from pendulum import local

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split


import time

from api.weather_api import WeatherAPI

logger = logging.getLogger("airflow.task")


def get_start_end_dates(year, month):
    start_date = datetime(year, month, 1)

    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)

    return start_date, end_date


api = WeatherAPI()

cleaned_csv_file_path = "/opt/airflow/db/weather_daily_imputed.csv"
csv_file_path = "/opt/airflow/db/weather_daily.csv"
DUCKDB_FILE = "/opt/airflow/db/analytics.duckdb"

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# TODO: Impute missing years.
def get_localities():
    """Fetch localities with latitude and longitude."""
    conn = duckdb.connect(DUCKDB_FILE)
    if os.path.exists(csv_file_path):
        query_load_csv = f"""
        CREATE OR REPLACE TEMP TABLE daily_weather AS
        SELECT DISTINCT locality_id, year, month
        FROM read_csv_auto('{csv_file_path}');
        """
        conn.execute(query_load_csv)
        daily_weather_exists = True
    else:
        logger.warning(
            f"CSV file {csv_file_path} not found. Fetching all unprocessed localities."
        )
        daily_weather_exists = False

    if daily_weather_exists:
        query = """
            WITH weather_data AS (
                SELECT id, locality_id, latitude, longitude, month, year
                FROM public.weather 
                WHERE avg_temperature IS NULL
            )

            SELECT wd.id, wd.locality_id, wd.latitude, wd.longitude, wd.month, wd.year
            FROM weather_data wd
            LEFT JOIN daily_weather dw
            ON wd.locality_id = dw.locality_id AND wd.year = dw.year AND wd.month = dw.month
            WHERE dw.locality_id IS NULL;
        """
    else:
        query = """
        SELECT id, locality_id, latitude, longitude, month, year
        FROM public.weather 
        WHERE avg_temperature IS NULL -- AND year >= 2016;
        """

    localities = conn.execute(query).fetchall()
    conn.close()
    return localities


def fetch_weather_data(locality):
    """Fetch weather data for a specific locality."""
    id, locality_id, latitude, longitude, month, year = locality

    locality_year = year
    if year < 2021:
        year = 2021
    start_date, end_date = get_start_end_dates(year, month)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "month": month,
        "year": locality_year,
    }

    logger.info(f"Fetching weather data for with Params: {params}")

    time.sleep(0.5)
    df = api.fetch_weather_data(
        latitude,
        longitude,
        start_date,
        end_date,
        month,
    )

    df["locality_id"] = locality_id
    df["year"] = locality_year

    return df


def impute_missing_data():
    """Impute missing weather data using an AI model"""

    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"{csv_file_path} does not exist. Cannot impute data.")

    df = pd.read_csv(csv_file_path)
    logger.info(f"Initial missing values:\n{df.isnull().sum()}")

    target_columns = ["temperature_2m_max", "temperature_2m_min", "precipitation_hours"]

    for target in target_columns:
        if target in df.columns:
            # Separate rows with and without missing values
            train_data = df[df[target].notnull()]
            missing_data = df[df[target].isnull()]

            if not missing_data.empty:
                X_train = train_data[["latitude", "longitude", "month", "year"]]
                y_train = train_data[target]
                X_missing = missing_data[["latitude", "longitude", "month", "year"]]

                model = RandomForestRegressor(random_state=42)
                model.fit(X_train, y_train)

                df.loc[df[target].isnull(), target] = model.predict(X_missing)

    logger.info(f"Missing values after AI-based imputation:\n{df.isnull().sum()}")

    df.to_csv(cleaned_csv_file_path, index=False)
    logger.info(f"AI-imputed data saved to {cleaned_csv_file_path}.")


def store_weather_data(df):
    """Store weather data in the DuckDB weather table."""

    if not os.path.exists(csv_file_path):
        df.to_csv(csv_file_path, index=False)
        print(f"Created new CSV file: {csv_file_path}")
    else:
        df.to_csv(csv_file_path, mode="a", header=False, index=False)
        print(f"Appended data to existing CSV file: {csv_file_path}")


def fetch_and_store_weather():
    """Fetch and store weather data for all localities."""
    localities = get_localities()
    for locality in localities:
        weather_data = fetch_weather_data(locality)
        if weather_data is not None:
            store_weather_data(weather_data)


def group_and_insert_cleaned_weather_data():
    """Group weather data by locality_id, year, month, and insert into public.weather."""
    conn = duckdb.connect(DUCKDB_FILE)

    query = f"""
    WITH aggregated_data AS (
        SELECT 
            locality_id,
            year,
            month,
            AVG(latitude) AS latitude,  -- Assuming latitude is constant for each locality
            AVG(longitude) AS longitude, -- Assuming longitude is constant for each locality
            AVG(temperature_2m_max) AS avg_temperature,
            SUM(precipitation_hours) AS precipitation
        FROM read_csv_auto('{cleaned_csv_file_path}')
        GROUP BY locality_id, year, month
    )
    UPDATE public.weather
    SET 
        avg_temperature = agg.avg_temperature,
        precipitation = agg.precipitation
    FROM aggregated_data agg
    WHERE 
        public.weather.locality_id = agg.locality_id
        AND public.weather.year = agg.year
        AND public.weather.month = agg.month;
    """

    conn.execute(query)
    conn.close()


with DAG(
    "fetch_weather_data",
    default_args=default_args,
    description="Fetch and store weather data using OpenMeteo API",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["duckdb", "dbt", "weather"],
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_weather",
        python_callable=fetch_and_store_weather,
    )

    impute_missing_data_task = PythonOperator(
        task_id="impute_missing_data",
        python_callable=impute_missing_data,
    )

    group_and_insert_cleaned_weather_data_task = PythonOperator(
        task_id="group_and_insert_cleaned_weather_data",
        python_callable=group_and_insert_cleaned_weather_data,
    )

    (
        fetch_and_store_task
        >> impute_missing_data_task
        >> group_and_insert_cleaned_weather_data_task
    )

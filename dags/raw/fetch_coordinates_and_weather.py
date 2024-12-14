import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from datetime import datetime, timedelta
import time
from typing import List, Dict

from ratelimit import limits, sleep_and_retry
import backoff
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import duckdb
import os
import logging

from api.geocoding_api import send_geocoding_request
from api.weather_api import WeatherAPI

from dotenv import load_dotenv

# Rate limits
GEOCODE_CALLS_PER_MINUTE = 50
WEATHER_CALLS_PER_MINUTE = 35

load_dotenv()

weather_api_client = WeatherAPI()

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

duckdb_path: str = os.getenv("DUCKDB_FILE", "duckdb.db")
raw_data_path = os.getenv("RAW_DATA_PATH")


def create_raw_tables():
    create_coordinates_table = """
        CREATE TABLE IF NOT EXISTS raw.coordinates (
            locality TEXT NOT NULL,
            country TEXT NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(locality, country)
        );
    """

    create_weather_table = """
        CREATE TABLE IF NOT EXISTS raw.weather (
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            temperature DOUBLE,
            precipitation DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(latitude, longitude, year, month)
        );
    """

    con = duckdb.connect(duckdb_path)

    con.execute(create_coordinates_table)
    con.execute(create_weather_table)

    con.close()


def table_exists(conn, schema, table):
    """Check if a table exists in DuckDB"""
    try:
        conn.execute(f"""
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = '{table}'
        """).fetchone() is not None
        return True
    except:
        return False


def get_missing_localities():
    conn = duckdb.connect(duckdb_path, read_only=True)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    except Exception as e:
        pass  # XXX: ignore this, schema already exists

    # Check if the required tables exist
    if not table_exists(conn, "raw", "wfp"):
        # If table doesn't exist, return empty list or handle initialization
        logger.info("WFP table does not exist yet - skipping locality fetch")
        conn.close()
        return []

    sql = """
        SELECT DISTINCT adm1_name, adm0_name 
        FROM raw.wfp t
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.coordinates c
            WHERE c.locality = t.adm0_name
            AND c.country = t.adm1_name
        )
    """
    results = conn.execute(sql).fetchall()
    logger.info(f"Found {len(results)} missing localities")
    return [{"locality": row[0], "country": row[1]} for row in results]


@sleep_and_retry
@limits(calls=GEOCODE_CALLS_PER_MINUTE, period=60)
@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException), max_tries=3)
def geocode_location(locality: str, country: str) -> Dict:
    """Geocode with rate limiting and retries"""

    params = {"state": locality, "country": country, "format": "json"}
    data = send_geocoding_request(params)

    # XXX: workaround for missing locality (state)
    if not data:
        logger.info(
            f"No results for '{locality}', '{country}'. Retrying without state."
        )
        params = {"country": country, "format": "json"}
        data = send_geocoding_request(params)

    if not data:
        return {}  # XXX: no results

    lat, lng = data[0]["lat"], data[0]["lon"]
    return {"lat": lat, "lng": lng}


@sleep_and_retry
@limits(calls=WEATHER_CALLS_PER_MINUTE, period=60)
def get_weather_api_data(lat: float, lng: float, year: int, month: int) -> Dict:
    """Weather API call with rate limiting and retries"""

    result_df = weather_api_client.fetch_weather_data(
        lat,
        lng,
        year,
        month,
    )

    if result_df.empty:
        return {}

    result = result_df.iloc[0].to_dict()
    return {
        "temperature": result["temperature_2m_max"],
        "precipitation": result["precipitation_hours"],
    }


def batch_time_periods(time_periods: List[Dict], batch_size: int = 10):
    """Batch time periods for bulk processing"""
    return [
        time_periods[i : i + batch_size]
        for i in range(0, len(time_periods), batch_size)
    ]


def geocode_locality(locality: str, country: str):
    coords = geocode_location(locality, country)
    if not coords:
        return {}

    check_query = """
        SELECT COUNT(*) 
        FROM raw.coordinates 
        WHERE locality = ? AND country = ?
    """
    store_coords_query = """
        INSERT INTO raw.coordinates (locality, country, latitude, longitude)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.coordinates 
            WHERE locality = ? AND country = ?
        );
    """

    update_query = """
        UPDATE raw.coordinates 
        SET 
            latitude = ?,
            longitude = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE locality = ? AND country = ?
    """

    conn = duckdb.connect(duckdb_path, read_only=False)

    try:
        exists = conn.execute(check_query, [locality, country]).fetchone()[0] > 0

        if exists:
            conn.execute(
                update_query, [coords["lat"], coords["lng"], locality, country]
            )
        else:
            # Insert
            conn.execute(
                store_coords_query,
                [locality, country, coords["lat"], coords["lng"], locality, country],
            )

        return {
            "locality": locality,
            "country": country,
            "latitude": coords["lat"],
            "longitude": coords["lng"],
        }
    finally:
        conn.close()

        return {
            "locality": locality,
            "country": country,
            "latitude": coords["lat"],
            "longitude": coords["lng"],
        }


def fetch_location_weather(location: dict):
    """
    Fetch weather data for a location and store it in DuckDB.

    Args:
        location (dict): Dictionary containing location details (latitude, longitude, locality (name), country)
    """

    conn = duckdb.connect(duckdb_path)
    try:
        # Get all missing time periods for this location
        sql = """
            WITH existing_weather AS (
                SELECT DISTINCT year, month 
                FROM raw.weather
                WHERE latitude = ? AND longitude = ?
            )
            SELECT DISTINCT mp_year, mp_month
            FROM raw.wfp
            WHERE adm1_name = ?
            AND adm0_name = ?
            AND CAST(mp_year AS INTEGER) > 2016 -- XXX: we need this for the API
            AND NOT EXISTS (
                SELECT 1 FROM existing_weather 
                WHERE year = mp_year AND month = mp_month
            )
            ORDER BY mp_year, mp_month
        """

        params = [
            location["latitude"],
            location["longitude"],
            location["locality"],
            location["country"],
        ]

        # Execute query and fetch all results
        time_periods = conn.execute(sql, params).fetchall()
        # Convert to list of dicts for consistency
        time_periods = [{"year": row[0], "month": row[1]} for row in time_periods]

        # Process in batches
        batches = batch_time_periods(time_periods, batch_size=10)
        for batch in batches:
            weather_data = []
            for period in batch:
                try:
                    from utils.date_utils import get_start_end_dates

                    print(
                        get_start_end_dates(int(period["year"]), int(period["month"]))
                    )

                    # XXX: workound since we have missing historical data before 2017 in the api
                    year_to_use = (
                        2017 if int(period["year"]) < 2017 else int(period["year"])
                    )

                    data = get_weather_api_data(
                        lat=location["latitude"],
                        lng=location["longitude"],
                        year=year_to_use,
                        month=int(period["month"]),
                    )

                    weather_data = {
                        **data,
                        "latitude": location["latitude"],
                        "longitude": location["longitude"],
                        "year": int(period["year"]),
                        "month": int(period["month"]),
                    }
                except Exception as e:
                    logger.error(
                        f"Failed to fetch weather for {location} at {period}: {str(e)}"
                    )

                    continue

                if weather_data:
                    columns_to_use = [
                        "temperature",
                        "precipitation",
                        "latitude",
                        "longitude",
                        "year",
                        "month",
                    ]

                    weather_data_df = pd.DataFrame([weather_data])[columns_to_use]
                    weather_data_df.fillna(0, inplace=True)

                    logger.info(f"DF {weather_data_df}")
                    # Create a temporary table for the batch insert
                    # create_temp_table_sql = """
                    #     CREATE TEMPORARY TABLE IF NOT EXISTS temp_weather AS
                    #     SELECT * FROM raw.weather WHERE 1=0
                    # """
                    # Create and populate temp_weather table from DataFrame
                    conn.execute(
                        """
                        CREATE TEMPORARY TABLE IF NOT EXISTS temp_weather AS 
                        SELECT * FROM weather_data_df """
                    )

                    try:
                        # Move data to main table and handle duplicates
                        insert_sql = """
                            INSERT INTO raw.weather (latitude, longitude, year, month, temperature, precipitation)
                            SELECT latitude, longitude, year, month, temperature, precipitation FROM temp_weather 
                            WHERE NOT EXISTS (
                                SELECT 1 FROM raw.weather w 
                                WHERE w.latitude = temp_weather.latitude 
                                AND w.longitude = temp_weather.longitude
                                AND w.year = temp_weather.year 
                                AND w.month = temp_weather.month
                            )
                        """
                        conn.execute(insert_sql)

                    except Exception as e:
                        logger.info(
                            f"Failed to insert data into temp_weather: {str(e)}"
                        )

                # Clean up temporary table
                conn.execute("DROP TABLE IF EXISTS temp_weather")

    except Exception as e:
        logger.error(f"Failed to process location {location}: {str(e)}")
        raise

    finally:
        conn.close()


def get_localities_callable():
    logger.info("Fetching missing localities")
    return get_missing_localities()


def process_localities_callable():
    localities = (
        get_missing_localities()
    )  # This runs at parse time just to create the tasks
    for i in range(0, len(localities), 50):
        batch = localities[i : i + 50]
        for j, locality in enumerate(batch):
            process_locality(locality, i + j, len(localities))


def process_locality(locality, idx, total):
    logger.info(f"Processing locality {locality} ({idx+1}/{total})")
    coords = geocode_locality(locality["locality"], locality["country"])
    if not coords:
        return

    fetch_location_weather(coords)


with DAG(
    "raw_additional_tables",
    default_args=default_args,
    description="Create raw tables",
    schedule_interval="@daily",
    catchup=False,
    tags=["raw", "tables", "fetch", "weather"],
) as dag:
    create_tables = PythonOperator(
        task_id="create_raw_tables", python_callable=create_raw_tables
    )
    #
    get_localities = PythonOperator(
        task_id="get_localities", python_callable=get_localities_callable
    )

    process_localities = PythonOperator(
        task_id="process_localities", python_callable=process_localities_callable
    )

    # with TaskGroup("process_locations") as process_locations:
    #     localities = (
    #         get_missing_localities()
    #     )  # This runs at parse time just to create the tasks
    #     for i in range(0, len(localities), 50):
    #         batch = localities[i : i + 50]
    #         with TaskGroup(f"batch_{i//50}") as batch_group:
    #             for j, locality in enumerate(batch):
    #                 PythonOperator(
    #                     task_id=f"process_locality_{i+j}",
    #                     python_callable=process_locality,
    #                     op_kwargs={
    #                         "locality": locality,
    #                         "idx": i + j,
    #                         "total": len(localities),
    #                     },
    #                 )

    create_tables >> get_localities >> process_localities
    # process_localities

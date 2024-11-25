import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO)  # Set to INFO to reduce verbosity
logger = logging.getLogger("airflow.task")

DUCKDB_FILE = "/opt/airflow/db/analytics.duckdb"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
BATCH_SIZE = 200
CUSTOM_USER_AGENT = "MarketFlow/1.0 (contact@ut.ee)"
HEADERS = {"User-Agent": CUSTOM_USER_AGENT}

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def execute_duckdb_query(query, params=None):
    """Execute a query on the DuckDB database."""
    conn = duckdb.connect(DUCKDB_FILE)
    try:
        if params:
            result = conn.execute(query, params).fetchall()
        else:
            result = conn.execute(query).fetchall()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Failed to execute query: {query}, Error: {e}")
        conn.close()
        return None


def get_total_localities():
    """Get the total number of localities missing latitude and longitude."""
    sql = """
        SELECT COUNT(*)
        FROM public.localities
        WHERE latitude IS NULL OR longitude IS NULL;
    """
    result = execute_duckdb_query(sql)
    total = result[0][0] if result else 0
    logger.info(f"Total localities to process: {total}")
    return total


def get_locality_ids():
    """Retrieve all locality IDs that need latitude and longitude updates."""
    sql = """
        SELECT
            l.id,
            l.name,
            c.name AS country
        FROM public.localities l
        JOIN public.countries c ON l.country_id = c.id
        WHERE l.latitude IS NULL OR l.longitude IS NULL
        ORDER BY l.id;
    """
    records = execute_duckdb_query(sql)
    logger.info(f"Fetched {len(records)} locality IDs to process.")
    return records


def create_batches(ti):
    """Create batch lists based on the list of locality IDs and batch size."""
    localities = ti.xcom_pull(task_ids="get_locality_ids")
    if not localities:
        logger.info("No localities to process. Pushing empty batches.")
        ti.xcom_push(key="batches", value=[])
        return

    batches = [
        localities[i : i + BATCH_SIZE] for i in range(0, len(localities), BATCH_SIZE)
    ]
    logger.info(f"Created {len(batches)} batches with batch size {BATCH_SIZE}.")
    ti.xcom_push(key="batches", value=batches)


def send_request(url, params, headers):
    """Send a request to the Nominatim API and return the JSON response."""
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            return data if data else None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for parameters {params}: {e}")
    return None


def fetch_coordinates(localities):
    """Fetch coordinates for localities using the Nominatim API."""
    results = []
    for loc in localities:
        time.sleep(0.5)  # Respect API rate limits
        loc_id, city, country = loc
        params = {"state": city, "country": country, "format": "json"}
        data = send_request(NOMINATIM_URL, params, HEADERS)

        if not data:
            logger.info(
                f"No results for '{city}', '{country}'. Retrying without state."
            )
            params = {"country": country, "format": "json"}
            data = send_request(NOMINATIM_URL, params, HEADERS)

        if data:
            lat, lon = data[0]["lat"], data[0]["lon"]
            results.append((lat, lon, loc_id))
            logger.info(
                f"Fetched coordinates for ID {loc_id}: Latitude={lat}, Longitude={lon}"
            )
        else:
            logger.warning(
                f"Failed to fetch coordinates for ID {loc_id}: '{city}', '{country}'"
            )
    return results


def fetch_and_update(ti):
    """Fetch batches from XCom and process each batch."""
    batches = ti.xcom_pull(task_ids="create_batches", key="batches")
    if not batches:
        logger.info("No batches to process.")
        return

    for batch_index, batch in enumerate(batches, start=1):
        logger.info(
            f"Processing batch {batch_index}/{len(batches)} with {len(batch)} localities."
        )
        results = fetch_coordinates(batch)

        if results:
            update_query = """
                UPDATE public.localities
                SET latitude = ?, longitude = ?
                WHERE id = ?;
            """
            logger.info(f"Updating {update_query, results}")

            try:
                conn = duckdb.connect(DUCKDB_FILE, read_only=False)
                conn.executemany(update_query, results)

                conn.commit()

                conn.close()
                logger.info(
                    f"Updated {len(results)} localities with coordinates in batch {batch_index}."
                )
            except Exception as e:
                logger.error(f"Failed to update database in batch {batch_index}: {e}")


with DAG(
    "fetch_lat_long_from_nominatim_with_batching_duckdb",
    default_args=default_args,
    description="Fetch missing latitude and longitude for localities using Nominatim with DuckDB",
    schedule_interval=None,
    start_date=datetime(2024, 11, 1),
    tags=["duckdb", "fetch", "lat-long"],
    catchup=False,
) as dag:
    get_total_localities_task = PythonOperator(
        task_id="get_total_localities",
        python_callable=get_total_localities,
    )

    get_locality_ids_task = PythonOperator(
        task_id="get_locality_ids",
        python_callable=get_locality_ids,
    )

    create_batches_task = PythonOperator(
        task_id="create_batches",
        python_callable=create_batches,
        provide_context=True,
    )

    process_batch_task = PythonOperator(
        task_id="process_batches",
        python_callable=fetch_and_update,
        provide_context=True,
    )

    # Define task dependencies
    (
        get_total_localities_task
        >> get_locality_ids_task
        >> create_batches_task
        >> process_batch_task
    )

import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import http.client as http_client

http_client.HTTPConnection.debuglevel = 1
logging.basicConfig(level=logging.INFO)  # Set to INFO to reduce verbosity
logging.getLogger("http.client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.INFO)

DB_CONNECTION_ID = "market_flow"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

BATCH_SIZE = 100

CUSTOM_USER_AGENT = "MarketFlow/1.0 (contact@ut.ee)"

headers = {"User-Agent": CUSTOM_USER_AGENT}

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def send_request(url, params, headers):
    """
    Send a request to the Nominatim API and return the JSON response.
    """
    logger = logging.getLogger("airflow.task")
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        if response.status_code == 200:
            data = response.json()
            if data:
                return data
            logger.debug(f"No data found for parameters: {params}")
        else:
            logger.warning(
                f"Unexpected status code {response.status_code} for {params}"
            )

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for parameters {params}: {e}")

    return None


def fetch_coordinates(localities, nominatim_url, headers):
    """
    Fetch coordinates for localities using the Nominatim API, with fallback logic.
    """
    logger = logging.getLogger("airflow.task")
    results = []

    for loc in localities:
        time.sleep(0.5)  # Respect API rate limits
        id, city, country = loc

        # Validate fetched locality data
        if not id or not city or not country:
            logger.warning(f"Invalid locality data: {loc}")
            continue

        params = {"state": city, "country": country, "format": "json"}
        data = send_request(nominatim_url, params, headers)

        if not data:
            logger.info(
                f"No results for '{city}', '{country}'. Retrying without state."
            )
            params = {"country": country, "format": "json"}
            data = send_request(nominatim_url, params, headers)

        if data:
            lat, lon = data[0]["lat"], data[0]["lon"]
            try:
                results.append((lat, lon, id))
                logger.info(
                    f"Fetched coordinates for ID {id}: Latitude={lat}, Longitude={lon}"
                )
            except ValueError:
                logger.error(f"Invalid ID format: {id}")
        else:
            logger.warning(
                f"Failed to fetch coordinates for ID {id}: '{city}', '{country}'"
            )

    logger.info(f"Total results fetched: {len(results)}")
    return results


def get_total_localities():
    """
    Get the total number of localities missing latitude and longitude.
    """
    pg_hook = PostgresHook(postgres_conn_id=DB_CONNECTION_ID)
    sql = """
        SELECT COUNT(*)
        FROM localities
        WHERE latitude IS NULL OR longitude IS NULL;
    """
    result = pg_hook.get_first(sql)
    total = result[0] if result else 0
    logger = logging.getLogger("airflow.task")
    logger.info(f"Total localities to process: {total}")
    return total


def get_locality_ids():
    """
    Retrieve all locality IDs that need latitude and longitude updates.
    """
    pg_hook = PostgresHook(postgres_conn_id=DB_CONNECTION_ID)
    sql = """
        SELECT
            l.id,
            l.name,
            c.name AS country
        FROM localities l
        JOIN countries c ON l.country_id = c.id
        WHERE l.latitude IS NULL OR l.longitude IS NULL
        ORDER BY l.id;
    """
    records = pg_hook.get_records(sql)
    logger = logging.getLogger("airflow.task")
    logger.info(f"Fetched {len(records)} locality IDs to process.")
    return records


def create_batches(ti):
    """
    Create batch lists based on the list of locality IDs and batch size.
    """
    logger = logging.getLogger("airflow.task")
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


def fetch_and_update(ti):
    """
    Fetch batches from XCom and process each batch.
    """
    logger = logging.getLogger("airflow.task")
    batches = ti.xcom_pull(task_ids="create_batches", key="batches")
    logger.debug(f"Retrieved {len(batches)} batches from XCom.")

    if not batches:
        logger.info("No batches to process.")
        return

    for batch_index, batch in enumerate(batches, start=1):
        logger.info(
            f"Processing batch {batch_index}/{len(batches)} with {len(batch)} localities."
        )

        # Fetch coordinates for the current batch
        results = fetch_coordinates(batch, NOMINATIM_URL, headers)

        if results:
            pg_hook = PostgresHook(postgres_conn_id=DB_CONNECTION_ID)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            update_query = """
            UPDATE localities
            SET latitude = %s, longitude = %s
            WHERE id = %s;
            """

            try:
                cursor.executemany(update_query, results)  # Efficient batch updates
                connection.commit()
                logger.info(
                    f"Updated {len(results)} localities with coordinates in batch {batch_index}."
                )
            except Exception as e:
                connection.rollback()
                logger.error(f"Failed to update database in batch {batch_index}: {e}")
            finally:
                cursor.close()
                connection.close()
        else:
            logger.info(f"No coordinates fetched for batch {batch_index}.")


with DAG(
    "fetch_lat_long_from_nominatim_with_batching",
    default_args=default_args,
    description="Fetch missing latitude and longitude for localities using Nominatim with batching",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
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

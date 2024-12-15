from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import duckdb
import os
import glob
import logging

from utils.csv_utils import clean_csv_file

from dotenv import load_dotenv

load_dotenv()

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


def hdi(csv_file_path: str) -> bool:
    if csv_file_path.startswith("hdi"):
        return True
    return False


def wfp(csv_file_path: str) -> bool:
    if csv_file_path.startswith("wfp"):
        return True
    return False


def create_raw_additional_tables():
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

    create_currencies_historical = """
    CREATE TABLE IF NOT EXISTS raw.currencies_historical (
        currency_code VARCHAR,
        year INTEGER,
        month INTEGER,
        value DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    con = duckdb.connect(duckdb_path)

    con.execute(create_coordinates_table)
    con.execute(create_weather_table)
    con.execute(create_currencies_historical)

    con.close()


def table_exists(con, table_name: str) -> bool:
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw'
    """).fetchall()
    for (table,) in tables:
        if table == table_name:
            return True
    return False


def ingest_to_raw(**context):
    """
    Ingest CSV files to raw layer while preserving original data.
    Creates raw tables with metadata columns for tracking.
    """
    con = duckdb.connect(duckdb_path)

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    csv_directory = raw_data_path
    pattern = "*.csv"

    csv_files = glob.glob(os.path.join(csv_directory, pattern))

    for csv_file in csv_files:
        cleaned_file = clean_csv_file(csv_file)
        file_name = os.path.basename(csv_file)
        table_name = "wfp" if wfp(file_name) else "hdi"

        # Get file metadata
        file_stats = os.stat(cleaned_file)
        if not table_exists(con, table_name):
            # Create table using schema from first file
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{table_name} AS 
                SELECT 
                    *,
                    '{file_name}' as _filename,
                    '{datetime.fromtimestamp(file_stats.st_mtime)}' as _modified_at,
                    '{datetime.now()}' as _loaded_at,
                    '{os.path.abspath(csv_file)}' as _source_path
                FROM read_csv_auto('{cleaned_file}', 
                                 all_varchar=true,
                                 sample_size=-1)    -- Read all rows for type inference
                                 WHERE 1=0
            """)

        con.execute(f"""
            INSERT INTO raw.{table_name}
            SELECT 
                *,
                '{file_name}' as _filename,
                '{datetime.fromtimestamp(file_stats.st_mtime)}' as _modified_at,
                '{datetime.now()}' as _loaded_at,
                '{os.path.abspath(csv_file)}' as _source_path
            FROM read_csv_auto('{cleaned_file}', 
                             all_varchar=true,
                             sample_size=-1)    -- Read all rows for type inference
        """)

        # Move to archive while maintaining directory structure
        archive_dir = os.path.join(
            csv_directory, "archive", datetime.now().strftime("%Y/%m/%d")
        )
        os.makedirs(archive_dir, exist_ok=True)
        os.rename(csv_file, os.path.join(archive_dir, file_name))

        try:
            # Remove cleaned temporary file
            if os.path.exists(cleaned_file):
                os.remove(cleaned_file)
        except Exception as e:
            logger.error(f"Error removing temporary file: {e}")

    con.close()


def verify_raw_load(**context):
    """
    Verify that raw data was loaded correctly and log the results.
    """
    con = duckdb.connect(duckdb_path)

    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw' and table_name like 'hdi%' or table_name like 'wfp%'
    """).fetchall()

    for (table,) in tables:
        # Get row count and last load time
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as row_count,
                MAX(_loaded_at) as last_load
            FROM raw.{table}
        """).fetchone()

        print(f"Table raw.{table}: {stats[0]} rows loaded at {stats[1]}")

    con.close()


dag = DAG(
    "raw_data_ingestion",
    default_args=default_args,
    description="Ingest raw CSV files into DuckDB",
    schedule_interval="@daily",
    catchup=False,
    tags=["raw", "kaggle", "ingest"],
)

# File sensors
with TaskGroup(group_id="file_sensors", dag=dag) as file_sensor_group:
    for file_pattern in ["hdi_*.csv", "wfp_*.csv"]:
        FileSensor(
            task_id=f'sense_{file_pattern.replace("*", "all")}',
            filepath=f"{raw_data_path}/{file_pattern}",
            poke_interval=20,
            mode="poke",
            soft_fail=True,
        )

# Ingest to raw layer
ingest_raw = PythonOperator(
    task_id="ingest_to_raw",
    python_callable=ingest_to_raw,
    provide_context=True,
    dag=dag,
)

# Verify raw load
verify_raw = PythonOperator(
    task_id="verify_raw_load",
    python_callable=verify_raw_load,
    provide_context=True,
    dag=dag,
)

create_additional_tables = PythonOperator(
    task_id="create_raw_additional_tables", python_callable=create_raw_additional_tables
)

file_sensor_group >> ingest_raw >> verify_raw >> create_additional_tables

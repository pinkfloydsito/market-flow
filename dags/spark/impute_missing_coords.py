from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import numpy as np

import os

from dotenv import load_dotenv
import logging

logger = logging.getLogger("airflow.task")

load_dotenv()

duckdb_path = os.getenv("DUCKDB_FILE", "duckdb.db")


def impute_coordinates():
    conn = duckdb.connect(duckdb_path)

    try:
        # Get all coordinates data
        df = conn.execute("""
            SELECT locality, country, latitude, longitude
            FROM raw.coordinates
            ORDER BY country, locality
        """).df()

        # Calculate country averages for non-null coordinates
        country_averages = (
            df.groupby("country")
            .agg({"latitude": "mean", "longitude": "mean"})
            .reset_index()
        )

        missing_coords = df[df["latitude"].isna() | df["longitude"].isna()]

        for idx, row in missing_coords.iterrows():
            country_avg = country_averages[
                country_averages["country"] == row["country"]
            ].iloc[0]

            conn.execute(
                """
                UPDATE raw.coordinates 
                SET 
                    latitude = ?,
                    longitude = ?
                WHERE locality = ? AND country = ?
                AND (latitude IS NULL OR longitude IS NULL)
            """,
                [
                    float(country_avg["latitude"]),
                    float(country_avg["longitude"]),
                    row["locality"],
                    row["country"],
                ],
            )

            print(
                f"Imputed coordinates for {row['locality']}, {row['country']} using country averages"
            )
            print(f"Lat: {country_avg['latitude']}, Long: {country_avg['longitude']}")

        result = conn.execute("""
            SELECT country, 
                   COUNT(*) as total_locations,
                   SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as locations_with_coords
            FROM raw.coordinates
            GROUP BY country
            ORDER BY country
        """).df()

        print("\nSummary by country:")
        print(result)

    finally:
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
    "coordinates_imputation",
    default_args=default_args,
    description="Impute missing coordinates based on country averages",
    schedule_interval="@daily",  # or your preferred schedule
    catchup=False,
    tags=["coordinates", "imputation"],
)

impute_coordinates_task = PythonOperator(
    task_id="impute_coordinates", python_callable=impute_coordinates, dag=dag
)

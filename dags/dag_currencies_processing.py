# Commentaries inside the code explain the logic for the Airflow DAG.
# This DAG:
# 1. Extracts year/month/currency combos from duckdb
# 2. Fetches historical data from the API or reuse previously fetched (if year < 2000)
# 3. Writes all results to a CSV file
# 4. Reads from the CSV and updates each row in the duckdb table using `execute` (not executemany)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import os
import csv
from dotenv import load_dotenv
import currencyapicom

import logging

logger = logging.getLogger("airflow.task")
currency_historical_file_path = "/opt/airflow/db/currencies_historical.csv"

DUCKDB_FILE = "/opt/airflow/db/analytics.duckdb"


def extract_data(**context):
    # Extract distinct year/month and currency info
    db = duckdb.connect(DUCKDB_FILE)
    query = """
    SELECT c.name, cv.currency_value_id, cv.year, cv.month
    FROM public.currencies_values cv 
    JOIN public.currencies c ON cv.currency_id = c.id
    ORDER BY cv.year ASC, cv.month ASC
    """
    df = db.execute(query).df()
    db.close()

    context["ti"].xcom_push(key="currency_data", value=df.to_dict(orient="records"))


def get_currency_value_id_from_currency_value(code, year, month):
    db = duckdb.connect(DUCKDB_FILE)
    query = """
    SELECT c.name, cv.currency_value_id, cv.year, cv.month
    FROM public.currencies_values cv 
    JOIN public.currencies c ON cv.currency_id = c.id
    WHERE c.name = ? AND cv.year = ? AND cv.month = ?
    ORDER BY cv.year ASC, cv.month ASC limit 1
    """
    result = db.execute(query, (code, year, month)).fetchdf()
    db.close()

    if len(result) == 0:
        return None

    item = result.iloc[0]
    return item["currency_value_id"]


def fetch_and_write_csv(**context):
    load_dotenv()
    currency_api_key = os.getenv("CURRENCY_API_KEY")
    client = currencyapicom.Client(currency_api_key)

    rows = context["ti"].xcom_pull(key="currency_data", task_ids="extract_data")
    if not rows:
        return

    # Distinct year/month combinations
    year_month_set = set((r["year"], r["month"]) for r in rows)

    # We'll store results per (year,month) in a dictionary to avoid redundant API calls
    fetched_data = {}  # key: (year, month), value: dict of {currency_code: value}

    # The CSV we will write to
    output_file = currency_historical_file_path
    existing_combinations = set()

    try:
        with open(output_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                year_month = (row["year"], row["month"])
                existing_combinations.add(year_month)
    except FileNotFoundError:
        pass

    # Prepare CSV file with header
    with open(output_file, "a+", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        # Columns: currency_value_id, currency_code, year, month, value
        # writer.writerow(
        #     ["currency_value_id", "currency_code", "year", "month", "value"]
        # )

        for r in rows:
            orig_year = r["year"]
            month = r["month"]
            # If year < 2000, we try to fetch from 2000 or reuse fetched values
            fetch_year = orig_year if orig_year >= 2000 else 2000

            year_month = (str(orig_year), str(month))
            if year_month in existing_combinations:
                continue
            currency_value_id = r["currency_value_id"]

            logger.info(f"Fetching data for {year_month}...")
            logger.info(f"keys -> {fetched_data.keys()}")

            logger.info(f"debug {(fetch_year, month) not in fetched_data.keys()}")
            if (fetch_year, month) not in fetched_data.keys():
                # Need to fetch new data
                date = f"{fetch_year}-{month}-01"
                result = client.historical(date)
                currency_data = result["data"]

                data_in_tuples = []

                for code, payload in currency_data.items():
                    value = payload["value"]

                    curr_value_id = get_currency_value_id_from_currency_value(
                        code, orig_year, month
                    )
                    if curr_value_id is None:
                        continue

                    item = (
                        curr_value_id,
                        code,
                        orig_year,
                        month,
                        value,
                    )
                    data_in_tuples.append(item)

                fetched_data[(fetch_year, month)] = data_in_tuples

                for tuple_cleaned in data_in_tuples:
                    writer.writerow(tuple_cleaned)


def update_from_csv(**context):
    # Now we read from the csv and for each row do an update using execute.
    db = duckdb.connect(DUCKDB_FILE)
    input_file = currency_historical_file_path

    # We'll do row-by-row updates
    update_query = """
    UPDATE public.currencies_values
    SET curr_value = ?
    WHERE currency_value_id = ?;
    """
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        # Skip header
        next(reader)
        for row in reader:
            currency_value_id = int(row[0])
            value = None if row[4] == "None" else float(row[4])
            # Execute update per row
            db.execute(update_query, (value, currency_value_id))
    db.commit()
    db.close()


default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

with DAG(
    "currency_values_update_dag",
    default_args=default_args,
    schedule_interval="@once",
    tags=["duckdb", "dbt", "currency"],
) as dag:
    t1 = PythonOperator(task_id="extract_data", python_callable=extract_data)

    t2 = PythonOperator(
        task_id="fetch_and_write_csv",
        python_callable=fetch_and_write_csv,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="update_from_csv", python_callable=update_from_csv, provide_context=True
    )

    t1 >> t2 >> t3

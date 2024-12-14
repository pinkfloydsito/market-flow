from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
import duckdb
import pandas as pd


def create_spark_session():
    return (
        SparkSession.builder.appName("Currency Imputation")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def load_currency_data(spark):
    conn = duckdb.connect("/db/analysis.db")

    df_pandas = conn.execute("""
        SELECT *
        FROM raw.currencies_historical
    """).fetchdf()

    df_spark = spark.createDataFrame(df_pandas)

    conn.close()
    return df_spark


def write_back_to_table(df):
    # Convert Spark DataFrame back to pandas
    df_pandas = df.toPandas()

    # Connect to DuckDB
    conn = duckdb.connect("/db/analysis.db")  # Use same path as above

    try:
        # Create backup of original table (optional)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.currencies_historical_backup AS 
            SELECT * FROM raw.currencies_historical
        """)

        # Replace the original table with new data
        conn.execute("DROP TABLE IF EXISTS raw.currencies_historical")
        conn.execute(
            "CREATE TABLE raw.currencies_historical AS SELECT * FROM df_pandas"
        )

    finally:
        conn.close()


def prepare_data(df):
    """
    Prepare the data for imputation by:
    1. get the currency values
    2. ensure the data type is double
    """
    # Get all columns except date/timestamp
    numeric_columns = [
        col_name
        for col_name in df.columns
        if col_name.lower() not in ["date", "timestamp"]
    ]

    # Convert identified columns to double type
    for column in numeric_columns:
        df = df.withColumn(column, col(column).cast(DoubleType()))

    return df, numeric_columns


def impute_missing_values(df, numeric_columns):
    # Create an imputer for each currency column
    imputer = Imputer(
        inputCols=numeric_columns,
        outputCols=[f"{col}_imputed" for col in numeric_columns],
        strategy="mean",  # median can work well too
    )

    imputer_model = imputer.fit(df)
    imputed_df = imputer_model.transform(df)

    for col in numeric_columns:
        imputed_df = imputed_df.drop(col).withColumnRenamed(f"{col}_imputed", col)

    return imputed_df


def main():
    # Initialize Spark session
    spark = create_spark_session()

    try:
        # Load data
        df = load_currency_data(spark)

        prepared_df, numeric_columns = prepare_data(df)

        imputed_df = impute_missing_values(prepared_df, numeric_columns)

        write_back_to_table(imputed_df)

        print("Imputation complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

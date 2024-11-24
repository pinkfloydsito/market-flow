import duckdb
import os
import pandas as pd


class DuckDBConnector:
    """
    Handles connections and operations with DuckDB.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = self.connect_to_duckdb()

    def connect_to_duckdb(self):
        """
        Connect to DuckDB, creating the database file if it doesn't exist.
        """
        try:
            db_dir = os.path.dirname(self.db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
                print(f"Created directory: {db_dir}")

            con = duckdb.connect(self.db_path)
            print(f"Connected to DuckDB database at {self.db_path}")
            return con
        except Exception as e:
            print(f"Failed to connect to DuckDB: {e}")
            raise

    def execute_query(self, query: str, params: tuple = ()):
        """
        Execute a query without fetching results.
        """
        if not self.connection:
            raise ConnectionError("DuckDB is not connected.")
        self.connection.execute(query, params)

    def fetch_dataframe(self, query: str, params: tuple = ()):
        """
        Execute a query and return the results as a pandas DataFrame.
        """
        if not self.connection:
            raise ConnectionError("DuckDB is not connected.")
        return self.connection.execute(query, params).df()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Insert a pandas DataFrame into a DuckDB table.
        """
        if not self.connection:
            raise ConnectionError("DuckDB is not connected.")
        self.connection.execute(
            f"INSERT INTO {table_name} VALUES ?", [df.values.tolist()]
        )

    def close_connection(self):
        """
        Close the DuckDB connection.
        """
        if self.connection:
            self.connection.close()
            print("DuckDB connection closed.")

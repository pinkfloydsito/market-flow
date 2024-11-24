import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresConnector:
    """
    Wrapper for interacting with a Postgres database using Airflow's PostgresHook.
    """

    def __init__(self, postgres_conn_id: str):
        self.conn_id = postgres_conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def execute_query(self, query: str, params: tuple = ()):
        """
        Execute a query without returning results.
        """
        self.hook.run(query, parameters=params)

    def fetch_all(self, query: str, params: tuple = (), as_dataframe: bool = False):
        """
        Fetch all results for a query. Optionally return as a pandas DataFrame.
        """
        connection = self.hook.get_conn()
        cursor = connection.cursor()

        cursor.execute(query, params)

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        cursor.close()
        connection.close()

        if as_dataframe:
            return pd.DataFrame(rows, columns=columns)
        return rows

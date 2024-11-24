from collections import namedtuple
from db.connectors.postgres_connector import PostgresConnector

Locality = namedtuple("Locality", ["id", "name", "latitude", "longitude", "country"])


class LocalitiesRepository:
    """
    Repository for handling operations related to localities.
    """

    def __init__(self, connector: PostgresConnector):
        self.connector = connector

    def get_all_localities(self):
        """
        Fetch all localities with latitude and longitude from the database.
        """
        query = """
            SELECT id, name, latitude, longitude, country
            FROM localities
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """
        return self.connector.fetch_all(query, as_dataframe=True)

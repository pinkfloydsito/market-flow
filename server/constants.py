import os
from enum import Enum

from dotenv import load_dotenv

load_dotenv()


class CacheTTL(Enum):
    SHORT = 60  # 1 minute
    MEDIUM = 300  # 5 minutes
    LONG = 3600  # 1 hour


default_ml_csv_path = "/app/db/ml_data.csv"
CSV_PATH = os.getenv("ML_DATA_FILE", default_ml_csv_path)

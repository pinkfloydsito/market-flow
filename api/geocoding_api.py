import requests
import logging

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
BATCH_SIZE = 200
CUSTOM_USER_AGENT = "MarketFlow/1.0 (contact@ut.ee)"
HEADERS = {"User-Agent": CUSTOM_USER_AGENT}

logger = logging.getLogger("airflow.task")

url = NOMINATIM_URL
headers = HEADERS


def send_geocoding_request(params):
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

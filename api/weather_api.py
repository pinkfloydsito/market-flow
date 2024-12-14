import requests
import pandas as pd
from datetime import datetime
from utils.date_utils import get_start_end_dates

import openmeteo_requests

import requests_cache
from retry_requests import retry
import logging

logger = logging.getLogger("airflow.task")
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


class WeatherAPI:
    """
    Handles interactions with the Open-Meteo API to fetch weather data.
    """

    BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

    def fetch_weather_data(
        self,
        latitude: float,
        longitude: float,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        """
        Fetch weather data for given coordinates and date range.
        Returns a pandas DataFrame with aggregated monthly data.
        """
        start_date, end_date = get_start_end_dates(year, month)

        print(
            f"Fetching weather data for {latitude}, {longitude} in {year}-{month}, {start_date} to {end_date} {start_date.strftime("%Y-%m-%d")}, {end_date.strftime("%Y-%m-%d")}"
        )

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": "auto",
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_hours",
            ],
        }

        try:
            responses = openmeteo.weather_api(WeatherAPI.BASE_URL, params=params)
            response = responses[0]

            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_precicipitation_hours = daily.Variables(2).ValuesAsNumpy()

            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left",
                )
            }

            daily_data["temperature_2m_max"] = daily_temperature_2m_max
            daily_data["temperature_2m_min"] = daily_temperature_2m_min
            daily_data["precipitation_hours"] = daily_precicipitation_hours
            daily_data["month"] = month
            daily_data["longitude"] = longitude
            daily_data["latitude"] = latitude

            daily_df = pd.DataFrame(data=daily_data)

            return self.process_weather_data(daily_df)
        except requests.RequestException as e:
            print(f"API request failed: {e}")
            return pd.DataFrame()

    def process_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the raw API data into aggregated metrics.
        """
        try:
            columns_to_aggregate = [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_hours",
            ]

            monthly_avg_df = (
                df.groupby("month")[columns_to_aggregate].mean().reset_index()
            )

            monthly_avg_df["latitude"] = df["latitude"].iloc[0]
            monthly_avg_df["longitude"] = df["longitude"].iloc[0]

            return monthly_avg_df
        except Exception as e:
            print(f"Data processing failed: {e}")
            return pd.DataFrame()

from api.weather_api import WeatherAPI
from datetime import datetime

api = WeatherAPI()

latitude = 52.5244
longitude = 13.4105
start_date = datetime(2023, 1, 1)  # Start of January 2023
end_date = datetime(2023, 1, 31)  # End of January 2023
month = 1

weather_data = api.fetch_weather_data(latitude, longitude, start_date, end_date, month)

print(weather_data)

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2024-11-09",
    "end_date": "2024-11-22",
    "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_hours"],
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")

# Process hourly data. The order of variables needs to be the same as requested.
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

daily_dataframe = pd.DataFrame(data=daily_data)
print(daily_dataframe)

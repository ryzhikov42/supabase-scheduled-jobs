import os
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import time

# Setup caching and retry for requests
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Define the coordinates for multiple cities
cities = [
    #{"latitude": 56.0271, "longitude": 37.4679},
    {"latitude": 54.710128, "longitude": 20.5105838},
]

end_date = "2025-07-29"

# Define the URL and common parameters
url = "https://archive-api.open-meteo.com/v1/archive"
params_template = {
    "start_date": "1940-01-01",
    "end_date": end_date,
    "daily": ["sunrise", "sunset"],
    "hourly": ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "apparent_temperature",
               "precipitation", "rain", "showers", "snowfall", "snow_depth", "is_day", "sunshine_duration"],
    "timezone": "Europe/Moscow",
    "temporal_resolution": "hourly_3"
}

# Initialize an empty DataFrame to store all hourly data
all_hourly_data = pd.DataFrame()

# Loop over each city
for city in cities:
    params = params_template.copy()
    params["latitude"] = [city["latitude"]]
    params["longitude"] = [city["longitude"]]

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Extract hourly data
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(1).ValuesAsNumpy(),
        "wind_direction_10m": hourly.Variables(2).ValuesAsNumpy(),
        "apparent_temperature": hourly.Variables(3).ValuesAsNumpy(),
        "precipitation": hourly.Variables(4).ValuesAsNumpy(),
        "rain": hourly.Variables(5).ValuesAsNumpy(),
        "showers": hourly.Variables(6).ValuesAsNumpy(),
        "snowfall": hourly.Variables(7).ValuesAsNumpy(),
        "snow_depth": hourly.Variables(8).ValuesAsNumpy(),
        "is_day": hourly.Variables(9).ValuesAsNumpy(),
        "sunshine_duration": hourly.Variables(10).ValuesAsNumpy(),
        "latitude": city["latitude"],
        "longitude": city["longitude"]
    }

    # Append the hourly data to the DataFrame
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    all_hourly_data = pd.concat([all_hourly_data, hourly_dataframe], ignore_index=True)

    # Define the CSV file path
    csv_file = r"C:\Users\user1\Desktop\openmeteo\_supabase_lobnya\archive_open_meteo.csv"

    # Append data to the CSV file if it exists, otherwise create a new file
    if os.path.exists(csv_file):
        all_hourly_data.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        all_hourly_data.to_csv(csv_file, index=False)
    print('Город добавлен')
    print(city["latitude"],city["longitude"])    
    time.sleep(5)


print(f"Конец скрипта")
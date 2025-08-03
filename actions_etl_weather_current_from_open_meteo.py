import psycopg2
from dotenv import load_dotenv
import os
import openmeteo_requests
import requests_cache
from retry_requests import retry
import time
import pandas as pd
import numpy as np

load_dotenv()

# Настройка клиента API Open-Meteo с кэшированием и повторением при ошибке
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Параметры базы данных
DB_CONFIG = {
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
    "dbname": os.getenv("dbname")
}

# Определение координат для нескольких городов
cities = [
    {"latitude": 56.0104473, "longitude": 37.4670831},
    {"latitude": 54.710128, "longitude": 20.5105838},
    # Добавьте другие города по необходимости
]

# Определение URL и общих параметров
url = "https://api.open-meteo.com/v1/forecast"
params_template = {
    "hourly": ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "apparent_temperature",
               "precipitation", "rain", "showers", "snowfall", "snow_depth", "is_day", "sunshine_duration"],
    "timezone": "Europe/Moscow",
    "past_days": 7,  # Получение данных за последние 7 дней
    "forecast_days": 7,  # Прогноз на 2 дня вперед
    "temporal_resolution": "hourly_3"
}

BATCH_SIZE = 8000

def convert_numpy_types(value):
    """Конвертирует numpy типы в стандартные python типы"""
    if isinstance(value, (np.float32, np.float64)):
        return float(value)
    elif isinstance(value, (np.int32, np.int64)):
        return int(value)
    elif isinstance(value, np.bool_):
        return bool(value)
    return value

def process_batch(conn, batch, is_first_batch):
    with conn.cursor() as cursor:
        if is_first_batch:
            cursor.execute("TRUNCATE TABLE lbn.weather_BUFFER")
            conn.commit()

        args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in batch)
        cursor.execute(f"INSERT INTO lbn.weather_BUFFER VALUES {args_str}")

        cursor.execute("""
            INSERT INTO lbn.weather
            SELECT date,
                temperature_2m,
                wind_speed_10m,
                wind_direction_10m,
                apparent_temperature,
                precipitation,
                rain,
                showers,
                snowfall,
                snow_depth,
                CASE WHEN is_day = 1 THEN TRUE ELSE FALSE END,
                sunshine_duration,
                latitude,
                longitude,
                CURRENT_TIMESTAMP
            FROM lbn.weather_BUFFER
            ON CONFLICT (latitude, longitude, date) DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                wind_direction_10m = EXCLUDED.wind_direction_10m,
                apparent_temperature = EXCLUDED.apparent_temperature,
                precipitation = EXCLUDED.precipitation,
                rain = EXCLUDED.rain,
                showers = EXCLUDED.showers,
                snowfall = EXCLUDED.snowfall,
                snow_depth = EXCLUDED.snow_depth,
                is_day = EXCLUDED.is_day,
                sunshine_duration = EXCLUDED.sunshine_duration,
                date_update = CURRENT_TIMESTAMP
        """)
        return len(batch)

try:
    # Устанавливаем соединение с базой данных
    with psycopg2.connect(**DB_CONFIG) as conn:
        total_rows = 0
        
        # Цикл по каждому городу
        for city in cities:
            params = params_template.copy()
            params["latitude"] = city["latitude"]
            params["longitude"] = city["longitude"]

            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]

            # Извлечение почасовой информации
            hourly = response.Hourly()
            
            # Подготовка данных для базы
            batch = []
            dates = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
            
            is_first_batch_for_city = True
            for i in range(len(dates)):
                row = [
                    dates[i],
                    convert_numpy_types(hourly.Variables(0).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(1).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(2).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(3).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(4).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(5).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(6).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(7).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(8).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(9).ValuesAsNumpy()[i]),
                    convert_numpy_types(hourly.Variables(10).ValuesAsNumpy()[i]),
                    city["latitude"],
                    city["longitude"]
                ]
                
                # Замена NaN на None
                processed_row = [None if x is not None and pd.isna(x) else x for x in row]
                batch.append(processed_row)
                
                if len(batch) >= BATCH_SIZE:
                    processed = process_batch(conn, batch, is_first_batch_for_city)
                    is_first_batch_for_city = False
                    total_rows += processed
                    print(f"Обработано: {total_rows} строк")
                    batch = []
            
            if batch:
                processed = process_batch(conn, batch, is_first_batch_for_city)
                total_rows += processed
                print(f"Обработано: {total_rows} строк (финальный пакет)")
            
            print(f'Данные для города с координатами {city["latitude"]}, {city["longitude"]} добавлены')
            time.sleep(1)  # Пауза между запросами для разных городов

        print(f"Всего загружено строк: {total_rows}")

except Exception as e:
    print(f"Ошибка: {e}")
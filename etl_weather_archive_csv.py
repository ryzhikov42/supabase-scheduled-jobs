import psycopg2
import csv
from dotenv import load_dotenv
import os

load_dotenv()

CSV_FILE_PATH = r'C:\Users\user1\Desktop\openmeteo\_supabase_lobnya\archive_open_meteo.csv'
BATCH_SIZE = 8000

def process_batch(conn, batch):
    with conn.cursor() as cursor:
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
    with psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname")
    ) as conn:
        total_rows = 0

        with open(CSV_FILE_PATH, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Пропускаем заголовок

            batch = []
            for row in reader:
                # Замена пустых значений на None
                processed_row = [None if x == '' else x for x in row]

                # Преобразование значений в столбце is_day (индекс 9)
                if processed_row[9] == '1.0':
                    processed_row[9] = 1
                elif processed_row[9] == '0.0':
                    processed_row[9] = 0

                batch.append(processed_row)

                if len(batch) >= BATCH_SIZE:
                    processed = process_batch(conn, batch)
                    total_rows += processed
                    print(f"Обработано: {total_rows} строк")
                    batch = []

            if batch:
                processed = process_batch(conn, batch)
                total_rows += processed
                print(f"Обработано: {total_rows} строк (финальный пакет)")

        print(f"Всего загружено строк: {total_rows}")

except Exception as e:
    print(f"Ошибка: {e}")   
import requests
import json
import psycopg2
from datetime import datetime, timedelta
import time
import logging
import argparse
from dotenv import load_dotenv
import sys
import os

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

# Загрузка настроек
load_dotenv()

# Параметры подключения
DB_PARAMS = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port")
}

# Список городов
CITIES = [
    {"name": "Лобня", "region_id": "46", "district_id": "46440"},
    {"name": "Калининград", "region_id": "27", "district_id": "27401"}
]

def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка данных о ДТП за указанный период.")
    parser.add_argument("--start_year", type=int, help="Начальный год (по умолчанию: текущий год - 2 месяца)")
    parser.add_argument("--start_month", type=int, help="Начальный месяц (по умолчанию: текущий месяц - 2)")
    parser.add_argument("--end_year", type=int, help="Конечный год (по умолчанию: текущий год)")
    parser.add_argument("--end_month", type=int, help="Конечный месяц (по умолчанию: текущий месяц)")
    return parser.parse_args()

def get_date_range(args):
    now = datetime.now()
    default_end_year = now.year
    default_end_month = now.month

    # Вычисляем начальный месяц и год (5 месяцев назад)
    default_start_month = default_end_month - 5
    default_start_year = default_end_year

    if default_start_month <= 0:
        default_start_month += 12
        default_start_year -= 1

    start_year = args.start_year if args.start_year is not None else default_start_year
    start_month = args.start_month if args.start_month is not None else default_start_month
    end_year = args.end_year if args.end_year is not None else default_end_year
    end_month = args.end_month if args.end_month is not None else default_end_month

    return start_year, start_month, end_year, end_month

def main():
    args = parse_args()
    start_year, start_month, end_year, end_month = get_date_range(args)

    logger.info(f"Загрузка данных с {start_month}.{start_year} по {end_month}.{end_year}")

    # Подключение к БД
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Успешное подключение к БД")
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

    try:
        for city in CITIES:
            logger.info(f"Обработка города: {city['name']}")

            for year in range(start_year, end_year + 1):
                start_m = start_month if year == start_year else 1
                end_m = end_month if year == end_year else 12

                for month in range(start_m, end_m + 1):
                    logger.info(f"Загрузка данных за {month}.{year}...")

                    # Формируем запрос к API
                    payload = {
                        "data": json.dumps({
                            "date": [f"MONTHS:{month}.{year}"],
                            "ParReg": city["region_id"],
                            "order": {"type": "1", "fieldName": "dat"},
                            "reg": city["district_id"],
                            "ind": "1",
                            "st": "1",
                            "en": "1000",
                            "fil": {"isSummary": False},
                            "fieldNames": ["dat", "time", "coordinates", "infoDtp"]
                        }, separators=(',', ':'))
                    }

                    headers = {
                        "User-Agent": "Mozilla/5.0",
                        "Content-Type": "application/json"
                    }

                    # Запрос к API
                    try:
                        response = requests.post(
                            "http://stat.gibdd.ru/map/getDTPCardData",
                            json=payload,
                            headers=headers,
                            timeout=30
                        )

                        # Проверка ответа
                        if response.status_code != 200:
                            logger.warning(f"Ошибка HTTP: {response.status_code}")
                            continue

                        try:
                            response_json = response.json()
                            if "data" not in response_json:
                                logger.warning("Нет поля 'data' в ответе API")
                                continue

                            data = json.loads(response_json["data"]).get("tab", [])
                        except json.JSONDecodeError as e:
                            logger.warning(f"Невалидный JSON в ответе: {e}")
                            continue

                        if not data:
                            logger.info("Нет данных для загрузки")
                            continue

                        # Вставка в БД с новыми полями
                        try:
                            with conn.cursor() as cur:
                                for record in data:
                                    try:
                                        json_str = json.dumps(record, ensure_ascii=False)

                                        # Добавляем информацию о городе в INSERT
                                        cur.execute(
                                            """
                                            INSERT INTO lbn.dtp_BUFFER
                                            (city_name, region_id, district_id, raw_json)
                                            VALUES (%s, %s, %s, %s)
                                            """,
                                            (
                                                city["name"],
                                                city["region_id"],
                                                city["district_id"],
                                                json_str
                                            )
                                        )
                                    except psycopg2.Error as e:
                                        logger.error(f"Ошибка вставки записи: {e}")
                                        conn.rollback()
                                        raise

                                conn.commit()
                                logger.info(f"Успешно добавлено {len(data)} записей")

                        except psycopg2.InterfaceError:
                            logger.error("Разрыв соединения с БД. Переподключение...")
                            conn = psycopg2.connect(**DB_PARAMS)
                            continue

                    except requests.exceptions.RequestException as e:
                        logger.error(f"Ошибка запроса: {e}")
                        continue

                    time.sleep(1)  # Пауза между запросами

    except KeyboardInterrupt:
        logger.info("Скрипт остановлен вручную")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        if 'conn' in locals() and not conn.closed:
            conn.close()
            logger.info("Соединение с БД закрыто")

    logger.info("Работа скрипта завершена")

if __name__ == "__main__":
    main()

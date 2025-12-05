import json
import psycopg2
from psycopg2 import pool
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='dtp_processing.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

# Загрузка настроек
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port")
}

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%d.%m.%Y').date() if date_str else None
    except:
        return None

def parse_time(time_str):
    try:
        return datetime.strptime(time_str, '%H:%M').time() if time_str else None
    except:
        return None

def parse_int(value):
    try:
        return int(value) if value is not None else 0
    except:
        return 0

def parse_float(value):
    try:
        return float(str(value).replace(',', '.')) if value is not None else 0.0
    except:
        return 0.0

def main():
    connection_pool = None
    conn = None
    try:
        logger.info("=" * 60)
        logger.info("НАЧАЛО ОБРАБОТКИ ДТП")
        connection_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **DB_CONFIG)
        if not connection_pool:
            logger.error("Не удалось создать пул подключений!")
            return

        conn = connection_pool.getconn()
        if not conn:
            logger.error("Не удалось получить соединение из пула!")
            return

        conn.autocommit = False
        cur = conn.cursor()
        batch_size = 1000

        while True:
            cur.execute("""
                SELECT id, region_id, district_id, raw_json, city_name
                FROM lbn.dtp_buffer
                WHERE date_processing IS NULL
                AND is_error = FALSE
                ORDER BY id
                LIMIT %s
            """, (batch_size,))

            rows = cur.fetchall()
            if not rows:
                break

            logger.info(f"Найдено {len(rows)} записей для обработки")

            for row in rows:
                id, region_id, district_id, raw_json, city_name = row
                city_name = city_name if city_name else "Не указан"

                try:
                    if isinstance(raw_json, dict):
                        data_list = [raw_json]
                    else:
                        try:
                            data_list = json.loads(raw_json)
                            if not isinstance(data_list, (list, dict)):
                                logger.error(f"Некорректный формат JSON для {id}: {raw_json}")
                                cur.execute("""
                                    UPDATE lbn.dtp_buffer
                                    SET is_error = TRUE
                                    WHERE id = %s
                                """, (id,))
                                conn.commit()
                                continue
                            if isinstance(data_list, dict):
                                data_list = [data_list]
                        except json.JSONDecodeError:
                            logger.error(f"Ошибка парсинга JSON для {id}: {raw_json}")
                            cur.execute("""
                                UPDATE lbn.dtp_buffer
                                SET is_error = TRUE
                                WHERE id = %s
                            """, (id,))
                            conn.commit()
                            continue

                    for data in data_list:
                        if not isinstance(data, dict):
                            logger.error(f"Некорректный формат данных для {id}: {data}")
                            continue

                        kart_id = data.get('KartId')
                        if not kart_id:
                            logger.warning(f"Пропуск: нет KartId для {id}")
                            continue

                        info = data.get('infoDtp', {})

                        # Обработка dtp_main
                        dtp_date = parse_date(data.get('date'))
                        dtp_time = parse_time(data.get('Time'))
                        settlement = info.get('n_p', city_name)

                        cur.execute("DELETE FROM lbn.dtp_main WHERE kart_id = %s AND region_id = %s AND district_id = %s",
                                    (kart_id, region_id, district_id))

                        cur.execute("""
                            INSERT INTO lbn.dtp_main (
                                kart_id, region_id, district_id, row_num, dtp_date, dtp_time, district,
                                dtp_type, deaths, wounded, vehicles_count, participants_count, emtp_number,
                                settlement, street, house, road, km, m, road_category, road_class,
                                road_quality, weather, road_condition, lighting, dtp_severity, coord_w, coord_l
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                      %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            kart_id, region_id, district_id, parse_int(data.get('rowNum')), dtp_date, dtp_time,
                            data.get('District'), data.get('DTP_V'), parse_int(data.get('POG', 0)),
                            parse_int(data.get('RAN', 0)), parse_int(data.get('K_TS', 0)),
                            parse_int(data.get('K_UCH', 0)), data.get('emtp_number', ''), settlement,
                            info.get('street', ''), info.get('house', ''), info.get('dor', ''),
                            info.get('km', ''), info.get('m', ''), info.get('k_ul', ''),
                            info.get('dor_z', ''), info.get('s_pog', ''), ', '.join(info.get('s_pog', [''])),
                            info.get('osv', ''), info.get('change_org_motion', ''),
                            info.get('s_dtp', ''), parse_float(info.get('COORD_W', 0.0)),
                            parse_float(info.get('COORD_L', 0.0))
                        ))

                        # Обработка dtp_vehicles
                        cur.execute("DELETE FROM lbn.dtp_vehicles WHERE kart_id = %s AND region_id = %s AND district_id = %s",
                                    (kart_id, region_id, district_id))

                        vehicles = info.get('ts_info', [])
                        for vehicle in vehicles:
                            vehicle_num = vehicle.get('n_ts', '')
                            cur.execute("""
                                INSERT INTO lbn.dtp_vehicles (
                                    kart_id, region_id, district_id, vehicle_num, vehicle_status, vehicle_type,
                                    brand, model, color, drive_type, year, damage, tech_condition,
                                    ownership, owner_type, date_update
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            """, (
                                kart_id, region_id, district_id, vehicle_num, vehicle.get('ts_s', ''),
                                vehicle.get('t_ts', ''), vehicle.get('marka_ts', ''),
                                vehicle.get('m_ts', ''), vehicle.get('color', ''),
                                vehicle.get('r_rul', ''), str(vehicle.get('g_v', '')),
                                vehicle.get('m_pov', ''), vehicle.get('t_n', ''),
                                vehicle.get('f_sob', ''), vehicle.get('o_pf', '')
                            ))

                            # Обработка dtp_participants
                            cur.execute("""
                                DELETE FROM lbn.dtp_participants
                                WHERE kart_id = %s AND region_id = %s AND district_id = %s AND vehicle_num = %s
                            """, (kart_id, region_id, district_id, vehicle_num))

                            participants = vehicle.get('ts_uch', [])
                            for participant in participants:
                                violations = participant.get('NPDD', [])
                                violations_list = violations if isinstance(violations, list) else [violations]
                                cur.execute("""
                                    INSERT INTO lbn.dtp_participants (
                                        kart_id, region_id, district_id, vehicle_num, participant_type, violations,
                                        status, gender, age, alcohol, safety_belt, participant_num, seat_group,
                                        injured_card_id, hidden_status, date_update
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                """, (
                                    kart_id, region_id, district_id, vehicle_num, participant.get('K_UCH', ''),
                                    violations_list,
                                    participant.get('S_T', ''), participant.get('POL', ''),
                                    str(participant.get('V_ST', '')), participant.get('ALCO', ''),
                                    participant.get('SAFETY_BELT', ''), str(participant.get('N_UCH', '')),
                                    participant.get('S_SEAT_GROUP', ''), str(participant.get('INJURED_CARD_ID', '')),
                                    participant.get('S_SM', '')
                                ))

                        # Обработка dtp_factors
                        cur.execute("DELETE FROM lbn.dtp_factors WHERE kart_id = %s AND region_id = %s AND district_id = %s",
                                    (kart_id, region_id, district_id))

                        for factor_type in ['ndu', 'sdor']:
                            factors = info.get(factor_type, [])
                            for factor in factors:
                                factor_description = ', '.join(factor) if isinstance(factor, list) else str(factor)
                                cur.execute("""
                                    INSERT INTO lbn.dtp_factors (
                                        kart_id, region_id, district_id, factor_type, factor_description, date_update
                                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                """, (kart_id, region_id, district_id, factor_type, factor_description))

                        # Обработка dtp_objects
                        cur.execute("DELETE FROM lbn.dtp_objects WHERE kart_id = %s AND region_id = %s AND district_id = %s",
                                    (kart_id, region_id, district_id))

                        objects = info.get('OBJ_DTP', [])
                        for obj in objects:
                            obj_description = ', '.join(obj) if isinstance(obj, list) else str(obj)
                            cur.execute("""
                                INSERT INTO lbn.dtp_objects (
                                    kart_id, region_id, district_id, object_description, date_update
                                ) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            """, (kart_id, region_id, district_id, obj_description))

                    # Помечаем запись как обработанную
                    cur.execute("""
                        UPDATE lbn.dtp_buffer
                        SET date_processing = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (id,))

                    conn.commit()
                    logger.info(f"Обработана запись с id={id}")

                except Exception as e:
                    logger.error(f"Ошибка обработки записи с id={id}: {e}")
                    conn.rollback()
                    cur.execute("""
                        UPDATE lbn.dtp_buffer
                        SET is_error = TRUE
                        WHERE id = %s
                    """, (id,))
                    conn.commit()

        logger.info("=" * 60)
        logger.info("ОБРАБОТКА ВСЕХ ЗАПИСЕЙ ЗАВЕРШЕНА")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
    finally:
        if conn:
            connection_pool.putconn(conn)
        if connection_pool:
            connection_pool.closeall()

if __name__ == "__main__":
    main()

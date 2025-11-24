import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

def load_nbu_data():
    load_dotenv()

    # --- Завантаження даних з API ---
    url = os.getenv("NBU_API_URL")
    if not url:
        raise ValueError("❌ Не знайдено NBU_API_URL у .env файлі")

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        print("✅ Дані успішно отримано з API")
    except Exception as e:
        print("❌ Помилка при запиті до API:", e)
        return

    # --- Збереження у файл ---
    try:
        os.makedirs("data", exist_ok=True)  # створення папки для файлів

        today = datetime.now().strftime("%Y-%m-%d")
        file_name = f"data/currency_{today}.json"

        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"💾 Дані збережено у файл: {file_name}")

    except Exception as e:
        print("❌ Помилка при збереженні JSON:", e)
        return

    # --- Налаштування доступу до PostgreSQL ---
    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dbname": os.getenv("DB_NAME")
    }

    # Перевірка наявності всіх змінних
    if not all(db_config.values()):
        print("❌ Помилка: деякі DB_* змінні відсутні у .env")
        return

    # --- Підключення до PostgreSQL ---
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        print("🔌 Підключення до PostgreSQL успішне")
    except Exception as e:
        print("❌ Не вдалося підключитись до PostgreSQL:", e)
        return

    try:
        # --- Створення таблиці ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nbu_exchange_rate (
                id SERIAL PRIMARY KEY,
                digital_code INT NOT NULL,
                name_curr TEXT NOT NULL,
                rate NUMERIC NOT NULL,
                letter_code_curr TEXT NOT NULL,
                exchange_date DATE NOT NULL
            );
        """)
        conn.commit()
        print("📄 Таблиця nbu_exchange_rate готова")

        # --- Запис даних ---
        insert_query = """
            INSERT INTO nbu_exchange_rate
            (digital_code, name_curr, rate, letter_code_curr, exchange_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """

        for item in data:
            cur.execute(insert_query, (
                item.get("r030"),
                item.get("txt"),
                item.get("rate"),
                item.get("cc"),
                datetime.strptime(item.get("exchangedate"), "%d.%m.%Y").date()
            ))

        conn.commit()
        print("✅ Дані успішно збережено у PostgreSQL")

    except Exception as e:
        print("❌ Помилка при роботі з базою:", e)

    finally:
        # Закриття з'єднання
        cur.close()
        conn.close()
        print("🔒 З'єднання з PostgreSQL закрито")

load_nbu_data()
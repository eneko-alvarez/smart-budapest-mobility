import os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from python.extractors.weather_extractor import run as extract_open_meteo  # noqa

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv()

def insert_staging(record):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")),
        user=os.getenv("DB_USER","bi_user"),
        password=os.getenv("DB_PASSWORD","bi_password_secure"),
        dbname=os.getenv("DB_NAME","bi_budapest"),
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO staging.weather_raw (received_at, source, payload) VALUES (%s, %s, %s)",
            (record["received_at"], record["source"], Json(record["payload"]))
        )
    conn.close()

def run():
    rec = extract_open_meteo()
    print(f"[INFO] Extracted weather at {rec['received_at']}")
    insert_staging(rec)
    print("[INFO] Successfully inserted to staging.weather_raw")


if __name__ == "__main__":
    run()

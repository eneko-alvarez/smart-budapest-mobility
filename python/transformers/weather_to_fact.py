import os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def load_weather_fact():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")),
        user=os.getenv("DB_USER","bi_user"),
        password=os.getenv("DB_PASSWORD","bi_password_secure"),
        dbname=os.getenv("DB_NAME","bi_budapest"),
    )
    with conn, conn.cursor() as cur:
        # map weather_code to dim_weather_type if needed (placeholder)
        cur.execute("""
            INSERT INTO dwh.fact_weather_conditions (time_key, weather_key, temperature_c, humidity_pct, wind_speed_ms, precipitation_mm)
            SELECT
              dt.time_key,
              NULL::BIGINT, -- opcional: mapear a dim_weather_type
              rwo.temperature_c,
              rwo.humidity_pct,
              rwo.wind_speed_ms,
              rwo.precipitation_mm
            FROM raw.weather_observations rwo
            JOIN dwh.dim_time dt ON date_trunc('hour', rwo.recorded_at) = dt.ts
            WHERE rwo.recorded_at > now() - interval '2 days';
        """)
        print("[INFO] Successfully inserted to dwh.fact_weather_conditions")
    conn.close()
    

if __name__ == "__main__":
    load_weather_fact()

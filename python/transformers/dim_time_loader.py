import os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


import os
from datetime import datetime, timedelta, timezone
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def ensure_time_dimension(days_back=7, days_forward=2):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")),
        user=os.getenv("DB_USER","bi_user"),
        password=os.getenv("DB_PASSWORD","bi_password_secure"),
        dbname=os.getenv("DB_NAME","bi_budapest"),
    )
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).replace(minute=0, second=0, microsecond=0)
    end = (datetime.now(timezone.utc) + timedelta(days=days_forward)).replace(minute=0, second=0, microsecond=0)
    current = start
    with conn, conn.cursor() as cur:
        while current <= end:
            cur.execute("""
                INSERT INTO dwh.dim_time (ts, date, hour, day_of_week, week, month, year, is_weekend, is_holiday)
                VALUES (%s, %s, %s, %s, EXTRACT(WEEK FROM %s)::SMALLINT, %s, %s, %s, FALSE)
                ON CONFLICT (ts) DO NOTHING
            """, (
                current, current.date(), current.hour, current.weekday(),
                current, current.month, current.year, current.weekday()>=5
            ))
            current += timedelta(hours=1)
            print("[INFO] Successfully inserted to dwh.dim_time")
    conn.close()

if __name__ == "__main__":
    ensure_time_dimension()

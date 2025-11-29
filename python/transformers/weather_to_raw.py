import os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


import os
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv()

def upsert_weather_from_staging(batch_limit=200):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")),
        user=os.getenv("DB_USER","bi_user"),
        password=os.getenv("DB_PASSWORD","bi_password_secure"),
        dbname=os.getenv("DB_NAME","bi_budapest"),
    )
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, payload
            FROM staging.weather_raw
            WHERE received_at > now() - interval '2 days'
            ORDER BY received_at DESC
            LIMIT %s
        """, (batch_limit,))
        rows = cur.fetchall()
        inserted = 0
        for _id, payload in rows:
            try:
                current = payload.get("current_weather", {})
                hourly = payload.get("hourly", {})
                ts_iso = current.get("time")
                if ts_iso:
                    recorded_at = datetime.fromisoformat(ts_iso.replace('Z','+00:00'))
                else:
                    times = hourly.get("time", [])
                    recorded_at = datetime.fromisoformat(times[0].replace('Z','+00:00')) if times else datetime.now(timezone.utc)

                temperature = current.get("temperature") or (hourly.get("temperature_2m",[None])[0])
                wind_speed = current.get("windspeed") or (hourly.get("wind_speed_10m",[None])[0])
                precipitation = (hourly.get("precipitation",[None])[0])
                humidity = (hourly.get("relative_humidity_2m",[None])[0])
                weather_code = str(current.get("weathercode")) if current.get("weathercode") is not None else None

                cur.execute("""
                    INSERT INTO raw.weather_observations
                      (location_code, temperature_c, humidity_pct, wind_speed_ms, precipitation_mm, weather_code, recorded_at, source_system)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN_METEO')
                """, ('BUDAPEST', temperature, humidity, wind_speed, precipitation, weather_code, recorded_at))
                inserted += 1
            except Exception as e:
                pass
        return inserted
    conn.close()

if __name__ == "__main__":
    print("Inserted:", upsert_weather_from_staging())

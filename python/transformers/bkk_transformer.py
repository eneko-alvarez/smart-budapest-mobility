import os
import sys
from pathlib import Path
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
load_dotenv()

def transform_bkk_to_dwh():
    """Transforma vehículos BKK staging → dwh.fact_transport_usage"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )

    with conn, conn.cursor() as cur:
        # Insertar time_key (solo fecha + hora)
        cur.execute("""
            INSERT INTO dwh.dim_time (ts, date, hour, day_of_week, week, month, year, is_weekend, is_holiday)
            SELECT DISTINCT
                date_trunc('hour', to_timestamp(timestamp))::timestamp,
                date_trunc('day', to_timestamp(timestamp))::date,
                extract(hour from to_timestamp(timestamp))::int,
                extract(dow from to_timestamp(timestamp))::int,
                extract(week from to_timestamp(timestamp))::int,
                extract(month from to_timestamp(timestamp))::int,
                extract(year from to_timestamp(timestamp))::int,
                CASE WHEN extract(dow from to_timestamp(timestamp)) IN (0,6) THEN TRUE ELSE FALSE END,
                FALSE
            FROM staging.bkk_vehicles_raw
            WHERE timestamp IS NOT NULL
              AND timestamp > 1000000000
              AND to_timestamp(timestamp) > '2020-01-01'::timestamp
            ON CONFLICT (ts) DO NOTHING;
        """)
        time_rows = cur.rowcount
        print(f"✅ Inserted {time_rows} new time_key rows")

        # Transformar vehículos a fact
        cur.execute("""
            INSERT INTO dwh.fact_transport_usage (
                time_key, route_key, stop_key, trip_id, vehicle_id,
                delay_seconds, occupancy_pct, events_count
            )
            SELECT 
                t.time_key,
                COALESCE(r.route_key, 
                    (SELECT route_key FROM dwh.dim_route WHERE route_id = 'UNKNOWN' LIMIT 1)
                ) AS route_key,
                NULL AS stop_key,
                v.trip_id,
                v.vehicle_id,
                COALESCE(v.delay_seconds, 0) AS delay_seconds,
                NULL AS occupancy_pct,
                1 AS events_count
            FROM staging.bkk_vehicles_raw v
            JOIN dwh.dim_time t ON 
                t.date = v.recorded_at::date AND
                t.hour = EXTRACT(HOUR FROM v.recorded_at)
            LEFT JOIN dwh.dim_route r ON r.route_id = v.route_id
            WHERE NOT EXISTS (
                SELECT 1 FROM dwh.fact_transport_usage f2
                WHERE f2.vehicle_id = v.vehicle_id 
                  AND f2.time_key = t.time_key
            );
        """)
        inserted = cur.rowcount
        print(f"✅ Transformed {inserted} vehicle records to fact_transport_usage")

        cur.execute("TRUNCATE TABLE staging.bkk_vehicles_raw;")
        print(f"✅ Staging table cleaned")

    conn.close()

if __name__ == "__main__":
    print("Transforming BKK vehicles staging → DWH fact...")
    transform_bkk_to_dwh()
    print("✅ BKK transformation complete!")

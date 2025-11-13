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
        # Insertar time_key (timestamp en SEGUNDOS, no milisegundos)
        cur.execute("""
            INSERT INTO dwh.dim_time (ts, date, hour, day_of_week, month, year)
            SELECT DISTINCT 
                to_timestamp(timestamp),
                date_trunc('day', to_timestamp(timestamp))::date,
                extract(hour from to_timestamp(timestamp))::int,
                extract(dow from to_timestamp(timestamp))::int,
                extract(month from to_timestamp(timestamp))::int,
                extract(year from to_timestamp(timestamp))::int
            FROM staging.bkk_vehicles_raw
            WHERE timestamp IS NOT NULL 
                AND timestamp > 1000000000
                AND to_timestamp(timestamp) > '2020-01-01'::timestamp
                AND NOT EXISTS (
                    SELECT 1 FROM dwh.dim_time dt
                    WHERE dt.date = date_trunc('day', to_timestamp(bkk_vehicles_raw.timestamp))::date
                      AND dt.hour = extract(hour from to_timestamp(bkk_vehicles_raw.timestamp))::int
                );
        """)
        time_rows = cur.rowcount
        print(f"✅ Inserted {time_rows} new time_key rows")
        
        # Transformar vehículos a fact
        cur.execute("""
            INSERT INTO dwh.fact_transport_usage (
                time_key,
                route_key, 
                trip_id,
                vehicle_id,
                delay_seconds,
                occupancy_pct,
                events_count
            )
            SELECT 
                dt.time_key,
                dr.route_key,
                v.trip_id,
                v.vehicle_id,
                v.delay_seconds,
                0,
                1
            FROM staging.bkk_vehicles_raw v
            LEFT JOIN dwh.dim_route dr ON 
                dr.route_id = REGEXP_REPLACE(v.route_id, '^(BKK_|volan_)', '', 'i')
            LEFT JOIN dwh.dim_time dt ON 
                dt.date = date_trunc('day', to_timestamp(v.timestamp))::date
                AND dt.hour = extract(hour from to_timestamp(v.timestamp))::int
            WHERE v.route_id IS NOT NULL 
                AND v.route_id != ''
                AND v.timestamp > 1000000000
                AND dr.route_key IS NOT NULL
                AND dt.time_key IS NOT NULL;
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

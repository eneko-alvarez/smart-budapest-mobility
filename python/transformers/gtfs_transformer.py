import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

load_dotenv()

def transform_gtfs_to_dwh():
    """Transforma GTFS staging → dwh.dim_route y dwh.dim_stop"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    with conn, conn.cursor() as cur:
        # Cargar dim_route
        cur.execute("""
            INSERT INTO dwh.dim_route (route_id, route_name, agency_id, route_type, valid_from)
            SELECT 
                route_id,
                COALESCE(NULLIF(route_long_name, ''), route_short_name, 'Unknown'),
                COALESCE(NULLIF(agency_id, ''), 'BKK'),
                route_type,
                CURRENT_DATE
            FROM staging.gtfs_routes_raw
            ON CONFLICT (route_id) DO UPDATE SET
                route_name = EXCLUDED.route_name,
                agency_id = EXCLUDED.agency_id,
                route_type = EXCLUDED.route_type;
        """)
        routes_count = cur.rowcount
        print(f"✅ Routes transformed: {routes_count}")
        
        # Cargar dim_stop
        cur.execute("""
            INSERT INTO dwh.dim_stop (stop_id, stop_name, latitude, longitude)
            SELECT 
                stop_id,
                stop_name,
                CASE 
                    WHEN stop_lat ~ '^[0-9.\\-]+$' THEN stop_lat::numeric
                    ELSE 0
                END,
                CASE 
                    WHEN stop_lon ~ '^[0-9.\\-]+$' THEN stop_lon::numeric
                    ELSE 0
                END
            FROM staging.gtfs_stops_raw
            WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
            ON CONFLICT (stop_id) DO UPDATE SET
                stop_name = EXCLUDED.stop_name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;
        """)
        stops_count = cur.rowcount
        print(f"✅ Stops transformed: {stops_count}")
        
        print(f"\n✅ TOTAL: {routes_count} routes, {stops_count} stops loaded to DWH")
    
    conn.close()

if __name__ == "__main__":
    print("Transforming GTFS staging → DWH...")
    transform_gtfs_to_dwh()
    print("✅ GTFS transformation complete!")

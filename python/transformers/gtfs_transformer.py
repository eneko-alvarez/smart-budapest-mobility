import os, sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
load_dotenv()

def transform_gtfs_to_dwh():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    with conn, conn.cursor() as cur:
        # Transform routes from JSONB staging → dwh.dim_route
        cur.execute("""
            INSERT INTO dwh.dim_route (route_id, route_name, agency_id, route_type, valid_from)
            SELECT DISTINCT
                payload->>'route_id',
                COALESCE(payload->>'route_long_name', payload->>'route_short_name'),
                payload->>'agency_id',
                (payload->>'route_type')::int,
                CURRENT_DATE
            FROM staging.gtfs_routes_raw
            WHERE payload->>'route_id' IS NOT NULL
            ON CONFLICT (route_id) DO UPDATE SET
                route_name = EXCLUDED.route_name,
                route_type = EXCLUDED.route_type;
        """)
        routes = cur.rowcount
        print(f"✅ Transformed {routes} routes")
        
        # Transform stops from JSONB staging → dwh.dim_stop
        cur.execute("""
            INSERT INTO dwh.dim_stop (stop_id, stop_name, latitude, longitude, zone)
            SELECT DISTINCT
                payload->>'stop_id',
                payload->>'stop_name',
                (payload->>'stop_lat')::numeric,
                (payload->>'stop_lon')::numeric,
                payload->>'zone_id'
            FROM staging.gtfs_stops_raw
            WHERE payload->>'stop_id' IS NOT NULL
                AND COALESCE(payload->>'location_type', '') IN ('', '0')
            ON CONFLICT (stop_id) DO UPDATE SET
                stop_name = EXCLUDED.stop_name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                zone = EXCLUDED.zone;
        """)
        stops = cur.rowcount
        print(f"✅ Transformed {stops} stops")
    
    conn.close()

if __name__ == "__main__":
    print("Transforming GTFS staging → DWH...")
    transform_gtfs_to_dwh()
    print("✅ GTFS transformation complete!")

import os
import sys
from pathlib import Path
import psycopg2
import csv
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

load_dotenv()

def load_gtfs_routes():
    """Carga routes.txt en staging.gtfs_routes_raw"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    gtfs_path = ROOT / "data" / "gtfs" / "routes.txt"
    
    with conn, conn.cursor() as cur, open(gtfs_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows_inserted = 0
        
        for row in reader:
            cur.execute("""
                INSERT INTO staging.gtfs_routes_raw (route_id, agency_id, route_short_name, route_long_name, route_type, route_color, route_text_color)
                VALUES (%(route_id)s, %(agency_id)s, %(route_short_name)s, %(route_long_name)s, %(route_type)s, %(route_color)s, %(route_text_color)s)
                ON CONFLICT (route_id) DO UPDATE SET
                    route_short_name = EXCLUDED.route_short_name,
                    route_long_name = EXCLUDED.route_long_name,
                    route_type = EXCLUDED.route_type,
                    route_color = EXCLUDED.route_color,
                    loaded_at = now();
            """, {
                "route_id": row.get("route_id", ""),
                "agency_id": row.get("agency_id", ""),
                "route_short_name": row.get("route_short_name", ""),
                "route_long_name": row.get("route_long_name", ""),
                "route_type": row.get("route_type", ""),
                "route_color": row.get("route_color", ""),
                "route_text_color": row.get("route_text_color", "")
            })
            rows_inserted += 1
        
        print(f"✅ GTFS routes loaded: {rows_inserted} rows")
    
    conn.close()

def load_gtfs_stops():
    """Carga stops.txt en staging.gtfs_stops_raw"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    gtfs_path = ROOT / "data" / "gtfs" / "stops.txt"
    
    with conn, conn.cursor() as cur, open(gtfs_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows_inserted = 0
        
        for row in reader:
            cur.execute("""
                INSERT INTO staging.gtfs_stops_raw (stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, wheelchair_boarding)
                VALUES (%(stop_id)s, %(stop_name)s, %(stop_lat)s, %(stop_lon)s, %(location_type)s, %(parent_station)s, %(wheelchair_boarding)s)
                ON CONFLICT (stop_id) DO UPDATE SET
                    stop_name = EXCLUDED.stop_name,
                    stop_lat = EXCLUDED.stop_lat,
                    stop_lon = EXCLUDED.stop_lon,
                    loaded_at = now();
            """, {
                "stop_id": row.get("stop_id", ""),
                "stop_name": row.get("stop_name", ""),
                "stop_lat": row.get("stop_lat", "0"),
                "stop_lon": row.get("stop_lon", "0"),
                "location_type": row.get("location_type", "0"),
                "parent_station": row.get("parent_station", ""),
                "wheelchair_boarding": row.get("wheelchair_boarding", "")
            })
            rows_inserted += 1
        
        print(f"✅ GTFS stops loaded: {rows_inserted} rows")
    
    conn.close()

def load_gtfs_trips():
    """Carga trips.txt en staging.gtfs_trips_raw"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    gtfs_path = ROOT / "data" / "gtfs" / "trips.txt"
    
    with conn, conn.cursor() as cur, open(gtfs_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows_inserted = 0
        
        for row in reader:
            cur.execute("""
                INSERT INTO staging.gtfs_trips_raw (trip_id, route_id, service_id, trip_headsign, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed)
                VALUES (%(trip_id)s, %(route_id)s, %(service_id)s, %(trip_headsign)s, %(direction_id)s, %(block_id)s, %(shape_id)s, %(wheelchair_accessible)s, %(bikes_allowed)s)
                ON CONFLICT (trip_id) DO UPDATE SET
                    route_id = EXCLUDED.route_id,
                    service_id = EXCLUDED.service_id,
                    loaded_at = now();
            """, {
                "trip_id": row.get("trip_id", ""),
                "route_id": row.get("route_id", ""),
                "service_id": row.get("service_id", ""),
                "trip_headsign": row.get("trip_headsign", ""),
                "direction_id": row.get("direction_id", ""),
                "block_id": row.get("block_id", ""),
                "shape_id": row.get("shape_id", ""),
                "wheelchair_accessible": row.get("wheelchair_accessible", ""),
                "bikes_allowed": row.get("bikes_allowed", "")
            })
            rows_inserted += 1
        
        print(f"✅ GTFS trips loaded: {rows_inserted} rows")
    
    conn.close()

if __name__ == "__main__":
    print("Loading GTFS data into staging...")
    load_gtfs_routes()
    load_gtfs_stops()
    load_gtfs_trips()
    print("✅ GTFS load complete!")

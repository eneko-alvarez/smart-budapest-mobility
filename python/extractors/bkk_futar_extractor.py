import os
import sys
from pathlib import Path
import psycopg2
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

load_dotenv()

def extract_vehicles():
    """Extrae posiciones de vehículos en tiempo real de BKK Futár"""
    
    url = "https://futar.bkk.hu/api/query/v1/ws/otp/api/where/vehicles-for-location.json"
    params = {
        "lat": 47.5,
        "lon": 19.05,
        "radius": 10000,
        "key": os.getenv("BKK_API_KEY")
    }
    
    print(f"Fetching vehicles from BKK Futár API...")
    response = requests.get(url, params=params, timeout=15)
    
    if response.status_code != 200:
        print(f"❌ API Error {response.status_code}: {response.text}")
        return
    
    data = response.json()
    vehicles = data.get('data', {}).get('list', [])
    print(f"✅ Fetched {len(vehicles)} vehicles")
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.bkk_vehicles_raw (
                id SERIAL PRIMARY KEY,
                vehicle_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                latitude NUMERIC(9,6),
                longitude NUMERIC(9,6),
                bearing NUMERIC(5,2),
                speed NUMERIC(5,2),
                delay_seconds INT,
                timestamp BIGINT,
                recorded_at TIMESTAMPTZ DEFAULT now(),
                raw_data JSONB
            );
        """)
        conn.commit()
    
    with conn, conn.cursor() as cur:
        inserted = 0
        for vehicle in vehicles:
            try:
                vehicle_id = vehicle.get('vehicleId', '')
                trip_id = vehicle.get('tripId', '')
                route_id = vehicle.get('routeId', '')
                lat = vehicle.get('location', {}).get('lat', 0)
                lon = vehicle.get('location', {}).get('lon', 0)
                bearing = vehicle.get('bearing', 0)
                speed = vehicle.get('speed', 0)
                
                # CAMBIO CLAVE: usar lastUpdateTime en lugar de timestamp
                timestamp = vehicle.get('lastUpdateTime', 0)
                
                # Delay puede no estar presente
                delay = 0
                
                cur.execute("""
                    INSERT INTO staging.bkk_vehicles_raw 
                    (vehicle_id, trip_id, route_id, latitude, longitude, bearing, speed, delay_seconds, timestamp, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (vehicle_id, trip_id, route_id, lat, lon, bearing, speed, delay, timestamp, json.dumps(vehicle)))
                inserted += 1
            except Exception as e:
                print(f"⚠️ Error inserting vehicle: {e}")
                continue
        
        print(f"✅ Inserted {inserted} vehicles into staging.bkk_vehicles_raw")
    
    conn.close()

if __name__ == "__main__":
    extract_vehicles()

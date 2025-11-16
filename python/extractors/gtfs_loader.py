import os, sys, json, zipfile, csv, requests
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
load_dotenv()

GTFS_URL = "https://opendata.bkk.hu/gtfs/budapest_gtfs.zip"
DATA_DIR = ROOT / "data" / "gtfs"
ZIP_PATH = DATA_DIR / "budapest_gtfs.zip"

def download_gtfs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if ZIP_PATH.exists():
        print(f"✅ Using existing {ZIP_PATH}")
        with zipfile.ZipFile(ZIP_PATH, 'r') as z:
            z.extractall(DATA_DIR)
        return
    
    print(f"Downloading GTFS from {GTFS_URL}...")
    r = requests.get(GTFS_URL, stream=True, timeout=30)
    r.raise_for_status()
    
    with open(ZIP_PATH, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"✅ Downloaded to {ZIP_PATH}")
    
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        z.extractall(DATA_DIR)

def load_gtfs_to_staging():
    download_gtfs()
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    with conn, conn.cursor() as cur:
        # Routes
        routes_file = DATA_DIR / "routes.txt"
        if routes_file.exists():
            count = 0
            with open(routes_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cur.execute(
                        "INSERT INTO staging.gtfs_routes_raw (payload) VALUES (%s)",
                        (json.dumps(dict(row)),)
                    )
                    count += 1
            print(f"✅ Loaded {count} routes")
        
        # Stops
        stops_file = DATA_DIR / "stops.txt"
        if stops_file.exists():
            count = 0
            with open(stops_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cur.execute(
                        "INSERT INTO staging.gtfs_stops_raw (payload) VALUES (%s)",
                        (json.dumps(dict(row)),)
                    )
                    count += 1
            print(f"✅ Loaded {count} stops")
    
    conn.close()

if __name__ == "__main__":
    print("Loading GTFS data...")
    load_gtfs_to_staging()
    print("✅ GTFS complete!")


import os, json
from datetime import datetime, timezone
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_bkk():
    api_key = os.getenv("BKK_API_KEY", "")
    base = os.getenv("BKK_API_BASE_URL", "https://api.bkk.hu/efs/api/v1")
    url = f"{base}/routerinfo"
    headers = {"apikey": api_key} if api_key else {}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        return {"service": "BKK", "status": r.status_code, "ok": r.ok}
    except Exception as e:
        return {"service": "BKK", "status": "ERROR", "error": str(e)}

def check_open_meteo():
    lat, lon = 47.4979, 19.0402
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&current_weather=true"
    try:
        r = requests.get(url, timeout=10)
        return {"service": "Open-Meteo", "status": r.status_code, "ok": r.ok}
    except Exception as e:
        return {"service": "Open-Meteo", "status": "ERROR", "error": str(e)}

def check_postgres():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST","localhost"),
            port=int(os.getenv("DB_PORT","5432")),
            user=os.getenv("DB_USER","bi_user"),
            password=os.getenv("DB_PASSWORD","bi_password_secure"),
            dbname=os.getenv("DB_NAME","bi_budapest"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT now();")
            ts = cur.fetchone()[0]
        conn.close()
        return {"service": "PostgreSQL", "status": 200, "ok": True, "now": ts.isoformat()}
    except Exception as e:
        return {"service": "PostgreSQL", "status": "ERROR", "error": str(e)}

if __name__ == "__main__":
    results = [check_bkk(), check_open_meteo(), check_postgres()]
    print(json.dumps({"checked_at": datetime.now(timezone.utc).isoformat(), "results": results}, indent=2, default=str))

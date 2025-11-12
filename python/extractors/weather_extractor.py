import os, json, time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()
LAT, LON = 47.4979, 19.0402

def fetch_open_meteo():
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&current_weather=true&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
        f"&timezone=UTC"
    )
    r = requests.get(url, timeout=8)
    r.raise_for_status()
    return r.json()

def run():
    payload = fetch_open_meteo()
    return {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "source": "OPEN_METEO",
        "payload": payload
    }

if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

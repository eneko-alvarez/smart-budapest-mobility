import os, sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

load_dotenv()

def calculate_and_load_kpi_daily(days_back=7):
    """
    Calcula KPIs diarios de clima agregando fact_weather_conditions por fecha.
    Inserta una fila por cada métrica (avg_temp_day, total_precip_day, wind_speed_avg_day).
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )
    
    with conn, conn.cursor() as cur:
        # Calcula avg_temp_day
        cur.execute("""
            INSERT INTO dwh.kpi_daily (kpi_date, metric_name, metric_value)
            SELECT 
                dt.date,
                'avg_temp_day',
                AVG(fw.temperature_c)
            FROM dwh.fact_weather_conditions fw
            JOIN dwh.dim_time dt ON fw.time_key = dt.time_key
            WHERE dt.date >= current_date - interval '%s days'
            GROUP BY dt.date
            ON CONFLICT (kpi_date, route_key, stop_key, metric_name)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                computed_at = now();
        """, (days_back,))
        
        # Calcula total_precip_day
        cur.execute("""
            INSERT INTO dwh.kpi_daily (kpi_date, metric_name, metric_value)
            SELECT 
                dt.date,
                'total_precip_day',
                SUM(fw.precipitation_mm)
            FROM dwh.fact_weather_conditions fw
            JOIN dwh.dim_time dt ON fw.time_key = dt.time_key
            WHERE dt.date >= current_date - interval '%s days'
            GROUP BY dt.date
            ON CONFLICT (kpi_date, route_key, stop_key, metric_name)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                computed_at = now();
        """, (days_back,))
        
        # Calcula wind_speed_avg_day
        cur.execute("""
            INSERT INTO dwh.kpi_daily (kpi_date, metric_name, metric_value)
            SELECT 
                dt.date,
                'wind_speed_avg_day',
                AVG(fw.wind_speed_ms)
            FROM dwh.fact_weather_conditions fw
            JOIN dwh.dim_time dt ON fw.time_key = dt.time_key
            WHERE dt.date >= current_date - interval '%s days'
            GROUP BY dt.date
            ON CONFLICT (kpi_date, route_key, stop_key, metric_name)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                computed_at = now();
        """, (days_back,))
        
        print(f"[INFO] KPI daily: weather metrics processed for last {days_back} days")
    
    conn.close()

if __name__ == "__main__":
    calculate_and_load_kpi_daily()

import os
import sys
from pathlib import Path
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
load_dotenv()

def transform_daily_kpis():
    """Agrega KPIs diarios por ruta desde fact_transport_usage → fact_route_performance"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "bi_postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )

    with conn, conn.cursor() as cur:
        # Calcular KPIs del día anterior
        cur.execute("""
            INSERT INTO dwh.fact_route_performance (
                time_key, route_key, total_trips, unique_vehicles,
                avg_delay_seconds, total_delay_seconds
            )
            SELECT 
                -- time_key del día (hora 0)
                t_day.time_key,
                f.route_key,
                COUNT(*) AS total_trips,
                COUNT(DISTINCT f.vehicle_id) AS unique_vehicles,
                AVG(f.delay_seconds)::int AS avg_delay_seconds,
                SUM(f.delay_seconds) AS total_delay_seconds
            FROM dwh.fact_transport_usage f
            JOIN dwh.dim_time t ON f.time_key = t.time_key
            JOIN dwh.dim_time t_day ON 
                t_day.date = t.date AND 
                t_day.hour = 0
            WHERE t.date = CURRENT_DATE - INTERVAL '1 day'
              AND f.route_key IS NOT NULL
            GROUP BY t_day.time_key, f.route_key
            ON CONFLICT (time_key, route_key) DO UPDATE SET
                total_trips = EXCLUDED.total_trips,
                unique_vehicles = EXCLUDED.unique_vehicles,
                avg_delay_seconds = EXCLUDED.avg_delay_seconds,
                total_delay_seconds = EXCLUDED.total_delay_seconds;
        """)
        inserted = cur.rowcount
        print(f"✅ Aggregated {inserted} route KPIs for yesterday")

        # Verificar total
        cur.execute("SELECT COUNT(*) FROM dwh.fact_route_performance;")
        total = cur.fetchone()[0]
        print(f"📊 Total route performance records: {total}")

    conn.close()

if __name__ == "__main__":
    print("Calculating daily KPIs...")
    transform_daily_kpis()
    print("✅ Daily KPI transformation complete!")

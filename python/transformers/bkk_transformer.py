import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
load_dotenv()


def transform_bkk_to_dwh():
    """Transforma vehículos BKK staging → dwh.fact_transport_usage y dwh.fact_route_performance."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )

    with conn, conn.cursor() as cur:
        # 1) Asegurar filas en dim_time basadas en recorded_at
        cur.execute(
            """
            INSERT INTO dwh.dim_time (
                ts, date, hour, day_of_week, week, month, year, is_weekend, is_holiday
            )
            SELECT DISTINCT
                date_trunc('hour', v.recorded_at)::timestamp                             AS ts,
                date_trunc('day',  v.recorded_at)::date                                 AS date,
                EXTRACT(hour FROM v.recorded_at)::int                                   AS hour,
                EXTRACT(dow  FROM v.recorded_at)::int                                   AS day_of_week,
                EXTRACT(week FROM v.recorded_at)::int                                   AS week,
                EXTRACT(month FROM v.recorded_at)::int                                  AS month,
                EXTRACT(year FROM v.recorded_at)::int                                   AS year,
                CASE WHEN EXTRACT(dow FROM v.recorded_at) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                FALSE AS is_holiday
            FROM staging.bkk_vehicles_raw v
            WHERE v.recorded_at IS NOT NULL
            ON CONFLICT (ts) DO NOTHING;
            """
        )
        time_rows = cur.rowcount
        print(f"✅ Inserted {time_rows} new time_key rows")

        # 2) Insertar en fact_transport_usage con route_key nunca nulo
        cur.execute(
            """
            INSERT INTO dwh.fact_transport_usage (
                time_key, route_key, stop_key, trip_id, vehicle_id,
                occupancy_pct, events_count
            )
            SELECT 
                t.time_key,
                COALESCE(
                    r.route_key,
                    (SELECT route_key FROM dwh.dim_route WHERE route_id = 'UNKNOWN' LIMIT 1)
                ) AS route_key,
                (
                    SELECT s.stop_key 
                    FROM dwh.dim_stop s
                    WHERE s.latitude IS NOT NULL 
                      AND s.longitude IS NOT NULL
                    ORDER BY 
                        (POWER(s.latitude  - v.latitude, 2) + 
                         POWER(s.longitude - v.longitude, 2))
                    LIMIT 1
                ) AS stop_key,
                v.trip_id,
                v.vehicle_id,
                NULL AS occupancy_pct,
                1 AS events_count
            FROM staging.bkk_vehicles_raw v
            JOIN dwh.dim_time t 
              ON t.date = v.recorded_at::date
             AND t.hour = EXTRACT(HOUR FROM v.recorded_at)
            LEFT JOIN dwh.dim_route r 
              ON r.route_id = REPLACE(v.route_id, 'BKK_', '')
            WHERE v.latitude IS NOT NULL 
              AND v.longitude IS NOT NULL
              -- Por seguridad, exige que exista route_key para poder insertar
              AND COALESCE(
                    r.route_key,
                    (SELECT route_key FROM dwh.dim_route WHERE route_id = 'UNKNOWN' LIMIT 1)
                  ) IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 
                  FROM dwh.fact_transport_usage f2
                  WHERE f2.vehicle_id = v.vehicle_id 
                    AND f2.time_key  = t.time_key
              );
            """
        )
        usage_rows = cur.rowcount
        print(f"✅ Transformed {usage_rows} vehicle records to fact_transport_usage")

        # 3) Agregar por ruta y time_key en fact_route_performance
        cur.execute(
            """
            INSERT INTO dwh.fact_route_performance (
                time_key, route_key, total_trips, unique_vehicles
            )
            SELECT
                t.time_key,
                u.route_key,
                COUNT(*) AS total_trips,
                COUNT(DISTINCT u.vehicle_id) AS unique_vehicles
            FROM dwh.fact_transport_usage u
            JOIN dwh.dim_time t ON t.time_key = u.time_key
            WHERE u.route_key IS NOT NULL
            GROUP BY t.time_key, u.route_key
            ON CONFLICT (time_key, route_key) DO UPDATE
            SET total_trips     = EXCLUDED.total_trips,
                unique_vehicles = EXCLUDED.unique_vehicles;
            """
        )
        route_rows = cur.rowcount
        print(f"✅ Upserted {route_rows} rows into fact_route_performance")

        # 4) Limpiar staging
        cur.execute("TRUNCATE TABLE staging.bkk_vehicles_raw;")
        print("✅ Staging table cleaned")

    conn.close()


if __name__ == "__main__":
    print("Transforming BKK vehicles staging → DWH fact...")
    transform_bkk_to_dwh()
    print("✅ BKK transformation complete!")

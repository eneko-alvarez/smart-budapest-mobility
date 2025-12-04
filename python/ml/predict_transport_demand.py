import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, date

import pandas as pd
import psycopg2
import joblib
from dotenv import load_dotenv

# === Paths base del proyecto ===
ROOT = Path(__file__).resolve().parents[2]  # raíz del repo
# En Docker, models no está montado en root, así que usamos una copia en python/models
MODELS_DIR = ROOT / "python" / "models"
MODEL_PATH = MODELS_DIR / "demand_forecast_rf_v2.pkl"
ROUTE_MAPPING_PATH = MODELS_DIR / "route_mapping.pkl"

# Asegurar que ROOT está en sys.path por si acaso
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Cargar .env desde la raíz del proyecto
load_dotenv(ROOT / ".env")


def get_connection():
    host = os.getenv("DB_HOST", "localhost")
    # Truco: si estamos en Windows (local) y el host es bi_postgres, usar localhost
    # En Linux (Docker), bi_postgres es correcto y accesible
    if os.name == 'nt' and host == "bi_postgres":
        host = "localhost"

    return psycopg2.connect(
        host=host,
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )


def load_model_and_mapping():
    print(f"Cargando modelo desde: {MODEL_PATH}")
    model = joblib.load(MODEL_PATH)

    print(f"Cargando mapping de rutas desde: {ROUTE_MAPPING_PATH}")
    route_mapping = joblib.load(ROUTE_MAPPING_PATH)

    return model, route_mapping


def build_features_for_date(conn, target_date, route_mapping):
    """
    Construye el dataframe de features para una fecha futura (target_date)
    usando promedios históricos de días similares (mismo día de semana, misma hora).
    """
    print(f"Building features for prediction date: {target_date}")
    
    # Get historical averages for each route, day_of_week, and hour
    # Using last 4 weeks of data for similar days
    historical_query = """
        WITH target_info AS (
            SELECT 
                %s::date as target_date,
                EXTRACT(DOW FROM %s::date)::smallint as target_dow,
                CASE WHEN EXTRACT(DOW FROM %s::date) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
                EXTRACT(MONTH FROM %s::date)::smallint as month
        ),
        historical_patterns AS (
            SELECT 
                r.route_id,
                t.hour,
                t.day_of_week,
                AVG(rp.unique_vehicles) as avg_unique_vehicles,
                AVG(rp.avg_delay_seconds) as avg_delay_seconds,
                COUNT(*) as sample_count
            FROM dwh.fact_route_performance rp
            JOIN dwh.dim_time t ON rp.time_key = t.time_key
            JOIN dwh.dim_route r ON rp.route_key = r.route_key
            CROSS JOIN target_info ti
            WHERE t.date >= ti.target_date - INTERVAL '28 days'
              AND t.date < ti.target_date
              AND t.day_of_week = ti.target_dow
            GROUP BY r.route_id, t.hour, t.day_of_week
            HAVING COUNT(*) >= 1  -- At least 1 sample
        ),
        weather_forecast AS (
            SELECT 
                t.hour,
                COALESCE(AVG(w.temperature_c), 10) as temperature_c,
                COALESCE(AVG(w.humidity_pct), 70) as humidity_pct,
                COALESCE(AVG(w.wind_speed_ms), 5) as wind_speed_ms,
                COALESCE(AVG(w.precipitation_mm), 0) as precipitation_mm
            FROM dwh.fact_weather_conditions w
            JOIN dwh.dim_time t ON w.time_key = t.time_key
            CROSS JOIN target_info ti
            WHERE t.date >= ti.target_date - INTERVAL '7 days'
              AND t.date < ti.target_date
            GROUP BY t.hour
        )
        SELECT 
            ti.target_date as date,
            hours.hour,
            ti.target_dow as day_of_week,
            ti.is_weekend,
            0 as is_holiday,
            ti.month,
            hp.route_id,
            COALESCE(hp.avg_unique_vehicles, 5) as unique_vehicles,
            COALESCE(hp.avg_delay_seconds, 0) as avg_delay_seconds,
            COALESCE(wf.temperature_c, 10) as temperature_c,
            COALESCE(wf.humidity_pct, 70) as humidity_pct,
            COALESCE(wf.wind_speed_ms, 5) as wind_speed_ms,
            COALESCE(wf.precipitation_mm, 0) as precipitation_mm
        FROM target_info ti
        CROSS JOIN generate_series(0, 23) as hours(hour)
        CROSS JOIN (
            SELECT DISTINCT route_id 
            FROM dwh.dim_route 
            WHERE route_id != 'UNKNOWN'
        ) routes
        LEFT JOIN historical_patterns hp 
            ON routes.route_id = hp.route_id 
            AND hours.hour = hp.hour
        LEFT JOIN weather_forecast wf ON hours.hour = wf.hour
        WHERE hp.route_id IS NOT NULL  -- Only routes with historical data
        ORDER BY hours.hour, routes.route_id;
    """
    
    print("Executing historical pattern query...")
    df = pd.read_sql(
        historical_query, 
        conn, 
        params=[target_date, target_date, target_date, target_date]
    )
    
    if df.empty:
        print(f"⚠ No historical patterns found for day_of_week matching {target_date}")
        return df
    
    print(f"✓ Generated {len(df):,} prediction records")
    print(f"✓ Unique routes: {df['route_id'].nunique()}")
    print(f"✓ Hours covered: {df['hour'].nunique()}")

    return df


def predict_for_date(conn, target_date, model, route_mapping):
    df = build_features_for_date(conn, target_date, route_mapping)

    if df.empty:
        return 0
    
    # Asegurar tipos
    df["route_id"] = df["route_id"].astype(str)
    
    # Mapear route_id -> route_code usando el mapping del entrenamiento
    df["route_code"] = df["route_id"].map(route_mapping)
    df["route_code"] = df["route_code"].fillna(-1).astype(int)
    
    # Preprocesamiento básico
    df = df.copy()
    df.fillna(0, inplace=True)

    feature_cols = [
        "route_code",
        "hour",
        "day_of_week",
        "is_weekend",
        "is_holiday",
        "month",
        "unique_vehicles",
        "avg_delay_seconds",
        "temperature_c",
        "humidity_pct",
        "wind_speed_ms",
        "precipitation_mm",
    ]

    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el dataset para predecir: {missing}")

    X = df[feature_cols]

    print(f"Generating predictions for {target_date}...")
    df["predicted_trips"] = model.predict(X).round().astype(int)
    print("Predictions generated.")

    # Insertar en fact_route_predictions
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO dwh.fact_route_predictions (
            date, route_id, hour, predicted_trips, model_version
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_fact_route_predictions_date_route_hour DO UPDATE
        SET predicted_trips = EXCLUDED.predicted_trips,
            model_version   = EXCLUDED.model_version;
    """

    model_version = "rf_v2"

    rows = 0
    print("Inserting predictions:")
    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (row["date"], row["route_id"], int(row["hour"]), int(row["predicted_trips"]), model_version),
        )
        rows += 1
        if rows % 500 == 0:  # Print every 500 rows to reduce output
            print(f"    Inserted {rows} predictions...")

    conn.commit()
    cursor.close()

    print(f"✅ Inserted {rows}! ")
    return rows


def main():
    # Fecha objetivo: mañana (calculado automáticamente)
    target_date = date.today() + timedelta(days=1)
    print(f"Fecha objetivo de predicción: {target_date}")

    conn = get_connection()
    try:
        model, route_mapping = load_model_and_mapping()
        rows = predict_for_date(conn, target_date, model, route_mapping)
        if rows == 0:
            print("⚠ No se generaron predicciones (sin datos base en fact_route_performance).")
        else:
            print(f"✅ Predictions completed for {target_date}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

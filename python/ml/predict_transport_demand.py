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
MODELS_DIR = ROOT / "models"
MODEL_PATH = MODELS_DIR / "demand_forecast_rf_v2.pkl"
ROUTE_MAPPING_PATH = MODELS_DIR / "route_mapping.pkl"

# Asegurar que ROOT está en sys.path por si acaso
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Cargar .env desde la raíz del proyecto
load_dotenv(ROOT / ".env")


def get_connection():
    host = os.getenv("DB_HOST", "localhost")
    # Truco: si estamos fuera de Docker y el host es bi_postgres, usar localhost
    if host == "bi_postgres":
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
    Construye el dataframe de features para una fecha concreta (target_date)
    usando histórico de fact_route_performance + dim_route + dim_time + clima.
    """
    query = """
        SELECT 
          t.date, 
          r.route_id, 
          SUM(rp.total_trips)              AS total_trips,
          SUM(rp.unique_vehicles)          AS unique_vehicles,
          COALESCE(AVG(w.temperature_c), 0)    AS temperature_c, 
          COALESCE(AVG(w.humidity_pct), 0)     AS humidity_pct,
          COALESCE(AVG(w.wind_speed_ms), 0)    AS wind_speed_ms,
          COALESCE(SUM(w.precipitation_mm), 0) AS precipitation_mm,
          MAX(t.day_of_week)               AS day_of_week, 
          BOOL_OR(t.is_holiday)            AS is_holiday
        FROM dwh.fact_route_performance rp
        JOIN dwh.dim_time  t ON rp.time_key  = t.time_key
        JOIN dwh.dim_route r ON rp.route_key = r.route_key
        LEFT JOIN dwh.fact_weather_conditions w 
              ON rp.time_key = w.time_key
        WHERE t.date = %s
        GROUP BY t.date, r.route_id
        ORDER BY t.date, r.route_id;
    """


    df = pd.read_sql(query, conn, params=[target_date])

    if df.empty:
        print(f"⚠ No hay datos históricos en fact_route_performance para {target_date}")
        return df

    # Asegurar tipos
    df["route_id"] = df["route_id"].astype(str)

    # Mapear route_id -> route_code usando el mapping del entrenamiento
    df["route_code"] = df["route_id"].map(route_mapping)
    df["route_code"] = df["route_code"].fillna(-1).astype(int)

    # Preprocesamiento básico
    df = df.copy()
    df.fillna(0, inplace=True)

    # Limpiar 'total_trips' por si acaso
    df["total_trips"] = (
        df["total_trips"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    return df


def predict_for_date(conn, target_date, model, route_mapping):
    df = build_features_for_date(conn, target_date, route_mapping)

    if df.empty:
        return 0

    feature_cols = [
        "route_code",
        "unique_vehicles",
        "temperature_c",
        "humidity_pct",
        "wind_speed_ms",
        "precipitation_mm",
        "day_of_week",
        "is_holiday",
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
            date, route_id, predicted_trips, model_version
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_fact_route_predictions_date_route DO UPDATE
        SET predicted_trips = EXCLUDED.predicted_trips,
            model_version   = EXCLUDED.model_version;
    """

    model_version = "rf_v2"

    rows = 0
    print("Inserting predictions:")
    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (row["date"], row["route_id"], int(row["predicted_trips"]), model_version),
        )
        rows += 1
        print(f"    Inserting ({rows})")

    conn.commit()
    cursor.close()

    print(f"✅ Inserted {rows}! ")
    return rows


def main():
    # Fecha objetivo: mañana
    target_date = date(2025, 11, 16)
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

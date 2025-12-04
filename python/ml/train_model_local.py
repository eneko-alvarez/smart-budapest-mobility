"""
ML Model Training Script - Local Execution
===========================================

This script trains a RandomForest model to predict transport demand (total trips)
based on temporal features, weather conditions, and route characteristics.

It connects directly to the PostgreSQL database and extracts training data
from the data warehouse tables.

Usage:
    python python/ml/train_model_local.py [--dry-run] [--days DAYS]

Arguments:
    --dry-run: Extract and display data without training
    --days: Number of days of historical data to use (default: 60)
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import psycopg2
import joblib
from dotenv import load_dotenv

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder

# === Paths ===
ROOT = Path(__file__).resolve().parents[2]
MODELS_DIR = ROOT / "models"
MODEL_PATH = MODELS_DIR / "demand_forecast_rf_v2.pkl"
ROUTE_MAPPING_PATH = MODELS_DIR / "route_mapping.pkl"

# Load environment variables
load_dotenv(ROOT / ".env")

# Ensure models directory exists
MODELS_DIR.mkdir(exist_ok=True)


def get_connection():
    """Create database connection"""
    host = os.getenv("DB_HOST", "localhost")
    # If running outside Docker and host is bi_postgres, use localhost
    if host == "bi_postgres":
        host = "localhost"
    
    return psycopg2.connect(
        host=host,
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "bi_user"),
        password=os.getenv("DB_PASSWORD", "bi_password_secure"),
        dbname=os.getenv("DB_NAME", "bi_budapest"),
    )


def extract_training_data(conn, days=60):
    """
    Extract training data from the database.
    
    Args:
        conn: Database connection
        days: Number of days of historical data to extract
    
    Returns:
        pandas.DataFrame with training features and target
    """
    print(f"\n{'='*60}")
    print(f"EXTRACTING TRAINING DATA ({days} days)")
    print(f"{'='*60}\n")
    
    query = """
        SELECT 
          t.date,
          t.hour,
          t.day_of_week,
          t.is_weekend,
          t.is_holiday,
          t.month,
          r.route_id,
          SUM(rp.total_trips) AS total_trips,
          SUM(rp.unique_vehicles) AS unique_vehicles,
          AVG(rp.avg_delay_seconds) AS avg_delay_seconds,
          COALESCE(AVG(w.temperature_c), 0) AS temperature_c,
          COALESCE(AVG(w.humidity_pct), 0) AS humidity_pct,
          COALESCE(AVG(w.wind_speed_ms), 0) AS wind_speed_ms,
          COALESCE(SUM(w.precipitation_mm), 0) AS precipitation_mm
        FROM dwh.fact_route_performance rp
        JOIN dwh.dim_time t ON rp.time_key = t.time_key
        JOIN dwh.dim_route r ON rp.route_key = r.route_key
        LEFT JOIN dwh.fact_weather_conditions w ON rp.time_key = w.time_key
        WHERE t.date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY t.date, t.hour, t.day_of_week, t.is_weekend, t.is_holiday, t.month, r.route_id
        ORDER BY t.date, t.hour, r.route_id;
    """
    
    print("Executing query...")
    df = pd.read_sql(query, conn, params=[days])
    
    print(f"✓ Extracted {len(df):,} records")
    print(f"✓ Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"✓ Unique routes: {df['route_id'].nunique()}")
    print(f"✓ Unique dates: {df['date'].nunique()}")
    
    return df


def preprocess_data(df):
    """
    Preprocess the training data.
    
    Args:
        df: Raw dataframe from database
    
    Returns:
        Tuple of (X, y, route_mapping, feature_names)
    """
    print(f"\n{'='*60}")
    print("PREPROCESSING DATA")
    print(f"{'='*60}\n")
    
    # Create a copy to avoid warnings
    df = df.copy()
    
    # Handle missing values
    df.fillna(0, inplace=True)
    
    # Ensure route_id is string
    df["route_id"] = df["route_id"].astype(str)
    
    # Encode route_id to numeric
    print("Encoding route IDs...")
    route_encoder = LabelEncoder()
    df["route_code"] = route_encoder.fit_transform(df["route_id"])
    
    # Create route mapping dictionary for later use
    route_mapping = dict(zip(route_encoder.classes_, route_encoder.transform(route_encoder.classes_)))
    print(f"✓ Encoded {len(route_mapping)} unique routes")
    
    # Convert boolean to int
    df["is_weekend"] = df["is_weekend"].astype(int)
    df["is_holiday"] = df["is_holiday"].astype(int)
    
    # Define feature columns
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
    
    # Prepare features and target
    X = df[feature_cols]
    y = df["total_trips"]
    
    print(f"\n✓ Features shape: {X.shape}")
    print(f"✓ Target shape: {y.shape}")
    print(f"\nFeature statistics:")
    print(X.describe())
    
    print(f"\nTarget statistics:")
    print(f"  Mean trips: {y.mean():.2f}")
    print(f"  Median trips: {y.median():.2f}")
    print(f"  Min trips: {y.min()}")
    print(f"  Max trips: {y.max()}")
    
    return X, y, route_mapping, feature_cols


def train_model(X, y, feature_names):
    """
    Train RandomForest model with cross-validation.
    
    Args:
        X: Feature matrix
        y: Target vector
        feature_names: List of feature names
    
    Returns:
        Trained model
    """
    print(f"\n{'='*60}")
    print("TRAINING MODEL")
    print(f"{'='*60}\n")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    print(f"Training set: {len(X_train):,} samples")
    print(f"Test set: {len(X_test):,} samples")
    
    # Train RandomForest
    print("\nTraining RandomForest model...")
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=20,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    model.fit(X_train, y_train)
    print("✓ Model trained")
    
    # Evaluate on test set
    print("\nEvaluating on test set...")
    y_pred = model.predict(X_test)
    
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    print(f"\n{'='*60}")
    print("MODEL PERFORMANCE")
    print(f"{'='*60}")
    print(f"R² Score:  {r2:.4f}")
    print(f"MAE:       {mae:.2f} trips")
    print(f"RMSE:      {rmse:.2f} trips")
    print(f"{'='*60}")
    
    # Cross-validation
    print("\nPerforming 5-fold cross-validation...")
    cv_scores = cross_val_score(
        model, X_train, y_train, cv=5, scoring='r2', n_jobs=-1
    )
    print(f"CV R² Scores: {cv_scores}")
    print(f"Mean CV R²:   {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
    
    # Feature importance
    print(f"\n{'='*60}")
    print("FEATURE IMPORTANCE")
    print(f"{'='*60}")
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for idx, row in feature_importance.iterrows():
        print(f"{row['feature']:20s}: {row['importance']:.4f}")
    
    return model


def save_model(model, route_mapping):
    """
    Save trained model and route mapping to disk.
    
    Args:
        model: Trained model
        route_mapping: Route ID to code mapping
    """
    print(f"\n{'='*60}")
    print("SAVING MODEL")
    print(f"{'='*60}\n")
    
    # Save model
    print(f"Saving model to: {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)
    model_size_mb = MODEL_PATH.stat().st_size / (1024 * 1024)
    print(f"✓ Model saved ({model_size_mb:.2f} MB)")
    
    # Save route mapping
    print(f"Saving route mapping to: {ROUTE_MAPPING_PATH}")
    joblib.dump(route_mapping, ROUTE_MAPPING_PATH)
    mapping_size_kb = ROUTE_MAPPING_PATH.stat().st_size / 1024
    print(f"✓ Route mapping saved ({mapping_size_kb:.2f} KB)")
    
    print(f"\n✅ Training complete! Model ready for predictions.")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Train ML model for transport demand forecasting")
    parser.add_argument("--dry-run", action="store_true", help="Extract data without training")
    parser.add_argument("--days", type=int, default=60, help="Number of days of historical data")
    args = parser.parse_args()
    
    print(f"\n{'#'*60}")
    print("# ML MODEL TRAINING - SMART BUDAPEST MOBILITY")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn = get_connection()
        print("✓ Connected")
        
        # Extract data
        df = extract_training_data(conn, days=args.days)
        conn.close()
        
        if df.empty:
            print("\n❌ No data found! Cannot train model.")
            return
        
        if args.dry_run:
            print("\n✓ Dry run complete. Data extracted successfully.")
            print("\nSample data:")
            print(df.head(10))
            return
        
        # Preprocess
        X, y, route_mapping, feature_names = preprocess_data(df)
        
        # Train
        model = train_model(X, y, feature_names)
        
        # Save
        save_model(model, route_mapping)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

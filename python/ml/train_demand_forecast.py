import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import joblib
import os

# Carga tu CSV exportado (ajusta nombre si hace falta)
df = pd.read_csv('dataset_forecast.csv')

# Preprocesamiento básico
# df['weather_description'] = df['weather_description'].astype('category').cat.codes # Column not found, commenting out
df.fillna(0, inplace=True)

# Clean 'total_trips' column: remove commas and convert to numeric
df['total_trips'] = df['total_trips'].astype(str).str.replace(',', '', regex=False).astype(float)

# Variables independientes (features)
X = df[['unique_vehicles', 'temperature_c', 'humidity_pct', 'wind_speed_ms', 'precipitation_mm', 'day_of_week', 'is_holiday']]
# Variable dependiente (target)
y = df['total_trips']

# Dividir en train/test (80/20)
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

# Entrenar modelo
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predecir y evaluar
y_pred = model.predict(X_test)
score = r2_score(y_test, y_pred)
print(f'R2 score: {score:.3f}')

# Crear el directorio 'models' si no existe
os.makedirs('models', exist_ok=True)

# Debugging: Check if directory exists right before saving
if not os.path.exists('models'):
    print("Warning: 'models' directory still does not exist after os.makedirs!")

# Guardar modelo
joblib.dump(model, 'models/demand_forecast_rf.pkl')
print("Model saved as 'models/demand_forecast_rf.pkl'")
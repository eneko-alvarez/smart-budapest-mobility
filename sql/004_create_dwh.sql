-- Dimensiones
CREATE TABLE IF NOT EXISTS dwh.dim_time (
  time_key BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL UNIQUE,
  date DATE NOT NULL,
  hour SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
  week SMALLINT NOT NULL CHECK (week BETWEEN 1 AND 53),
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
  year SMALLINT NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  is_holiday BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dwh.dim_route (
  route_key BIGSERIAL PRIMARY KEY,
  route_id TEXT NOT NULL UNIQUE,
  route_name TEXT,
  agency_id TEXT,
  route_type TEXT,
  valid_from DATE,
  valid_to DATE
);

CREATE TABLE IF NOT EXISTS dwh.dim_stop (
  stop_key BIGSERIAL PRIMARY KEY,
  stop_id TEXT NOT NULL UNIQUE,
  stop_name TEXT,
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  zone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_weather_type (
  weather_key BIGSERIAL PRIMARY KEY,
  weather_code TEXT NOT NULL UNIQUE,
  description TEXT,
  severity SMALLINT
);

-- Hechos
CREATE TABLE IF NOT EXISTS dwh.fact_transport_usage (
  fact_id BIGSERIAL PRIMARY KEY,
  time_key BIGINT NOT NULL REFERENCES dwh.dim_time(time_key),
  route_key BIGINT REFERENCES dwh.dim_route(route_key),
  stop_key BIGINT REFERENCES dwh.dim_stop(stop_key),
  trip_id TEXT,
  vehicle_id TEXT,
  delay_seconds INT,
  occupancy_pct NUMERIC(5,2),
  events_count INT DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_fact_trans_time ON dwh.fact_transport_usage (time_key);
CREATE INDEX IF NOT EXISTS ix_fact_trans_route_time ON dwh.fact_transport_usage (route_key, time_key);

CREATE TABLE IF NOT EXISTS dwh.fact_weather_conditions (
  fact_id BIGSERIAL PRIMARY KEY,
  time_key BIGINT NOT NULL REFERENCES dwh.dim_time(time_key),
  weather_key BIGINT REFERENCES dwh.dim_weather_type(weather_key),
  temperature_c NUMERIC(5,2),
  humidity_pct NUMERIC(5,2),
  wind_speed_ms NUMERIC(6,2),
  precipitation_mm NUMERIC(6,2)
);
CREATE INDEX IF NOT EXISTS ix_fact_weather_time ON dwh.fact_weather_conditions (time_key);

-- KPIs diarios
CREATE TABLE IF NOT EXISTS dwh.kpi_daily (
  id BIGSERIAL PRIMARY KEY,
  kpi_date DATE NOT NULL,
  route_key BIGINT REFERENCES dwh.dim_route(route_key),
  stop_key BIGINT REFERENCES dwh.dim_stop(stop_key),
  metric_name TEXT NOT NULL,
  metric_value NUMERIC(18,4) NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (kpi_date, route_key, stop_key, metric_name)
);

-- Correlación
DROP TABLE IF EXISTS dwh.correlation_results CASCADE;
CREATE TABLE IF NOT EXISTS dwh.correlation_results (
  id BIGSERIAL PRIMARY KEY,
  window_granularity TEXT NOT NULL, -- 'hourly' | 'daily'
  route_key BIGINT REFERENCES dwh.dim_route(route_key),
  metric_x TEXT NOT NULL,
  metric_y TEXT NOT NULL,
  pearson_r NUMERIC(6,4),
  p_value NUMERIC(6,4),
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


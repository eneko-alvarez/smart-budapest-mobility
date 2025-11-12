-- Raw transport events (hypertable por recorded_at, sin PK única)
DROP TABLE IF EXISTS raw.transport_events CASCADE;
CREATE TABLE IF NOT EXISTS raw.transport_events (
  event_id UUID DEFAULT uuid_generate_v4(),
  route_id TEXT NOT NULL,
  stop_id TEXT NOT NULL,
  trip_id TEXT,
  vehicle_id TEXT,
  delay_seconds INT,
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  recorded_at TIMESTAMPTZ NOT NULL,
  source_timestamp TIMESTAMPTZ,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system TEXT NOT NULL DEFAULT 'BKK_FUTAR'
);
SELECT create_hypertable('raw.transport_events','recorded_at', if_not_exists => TRUE);

-- Índices (no únicos) para consultas
CREATE INDEX IF NOT EXISTS ix_transport_events_time ON raw.transport_events (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_transport_events_route_time ON raw.transport_events (route_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_transport_events_stop_time ON raw.transport_events (stop_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_transport_events_trip_time ON raw.transport_events (trip_id, recorded_at DESC);

-- Raw weather observations (hypertable)
DROP TABLE IF EXISTS raw.weather_observations CASCADE;
CREATE TABLE IF NOT EXISTS raw.weather_observations (
  observation_id UUID DEFAULT uuid_generate_v4(),
  location_code TEXT NOT NULL DEFAULT 'BUDAPEST',
  temperature_c NUMERIC(5,2),
  humidity_pct NUMERIC(5,2),
  wind_speed_ms NUMERIC(6,2),
  precipitation_mm NUMERIC(6,2),
  weather_code TEXT,
  recorded_at TIMESTAMPTZ NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_system TEXT NOT NULL DEFAULT 'OPEN_METEO'
);
SELECT create_hypertable('raw.weather_observations','recorded_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_weather_obs_time ON raw.weather_observations (recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_weather_obs_loc_time ON raw.weather_observations (location_code, recorded_at DESC);

-- Data quality logs
CREATE TABLE IF NOT EXISTS metadata.data_quality_results (
  id BIGSERIAL PRIMARY KEY,
  table_name TEXT NOT NULL,
  rule_name TEXT NOT NULL,
  passed_count BIGINT NOT NULL,
  failed_count BIGINT NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sample_rows JSONB
);

-- Linaje simple
CREATE TABLE IF NOT EXISTS metadata.table_lineage (
  id BIGSERIAL PRIMARY KEY,
  source_table TEXT NOT NULL,
  target_table TEXT NOT NULL,
  transformation_logic TEXT,
  version TEXT,
  documented_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

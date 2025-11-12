-- Staging: payloads crudos (sin PK estricta sobre id; índice por tiempo)
DROP TABLE IF EXISTS staging.bkk_futar_raw CASCADE;
CREATE TABLE IF NOT EXISTS staging.bkk_futar_raw (
  id UUID DEFAULT uuid_generate_v4(),
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL DEFAULT 'BKK_FUTAR',
  payload JSONB NOT NULL
);
-- Hypertable por received_at
SELECT create_hypertable('staging.bkk_futar_raw','received_at', if_not_exists => TRUE);
-- Índices útiles
CREATE INDEX IF NOT EXISTS ix_bkk_futar_received_at ON staging.bkk_futar_raw (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_bkk_futar_source ON staging.bkk_futar_raw (source);

DROP TABLE IF EXISTS staging.weather_raw CASCADE;
CREATE TABLE IF NOT EXISTS staging.weather_raw (
  id UUID DEFAULT uuid_generate_v4(),
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL DEFAULT 'OPEN_METEO',
  payload JSONB NOT NULL
);
SELECT create_hypertable('staging.weather_raw','received_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_weather_received_at ON staging.weather_raw (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_weather_source ON staging.weather_raw (source);

-- Staging: GTFS estático (para dimensiones), aquí sí sirve PK simple porque no es hypertable
DROP TABLE IF EXISTS staging.gtfs_routes_raw CASCADE;
CREATE TABLE IF NOT EXISTS staging.gtfs_routes_raw (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL
);

DROP TABLE IF EXISTS staging.gtfs_stops_raw CASCADE;
CREATE TABLE IF NOT EXISTS staging.gtfs_stops_raw (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL
);

-- Ingest log (se creó en 002 o 003; si no existe, crearlo)
CREATE TABLE IF NOT EXISTS metadata.job_execution (
  id BIGSERIAL PRIMARY KEY,
  job_name TEXT NOT NULL,
  status TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  end_time TIMESTAMPTZ,
  records_processed BIGINT,
  error_message TEXT
);

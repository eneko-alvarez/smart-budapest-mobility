-- Retención
SELECT add_retention_policy('staging.bkk_futar_raw', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('staging.weather_raw', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('raw.transport_events', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('raw.weather_observations', INTERVAL '180 days', if_not_exists => TRUE);

-- Reordenación por tiempo (opcional en TSDB 2.x)
-- SELECT reorder_chunk(ch, 'recorded_at') FROM show_chunks('raw.transport_events') ch;

-- Índices extra para consultas de Superset
CREATE INDEX IF NOT EXISTS ix_fact_trans_stop_time ON dwh.fact_transport_usage (stop_key, time_key);

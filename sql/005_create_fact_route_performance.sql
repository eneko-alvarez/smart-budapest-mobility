-- 005_create_fact_route_performance.sql
-- Crear tabla de rendimiento de rutas si no existe (faltaba en el backup)

CREATE TABLE IF NOT EXISTS dwh.fact_route_performance (
    time_key TIMESTAMP NOT NULL,
    route_key INTEGER NOT NULL,
    total_trips INTEGER DEFAULT 0,
    unique_vehicles INTEGER DEFAULT 0,
    PRIMARY KEY (time_key, route_key),
    CONSTRAINT fk_frp_time FOREIGN KEY (time_key) REFERENCES dwh.dim_time(time_key),
    CONSTRAINT fk_frp_route FOREIGN KEY (route_key) REFERENCES dwh.dim_route(route_key)
);

-- Índice para búsquedas por ruta
CREATE INDEX IF NOT EXISTS idx_fact_route_perf_route ON dwh.fact_route_performance(route_key);

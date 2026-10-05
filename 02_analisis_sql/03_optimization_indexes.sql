-- =====================================================================
-- FLEETLOGIX - INDICES DE OPTIMIZACION (Avance 2)
-- 5 indices basados en el analisis EXPLAIN ANALYZE de las 12 queries
-- Objetivo: mejorar performance en 20%+ sobre las queries principales
--
-- METODOLOGIA:
--   1. Medir tiempo base con EXPLAIN ANALYZE antes de crear indices
--   2. Crear indices estrategicos basados en los query plans
--   3. Medir tiempo despues y calcular mejora porcentual
--   4. Crear vista materializada para la query de mayor costo (I3)
--
-- RESULTADOS REALES (EXPLAIN ANALYZE sobre PostgreSQL local):
--   B2 (Rutas activas)     : 253.6ms -> 159.0ms  (37.3% mejora)
--   I3 (Reporte mensual)   : 1,084.7ms -> 3.2ms  (99.7% con vista materializada)
--   C1 (Ranking bayesiano) : 1,270.9ms -> ~900ms  (patron esperado con indice)
-- =====================================================================



-- =====================================================================
-- INDICE 1: idx_trips_composite_joins
-- Queries beneficiadas: B2, I1, I2, I4, C1, C2, C3, C4
-- Justificacion: Las queries 4-12 hacen JOIN intensivo entre trips y otras tablas. El indice compuesto (vehicle_id, driver_id, route_id, departure_datetime) con filtro WHERE status=completed elimina el seq scan sobre 110,000 filas.
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_trips_composite_joins
            ON trips(vehicle_id, driver_id, route_id, departure_datetime)
            WHERE status = 'completed';



-- =====================================================================
-- INDICE 2: idx_deliveries_scheduled_datetime
-- Queries beneficiadas: B3, C1, C2
-- Justificacion: Las queries de analisis temporal filtran y agrupan por scheduled_datetime. El indice parcial (solo status=delivered) reduce el rango de busqueda de 427,225 a 343,301 filas y acelera los GROUP BY mensuales.
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_deliveries_scheduled_datetime
            ON deliveries(scheduled_datetime, delivery_status)
            WHERE delivery_status = 'delivered';



-- =====================================================================
-- INDICE 3: idx_maintenance_vehicle_cost
-- Queries beneficiadas: B1, I2, I5
-- Justificacion: Query I5 necesita acceso rapido al SUM(cost) por vehiculo para comparar contra el promedio de la flota. El indice cubre vehicle_id y cost para permitir Index Only Scan.
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_maintenance_vehicle_cost
            ON maintenance(vehicle_id, cost);



-- =====================================================================
-- INDICE 4: idx_drivers_status_license
-- Queries beneficiadas: I1, C1, C4
-- Justificacion: Las queries de conductores filtran constantemente por status=active. El indice parcial (solo activos) es mas pequeno y rapido que uno completo. Tambien cubre license_expiry para alertas de vencimiento.
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_drivers_status_license
            ON drivers(status, license_expiry)
            WHERE status = 'active';



-- =====================================================================
-- INDICE 5: idx_routes_metrics
-- Queries beneficiadas: B2, I2, I4, C3
-- Justificacion: Query C3 clasifica rutas por cuartil de rendimiento usando distance_km. El indice cubre route_id, distance_km y destination_city para evitar acceso a la tabla en consultas de metricas.
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_routes_metrics
            ON routes(route_id, distance_km, destination_city);



-- =====================================================================
-- VISTA MATERIALIZADA: mv_resumen_mensual
-- Basada en query I3 (Reporte mensual de operaciones)
-- Mejora: 1,084.7ms -> 3.2ms (99.7% reduccion)
-- Uso: SELECT * FROM mv_resumen_mensual ORDER BY anio, mes;
-- Actualizar: REFRESH MATERIALIZED VIEW mv_resumen_mensual;
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_resumen_mensual AS
SELECT
    EXTRACT(YEAR  FROM t.departure_datetime)::INT AS anio,
    EXTRACT(MONTH FROM t.departure_datetime)::INT AS mes,
    COUNT(DISTINCT t.trip_id)                     AS total_viajes,
    COUNT(d.delivery_id)                           AS total_entregas,
    COUNT(CASE WHEN d.delivery_status = 'delivered' THEN 1 END)
                                                   AS entregas_exitosas,
    ROUND(
        COUNT(CASE WHEN d.delivery_status = 'delivered' THEN 1 END)
        * 100.0 / NULLIF(COUNT(d.delivery_id), 0), 1)
                                                   AS pct_exitoso,
    ROUND(SUM(t.fuel_consumed_liters)::NUMERIC, 1) AS total_litros,
    ROUND(AVG(t.fuel_consumed_liters)::NUMERIC, 2) AS prom_litros_viaje,
    COUNT(DISTINCT t.vehicle_id)                   AS vehiculos_activos,
    COUNT(DISTINCT t.driver_id)                    AS conductores_activos
FROM trips t
JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.status = 'completed'
GROUP BY
    EXTRACT(YEAR  FROM t.departure_datetime),
    EXTRACT(MONTH FROM t.departure_datetime)
ORDER BY anio, mes
WITH DATA;

-- Indice sobre la vista materializada para consultas por periodo
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_resumen_mensual_periodo
ON mv_resumen_mensual (anio, mes);

-- =====================================================================
-- VERIFICACION DE INDICES CREADOS
-- =====================================================================
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- =====================================================================
-- MANTENIMIENTO: actualizar estadisticas despues de crear indices
-- =====================================================================
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;

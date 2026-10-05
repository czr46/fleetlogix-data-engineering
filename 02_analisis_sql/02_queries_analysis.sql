-- =====================================================================
-- FLEETLOGIX - ANALISIS SQL (Avance 2)
-- 12 Queries organizadas por nivel de complejidad
-- Base de datos: PostgreSQL fleetlogix (505,556 registros)
--
-- ESTRUCTURA:
--   Nivel Basico   (B1-B3): JOINs, GROUP BY, funciones de agregacion
--   Nivel Intermedio (I1-I5): Subconsultas, funciones de fecha, HAVING
--   Nivel Complejo (C1-C4): CTEs, window functions, modelo bayesiano
--
-- QUERY DESTACADA: C1 - Ranking Bayesiano de Conductores
--   Implementa Empirical Bayes para ponderar 3 factores de eficiencia:
--   40% entregas exitosas + 35% puntualidad + 25% combustible/km
--   Prior m=500 (media de flota). Score max: 82.84, min: 11.48, SD: 11.18
-- =====================================================================



-- =====================================================================
-- NIVEL BASICO
-- =====================================================================


-- ---------------------------------------------------------------------
-- B1 — Vehiculos activos y fecha de ultimo mantenimiento
-- 
Nivel: Basico | Conceptos: LEFT JOIN, MAX(), GROUP BY, ORDER BY-- Pregunta: Que vehiculos estan en riesgo de falla por falta de mantenimiento?
-- ---------------------------------------------------------------------
SELECT
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    v.status,
    MAX(m.maintenance_date)      AS ultimo_mantenimiento,
    MIN(m.next_maintenance_date) AS proximo_mantenimiento
FROM vehicles v
LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type, v.status
ORDER BY ultimo_mantenimiento ASC NULLS FIRST
LIMIT 20



-- ---------------------------------------------------------------------
-- B2 — Rutas mas activas por volumen y consumo
-- 
Nivel: Basico | Conceptos: INNER JOIN, COUNT, SUM, AVG, ROUND, WHERE-- Pregunta: Que rutas concentran mas operacion y mas consumo de combustible?
-- ---------------------------------------------------------------------
SELECT
    r.origin_city                                     AS ciudad_origen,
    r.destination_city                                AS ciudad_destino,
    COUNT(t.trip_id)                                  AS total_viajes,
    ROUND(AVG(t.fuel_consumed_liters), 2)             AS promedio_combustible_l,
    ROUND(SUM(t.fuel_consumed_liters), 0)             AS total_combustible_l,
    ROUND(AVG(r.distance_km), 0)                      AS distancia_km
FROM routes r
JOIN trips t ON r.route_id = t.route_id
WHERE t.status = 'completed'
GROUP BY r.origin_city, r.destination_city
ORDER BY total_viajes DESC
LIMIT 10



-- ---------------------------------------------------------------------
-- B3 — KPI de estados de entrega
-- 
Nivel: Basico | Conceptos: GROUP BY, COUNT, porcentaje sobre total con OVER()-- Pregunta: Cual es la tasa de exito de entregas por estado?
-- ---------------------------------------------------------------------
SELECT
    delivery_status,
    COUNT(*)                                                        AS total_entregas,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)            AS porcentaje
FROM deliveries
GROUP BY delivery_status
ORDER BY total_entregas DESC



-- =====================================================================
-- NIVEL INTERMEDIO
-- =====================================================================


-- ---------------------------------------------------------------------
-- I1 — Top conductores mas productivos
-- 
Nivel: Intermedio | Conceptos: JOIN 3 tablas, COUNT DISTINCT, HAVING, INTERVAL-- Pregunta: Quienes son los conductores con mas viajes completados este año?
-- ---------------------------------------------------------------------
SELECT
    d.driver_id,
    d.first_name || ' ' || d.last_name              AS conductor,
    COUNT(DISTINCT t.trip_id)                        AS total_viajes,
    COUNT(del.delivery_id)                           AS total_entregas,
    ROUND(
        COUNT(CASE WHEN del.delivery_status = 'delivered' THEN 1 END) * 100.0
        / NULLIF(COUNT(del.delivery_id), 0), 1)      AS tasa_exito_pct,
    ROUND(AVG(del.package_weight_kg), 2)             AS peso_prom_kg
FROM drivers d
JOIN trips t       ON d.driver_id = t.driver_id
JOIN deliveries del ON t.trip_id  = del.trip_id
WHERE t.departure_datetime >= NOW() - INTERVAL '12 months'
GROUP BY d.driver_id, d.first_name, d.last_name
HAVING COUNT(del.delivery_id) > 10
ORDER BY total_entregas DESC
LIMIT 15



-- ---------------------------------------------------------------------
-- I2 — Eficiencia de combustible por tipo de vehiculo
-- 
Nivel: Intermedio | Conceptos: LEFT JOIN multiple, condicion en ON, NULLIF-- Pregunta: Que tipo de vehiculo tiene menor consumo de combustible por km?
-- ---------------------------------------------------------------------
SELECT
    v.vehicle_type,
    COUNT(DISTINCT v.vehicle_id)                                   AS flota,
    COUNT(t.trip_id)                                               AS viajes_completados,
    ROUND(AVG(t.fuel_consumed_liters), 2)                          AS consumo_prom_litros,
    ROUND(AVG(t.fuel_consumed_liters / NULLIF(r.distance_km,0) * 100), 2) AS litros_por_100km,
    ROUND(SUM(m.cost) / NULLIF(COUNT(DISTINCT v.vehicle_id), 0), 2) AS costo_mant_prom_usd
FROM vehicles v
LEFT JOIN trips       t ON v.vehicle_id = t.vehicle_id AND t.status = 'completed'
LEFT JOIN routes      r ON t.route_id   = r.route_id
LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
GROUP BY v.vehicle_type
ORDER BY litros_por_100km ASC NULLS LAST



-- ---------------------------------------------------------------------
-- I3 — Reporte mensual de operaciones
-- 
Nivel: Intermedio | Conceptos: EXTRACT, CASE WHEN, conteo condicional-- Pregunta: Como evoluciona el volumen de operacion mes a mes?
-- ---------------------------------------------------------------------
SELECT
    EXTRACT(YEAR  FROM t.departure_datetime)::INT                    AS anio,
    EXTRACT(MONTH FROM t.departure_datetime)::INT                    AS mes,
    COUNT(t.trip_id)                                                 AS total_viajes,
    COUNT(CASE WHEN t.status = 'completed' THEN 1 END)               AS completados,
    COUNT(CASE WHEN t.status = 'in_progress' THEN 1 END)             AS en_progreso,
    ROUND(COUNT(CASE WHEN t.status='completed' THEN 1 END)*100.0
          / NULLIF(COUNT(t.trip_id),0), 1)                           AS tasa_exito_pct,
    COUNT(del.delivery_id)                                           AS total_entregas,
    ROUND(SUM(t.fuel_consumed_liters), 0)                            AS combustible_litros
FROM trips t
LEFT JOIN deliveries del ON t.trip_id = del.trip_id
GROUP BY
    EXTRACT(YEAR  FROM t.departure_datetime),
    EXTRACT(MONTH FROM t.departure_datetime)
ORDER BY anio, mes



-- ---------------------------------------------------------------------
-- I4 — Tiempo real vs estimado por ruta
-- 
Nivel: Intermedio | Conceptos: EXTRACT(EPOCH FROM ...), HAVING con COUNT-- Pregunta: Que rutas se desvian mas del tiempo estimado de viaje?
-- ---------------------------------------------------------------------
SELECT
    r.origin_city,
    r.destination_city,
    ROUND(r.distance_km, 0)                                          AS km,
    COUNT(t.trip_id)                                                  AS viajes,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600
    ), 2)                                                             AS duracion_real_h,
    r.estimated_duration_hours                                        AS duracion_est_h,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600
    ) - r.estimated_duration_hours, 2)                               AS desvio_h,
    COUNT(CASE WHEN del.delivery_status = 'delivered' THEN 1 END)    AS entregas_ok,
    COUNT(CASE WHEN del.delivery_status = 'failed'    THEN 1 END)    AS entregas_fallidas
FROM routes r
JOIN trips t       ON r.route_id = t.route_id AND t.status = 'completed'
LEFT JOIN deliveries del ON t.trip_id = del.trip_id
GROUP BY r.route_id, r.origin_city, r.destination_city, r.distance_km, r.estimated_duration_hours
HAVING COUNT(t.trip_id) >= 50
ORDER BY desvio_h DESC



-- ---------------------------------------------------------------------
-- I5 — Vehiculos con costo de mantenimiento sobre el promedio
-- 
Nivel: Intermedio | Conceptos: Subconsulta anidada en HAVING (2 niveles)-- Pregunta: Que vehiculos son candidatos a reemplazo por alto costo de mantenimiento?
-- ---------------------------------------------------------------------
SELECT
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    COUNT(m.maintenance_id)          AS num_mantenimientos,
    ROUND(SUM(m.cost), 2)            AS costo_total,
    ROUND(AVG(m.cost), 2)            AS costo_promedio,
    MAX(m.maintenance_date)          AS ultimo_mantenimiento,
    MIN(m.next_maintenance_date)     AS proximo_mantenimiento
FROM vehicles v
JOIN maintenance m ON v.vehicle_id = m.vehicle_id
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type
HAVING SUM(m.cost) > (
    SELECT AVG(costo_total)
    FROM (
        SELECT vehicle_id, SUM(cost) AS costo_total
        FROM maintenance
        GROUP BY vehicle_id
    ) sub
)
ORDER BY costo_total DESC



-- =====================================================================
-- NIVEL COMPLEJO
-- =====================================================================


-- ---------------------------------------------------------------------
-- C1 — Ranking bayesiano de eficiencia de conductores (MODELO PRINCIPAL)
-- 
Nivel: Complejo | Conceptos: 5 CTEs encadenadas, RANK(), NTILE(), Empirical Bayes-- Pregunta: Quien es el mejor conductor considerando entregas exitosas (40%),-- puntualidad en tiempo y forma (35%) y eficiencia de combustible/km (25%)?-- Metodologia: Shrinkage bayesiano con prior m=500 (media de flota como referencia),-- normalizacion a [0,100] y score compuesto ponderado. 344 conductores, 403,874 entregas.-- Score maximo: 82.84 (Sergio Ruelas Amaya) | Minimo: 11.48 | SD: 11.18
-- ---------------------------------------------------------------------
WITH
-- ─────────────────────────────────────────────────────────────────
-- PASO 1a: Combustible proporcional por entrega
-- Se calcula ANTES del GROUP BY porque PostgreSQL no permite
-- funciones de ventana (OVER) dentro de funciones de agregacion (AVG).
-- Aqui cada entrega recibe la parte proporcional del combustible
-- del viaje: fuel_del_viaje / numero_de_entregas_del_viaje
-- ─────────────────────────────────────────────────────────────────
fuel_por_entrega AS (
    SELECT
        t.driver_id,
        t.route_id,
        del.delivery_id,
        del.delivery_status,
        del.scheduled_datetime,
        del.delivered_datetime,
        -- Combustible proporcional: total del viaje / N entregas del viaje
        t.fuel_consumed_liters
            / NULLIF(COUNT(del.delivery_id) OVER (PARTITION BY t.trip_id), 0)
                                                AS fuel_entrega,
        r.distance_km
    FROM trips      t
    JOIN deliveries del ON t.trip_id  = del.trip_id
    JOIN routes     r   ON t.route_id = r.route_id
    WHERE t.status = 'completed'
),

-- ─────────────────────────────────────────────────────────────────
-- PASO 1b: Metricas base por conductor
-- Ahora si podemos agrupar sin conflicto con ventanas.
-- Los tres insumos del score: n, ok, on_time, avg_l_per_100km
-- ─────────────────────────────────────────────────────────────────
metricas_base AS (
    SELECT
        d.driver_id,
        d.first_name || ' ' || d.last_name               AS conductor,
        COUNT(f.delivery_id)                              AS n,
        -- Entregas exitosas (tiempo y forma)
        COUNT(CASE WHEN f.delivery_status = 'delivered'   THEN 1 END)
                                                          AS ok,
        -- Entregas puntuales: entregadas en <= 30 min del horario
        COUNT(CASE WHEN f.delivery_status = 'delivered'
                    AND EXTRACT(EPOCH FROM (
                            f.delivered_datetime
                            - f.scheduled_datetime)) / 60.0 <= 30
                   THEN 1 END)                            AS on_time,
        -- Combustible por 100 km: ya viene proporcional del paso 1a
        AVG(f.fuel_entrega / NULLIF(f.distance_km, 0) * 100.0)
                                                          AS avg_l_per_100km
    FROM drivers d
    JOIN fuel_por_entrega f ON d.driver_id = f.driver_id
    GROUP BY d.driver_id, d.first_name, d.last_name
    HAVING COUNT(f.delivery_id) > 0
),

-- ─────────────────────────────────────────────────────────────────
-- PASO 2: Priors bayesianos (medias de la flota)
-- mu_ok  = promedio de % exitoso entre todos los conductores
-- mu_ot  = promedio de % puntual entre todos los conductores
-- mu_fuel= promedio de L/100km entre todos los conductores
-- ─────────────────────────────────────────────────────────────────
priors AS (
    SELECT
        AVG(ok      * 1.0 / NULLIF(n, 0))  AS mu_ok,    -- ~0.850
        AVG(on_time * 1.0 / NULLIF(n, 0))  AS mu_ot,    -- ~0.427
        AVG(avg_l_per_100km)                AS mu_fuel   -- ~2.293 L/100km
    FROM metricas_base
),

-- ─────────────────────────────────────────────────────────────────
-- PASO 3: Ajuste bayesiano (shrinkage hacia la media de la flota)
-- m = 500: equivale a darle el peso de 500 entregas al prior.
-- Conductores con pocas entregas quedan cerca de la media de flota.
-- Conductores con muchas entregas dominan su propio promedio.
-- Formula: posterior = (exitos + m * mu) / (n + m)
-- ─────────────────────────────────────────────────────────────────
bayes AS (
    SELECT
        mb.driver_id,
        mb.conductor,
        mb.n,
        ROUND(mb.ok      * 100.0 / NULLIF(mb.n, 0), 1)  AS pct_exitoso_raw,
        ROUND(mb.on_time * 100.0 / NULLIF(mb.n, 0), 1)  AS pct_puntual_raw,
        ROUND(mb.avg_l_per_100km, 3)                     AS l_100km_raw,
        ROUND((mb.ok      + 500.0 * p.mu_ok)
              / (mb.n + 500.0) * 100, 2)                 AS bayes_exitoso,
        ROUND((mb.on_time + 500.0 * p.mu_ot)
              / (mb.n + 500.0) * 100, 2)                 AS bayes_puntual,
        ROUND((mb.n * mb.avg_l_per_100km + 500.0 * p.mu_fuel)
              / (mb.n + 500.0), 4)                       AS bayes_fuel
    FROM metricas_base mb
    CROSS JOIN priors p
),

-- ─────────────────────────────────────────────────────────────────
-- PASO 4: Normalizar cada componente a [0, 100]
-- Esto garantiza que los pesos 40/35/25 se apliquen en esa proporcion.
-- Sin normalizar, el componente con mayor rango absoluto dominaria
-- el score sin importar el peso asignado.
-- El combustible se INVIERTE: 0 = mayor consumo, 100 = mas eficiente
-- ─────────────────────────────────────────────────────────────────
scores AS (
    SELECT *,
        ROUND(
            (bayes_exitoso - MIN(bayes_exitoso) OVER ())
            / NULLIF(MAX(bayes_exitoso) OVER ()
                     - MIN(bayes_exitoso) OVER (), 0) * 100, 2) AS s_exitoso,
        ROUND(
            (bayes_puntual - MIN(bayes_puntual) OVER ())
            / NULLIF(MAX(bayes_puntual) OVER ()
                     - MIN(bayes_puntual) OVER (), 0) * 100, 2) AS s_puntual,
        ROUND(
            (1.0 - (bayes_fuel - MIN(bayes_fuel) OVER ())
                 / NULLIF(MAX(bayes_fuel) OVER ()
                          - MIN(bayes_fuel) OVER (), 0)) * 100, 2) AS s_fuel
    FROM bayes
)

-- ─────────────────────────────────────────────────────────────────
-- RESULTADO FINAL: score compuesto ponderado 40 / 35 / 25
-- RANK()  -> posicion del conductor (1 = mejor)
-- NTILE(5) -> quintil (1=Elite, 5=Bajo rendimiento)
-- ─────────────────────────────────────────────────────────────────
SELECT
    RANK() OVER (ORDER BY
        0.40*s_exitoso + 0.35*s_puntual + 0.25*s_fuel DESC) AS ranking,
    conductor,
    n                   AS total_entregas,
    pct_exitoso_raw,
    pct_puntual_raw,
    l_100km_raw,
    bayes_exitoso,
    bayes_puntual,
    s_exitoso,
    s_puntual,
    s_fuel,
    ROUND(0.40*s_exitoso + 0.35*s_puntual + 0.25*s_fuel, 2) AS score_compuesto,
    NTILE(5) OVER (ORDER BY
        0.40*s_exitoso + 0.35*s_puntual + 0.25*s_fuel DESC) AS quintil
FROM scores
ORDER BY score_compuesto DESC
LIMIT 20



-- ---------------------------------------------------------------------
-- C2 — Tendencia mensual y variacion porcentual por ciudad
-- 
Nivel: Complejo | Conceptos: LAG(), PARTITION BY, promedio movil 3 meses-- Pregunta: Las entregas crecen o decaen por ciudad? Hay estacionalidad?
-- ---------------------------------------------------------------------
WITH entregas_mensuales AS (
    SELECT
        EXTRACT(YEAR  FROM t.departure_datetime)::INT AS anio,
        EXTRACT(MONTH FROM t.departure_datetime)::INT AS mes,
        r.origin_city                                  AS ciudad,
        COUNT(del.delivery_id)                         AS entregas
    FROM trips t
    JOIN routes      r   ON t.route_id = r.route_id
    JOIN deliveries del  ON t.trip_id  = del.trip_id
    WHERE del.delivery_status = 'delivered'
    GROUP BY anio, mes, ciudad
)
SELECT
    anio, mes, ciudad, entregas,
    LAG(entregas) OVER (PARTITION BY ciudad ORDER BY anio, mes) AS mes_anterior,
    ROUND(
        (entregas - LAG(entregas) OVER (PARTITION BY ciudad ORDER BY anio, mes))
        * 100.0
        / NULLIF(LAG(entregas) OVER (PARTITION BY ciudad ORDER BY anio, mes), 0),
    1) AS variacion_pct,
    ROUND(AVG(entregas) OVER (
        PARTITION BY ciudad
        ORDER BY anio, mes
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0) AS promedio_movil_3m
FROM entregas_mensuales
ORDER BY ciudad, anio, mes



-- ---------------------------------------------------------------------
-- C3 — Clasificacion de rutas por cuartil de rendimiento
-- 
Nivel: Complejo | Conceptos: NTILE(4), CTE encadenada, CASE WHEN multivariable-- Pregunta: Que rutas son de alto rendimiento y cuales necesitan intervencion?
-- ---------------------------------------------------------------------
WITH eficiencia AS (
    SELECT
        r.route_id,
        r.origin_city || ' -> ' || r.destination_city       AS ruta,
        ROUND(r.distance_km, 0)                              AS km,
        COUNT(t.trip_id)                                     AS viajes,
        ROUND(AVG(t.fuel_consumed_liters / NULLIF(r.distance_km,0) * 100), 2) AS l_100km,
        ROUND(AVG(
            EXTRACT(EPOCH FROM (t.arrival_datetime - t.departure_datetime)) / 3600
            / NULLIF(r.estimated_duration_hours, 0)
        ), 2)                                                AS ratio_tiempo
    FROM routes r
    JOIN trips t ON r.route_id = t.route_id AND t.status = 'completed'
    GROUP BY r.route_id, r.origin_city, r.destination_city, r.distance_km, r.estimated_duration_hours
    HAVING COUNT(t.trip_id) >= 30
),
cuartiles AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY l_100km    ASC) AS q_combustible,
        NTILE(4) OVER (ORDER BY ratio_tiempo ASC) AS q_tiempo
    FROM eficiencia
)
SELECT
    ruta, km, viajes, l_100km, ratio_tiempo,
    q_combustible, q_tiempo,
    CASE
        WHEN q_combustible = 1 AND q_tiempo = 1 THEN '🟢 Excelente'
        WHEN q_combustible <= 2 AND q_tiempo <= 2 THEN '🟡 Buena'
        WHEN q_combustible >= 4 OR  q_tiempo >= 4 THEN '🔴 A mejorar'
        ELSE '🟠 Regular'
    END AS clasificacion
FROM cuartiles
ORDER BY l_100km ASC



-- ---------------------------------------------------------------------
-- C4 — Analisis de fatiga: conductores sin descanso minimo
-- 
Nivel: Complejo | Conceptos: LAG() PARTITION BY driver_id, diferencia temporal-- Pregunta: Hay conductores con menos de 8 horas entre viajes consecutivos? (riesgo operacional)
-- ---------------------------------------------------------------------
WITH viajes_conductor AS (
    SELECT
        d.driver_id,
        d.first_name || ' ' || d.last_name AS conductor,
        t.trip_id,
        t.departure_datetime,
        ROUND(EXTRACT(EPOCH FROM (
            t.departure_datetime
            - LAG(t.departure_datetime) OVER (
                PARTITION BY d.driver_id
                ORDER BY t.departure_datetime
            )
        )) / 3600.0, 1) AS horas_descanso
    FROM drivers d
    JOIN trips t ON d.driver_id = t.driver_id
),
resumen AS (
    SELECT
        driver_id,
        conductor,
        COUNT(trip_id)                                              AS total_viajes,
        ROUND(AVG(horas_descanso), 1)                              AS descanso_prom_h,
        MIN(horas_descanso)                                        AS descanso_min_h,
        COUNT(CASE WHEN horas_descanso < 8 THEN 1 END)             AS viajes_sin_8h_descanso
    FROM viajes_conductor
    GROUP BY driver_id, conductor
)
SELECT
    conductor,
    total_viajes,
    descanso_prom_h,
    descanso_min_h,
    viajes_sin_8h_descanso,
    CASE
        WHEN viajes_sin_8h_descanso > 10 THEN '🔴 Alto riesgo'
        WHEN viajes_sin_8h_descanso > 5  THEN '🟡 Riesgo moderado'
        ELSE '🟢 Normal'
    END AS nivel_riesgo
FROM resumen
WHERE total_viajes > 5
ORDER BY viajes_sin_8h_descanso DESC
LIMIT 20


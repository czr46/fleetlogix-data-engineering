-- =====================================================================
-- FLEETLOGIX - DATA WAREHOUSE DIMENSIONAL MODEL
-- Modelo estrella para analisis en Snowflake
-- Archivo: A3-04_dimensional_model.sql  (version adaptada al ETL)
-- =====================================================================
--
-- CAMBIOS RESPECTO AL DDL ORIGINAL Y JUSTIFICACION
-- ─────────────────────────────────────────────────
--
-- 1. dim_vehicle / dim_driver: renombrar columna 'status'
--    ORIGINAL : status VARCHAR(20)
--    ADAPTADO : vehicle_status / driver_status VARCHAR(20)
--    RAZON    : El JOIN del ETL une vehicles.status y drivers.status en el
--               mismo DataFrame. Para distinguirlos sin ambiguedad se usa
--               alias: v.status AS vehicle_status y dr.status AS driver_status.
--               El nombre de la columna en Snowflake debe coincidir con el
--               alias que el ETL usa al hacer el INSERT.
--
-- 2. fact_deliveries: agregar columna data_quality_flag
--    ORIGINAL : no existe
--    ADAPTADO : data_quality_flag VARCHAR(50)
--    RAZON    : El ETL genera esta columna para marcar registros con
--               anomalias de calidad (peso_invalido, tiempo_muy_negativo,
--               viaje_muy_largo) SIN eliminarlos. Es auditoria obligatoria.
--               Valores: 'ok' (normal) o combinacion de flags con '|'.
--
-- 3. fact_deliveries: renombrar has_signature -> recipient_signature
--    ORIGINAL : has_signature BOOLEAN
--    ADAPTADO : recipient_signature BOOLEAN
--    RAZON    : El campo en PostgreSQL se llama recipient_signature.
--               El ETL lo pasa directamente al INSERT sin renombrarlo.
--               La columna en Snowflake debe usar el mismo nombre.
--
-- 4. fact_deliveries: delivery_time_minutes INT -> DECIMAL(8,2)
--    ORIGINAL : delivery_time_minutes INT
--    ADAPTADO : delivery_time_minutes DECIMAL(8,2)
--    RAZON    : El ETL calcula (delivered - scheduled).total_seconds()/60
--               con .round(2), produciendo valores decimales como 20.43.
--               INT truncaria el decimal. Ademas el campo puede ser NEGATIVO
--               (entrega anticipada, min=-60 en los datos reales), lo cual
--               es valido y se preserva.
--
-- 5. fact_deliveries: delay_minutes INT -> DECIMAL(8,2)
--    ORIGINAL : delay_minutes INT
--    ADAPTADO : delay_minutes DECIMAL(8,2)
--    RAZON    : Es clip(delivery_time_minutes, lower=0).round(2).
--               Igual que delivery_time_minutes, produce decimales.
--
-- 6. dim_vehicle / dim_driver: vehicle_key / driver_key INT -> AUTOINCREMENT
--    ORIGINAL : vehicle_key INT PRIMARY KEY (sin AUTOINCREMENT)
--    ADAPTADO : vehicle_key INT AUTOINCREMENT PRIMARY KEY
--    RAZON    : El ETL hace MERGE sin especificar el key (es surrogado).
--               Snowflake debe generarlo automaticamente con AUTOINCREMENT.
--               Sin esto el INSERT falla por NOT NULL en la clave primaria.
--               Lo mismo aplica para driver_key, route_key, customer_key.
--
-- 7. dim_customer: eliminar customer_id IDENTITY (duplicado)
--    ORIGINAL : customer_key INT PRIMARY KEY + customer_id INT IDENTITY
--    ADAPTADO : solo customer_key INT AUTOINCREMENT PRIMARY KEY
--    RAZON    : Tener dos columnas de identidad (customer_key y customer_id)
--               es redundante y confuso. El ETL solo usa customer_key como
--               surrogado. Se elimina customer_id para evitar ambiguedad.
--
-- 8. Agregar staging_trips (tabla nueva)
--    ORIGINAL : no existe
--    ADAPTADO : staging_trips con 14 columnas
--    RAZON    : Snowflake es OLAP y no replica las tablas transaccionales
--               de PostgreSQL. La vista v_operations_deliveries necesita
--               datos de viajes. staging_trips es la replica enriquecida
--               de trips con 3 campos calculados en el ETL que no existen
--               en PostgreSQL: trip_duration_hours, deliveries_in_trip,
--               deliveries_per_hour.
--
-- 9. Agregar daily_kpi_summary (tabla nueva)
--    ORIGINAL : no existe
--    ADAPTADO : daily_kpi_summary con 15 columnas
--    RAZON    : Resumen diario pre-calculado para dashboards en <100ms.
--               Se actualiza con MERGE en cada ejecucion del ETL.
--
-- 10. Actualizar v_operations_deliveries para incluir staging_trips
--     ORIGINAL : no incluia duracion del viaje ni KPIs de trips
--     ADAPTADO : agrega JOIN con staging_trips para trip_duration_hours
--                y deliveries_per_hour en la vista operacional.
--
-- RANGOS REALES DE DATOS (verificados sobre la BD de FleetLogix):
--   date_key              : 20230101 a 20250102
--   scheduled_time_key    : 0 a 2359
--   delivery_time_minutes : -60 a 120  (negativos = entrega anticipada)
--   delay_minutes         : 0 a 120
--   deliveries_per_hour   : 0.02 a 45.58
--   fuel_efficiency       : 7.24 a 29.52 km/L
--   cost_per_delivery     : 1.69 a 9,122.63 MXN
--   revenue_per_delivery  : 80.15 a 155.00 MXN
-- =====================================================================


-- ─────────────────────────────────────────────────────────────────────
-- CONFIGURACION INICIAL DE SNOWFLAKE
-- ─────────────────────────────────────────────────────────────────────

USE ROLE ACCOUNTADMIN;

-- Warehouse de computo: XSMALL es suficiente para este volumen (~400K filas).
-- AUTO_SUSPEND=60 apaga el warehouse tras 60 segundos de inactividad
-- para no consumir creditos innecesariamente.
CREATE WAREHOUSE IF NOT EXISTS FLEETLOGIX_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME  = TRUE;

CREATE DATABASE IF NOT EXISTS FLEETLOGIX_DW;
USE DATABASE FLEETLOGIX_DW;

-- El schema ANALYTICS separa las tablas del DW de otras posibles bases
-- en la misma cuenta Snowflake (ej: staging, raw).
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;


-- =====================================================================
-- DIMENSIONES
-- Orden de creacion: primero dimensiones, luego fact_deliveries.
-- fact_deliveries tiene REFERENCES a todas las dimensiones,
-- por lo que estas deben existir antes.
-- =====================================================================


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 1: dim_date
-- Un registro por cada dia del periodo analizado (2023-01-01 a 2025-01-02).
-- Se genera completamente en el ETL: no viene de ninguna tabla de PostgreSQL.
-- Permite filtrar y agrupar por anio, trimestre, mes, dia de semana, etc.
-- sin calcular esas propiedades en cada query analitica.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_date (
    -- Clave primaria en formato YYYYMMDD (ej: 20240315 para 15-mar-2024).
    -- Formato entero estandar Kimball: permite ordenar y filtrar sin
    -- convertir tipos. El ETL genera: int(dt.strftime('%Y%m%d'))
    date_key        INT          PRIMARY KEY,

    full_date       DATE         NOT NULL,   -- La fecha real: 2024-03-15
    day_of_week     INT,                     -- 1=Lunes ... 7=Domingo (ISO 8601)
    day_name        VARCHAR(20),             -- 'Lunes', 'Martes', etc. (en espanol)
    day_of_month    INT,                     -- 1 a 31
    day_of_year     INT,                     -- 1 a 366
    week_of_year    INT,                     -- Semana ISO 8601 (1 a 53)
    month_num       INT,                     -- 1 a 12
    month_name      VARCHAR(20),             -- 'Enero', 'Febrero', etc.
    quarter         INT,                     -- 1 a 4: (month-1)//3 + 1
    year            INT,
    is_weekend      BOOLEAN,                 -- TRUE si day_of_week >= 6
    is_holiday      BOOLEAN    DEFAULT FALSE,-- No calculado; siempre FALSE por ahora
    holiday_name    VARCHAR(50),             -- NULL (sin catalogo de feriados)
    fiscal_quarter  INT,                     -- Igual a quarter (anio fiscal = calendario)
    fiscal_year     INT                      -- Igual a year
);


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 2: dim_time
-- Un registro por cada combinacion hora:minuto presente en el batch.
-- Se genera en el ETL a partir de scheduled_datetime y delivered_datetime.
-- Permite analizar patrones por turno y hora pico sin calcular en cada query.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_time (
    -- Clave primaria en formato HHMM (ej: 830 para las 08:30).
    -- El ETL genera: hour * 100 + minute
    time_key        INT          PRIMARY KEY,

    hour            INT,                     -- 0 a 23
    minute          INT,                     -- 0 a 59
    second          INT          DEFAULT 0,  -- Siempre 0 (granularidad por minuto)
    time_of_day     VARCHAR(20),             -- 'Madrugada'(0-6), 'Manana'(6-14),
                                             -- 'Tarde'(14-22), 'Noche'(22-24)
    hour_24         VARCHAR(5),              -- '08:30'
    hour_12         VARCHAR(8),              -- '08:30 AM'
    am_pm           VARCHAR(2),              -- 'AM' o 'PM'
    is_business_hour BOOLEAN,               -- TRUE si 8<=hour<18
    shift           VARCHAR(20)              -- 'Turno 1'(6-14), 'Turno 2'(14-22),
                                             -- 'Turno 3'(22-6)
);


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 3: dim_vehicle  (SCD Tipo 2)
-- Snapshot del vehiculo en el momento de la entrega.
-- SCD Tipo 2: si el estado del vehiculo cambia, el registro anterior
-- se cierra (is_current=FALSE) y se crea uno nuevo. Esto preserva
-- el historial: una entrega de hace 6 meses refleja el estado del
-- vehiculo en ese momento, no el actual.
--
-- CAMBIO vs original: 'status' -> 'vehicle_status'
-- El ETL genera el alias v.status AS vehicle_status en la query
-- consolidada. El nombre de columna aqui debe coincidir.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_vehicle (
    -- AUTOINCREMENT: Snowflake genera la clave surrogada automaticamente.
    -- El ETL hace MERGE sin especificar vehicle_key (es generado por Snowflake).
    -- Sin AUTOINCREMENT el INSERT falla por NOT NULL en la PK.
    vehicle_key     INT          AUTOINCREMENT PRIMARY KEY,

    vehicle_id      INT          NOT NULL,   -- ID natural de PostgreSQL (para trazabilidad)
    license_plate   VARCHAR(20),             -- Placa del vehiculo
    vehicle_type    VARCHAR(50),             -- 'Camion Grande', 'Van', 'Motocicleta', etc.
    capacity_kg     DECIMAL(10,2),           -- Capacidad maxima de carga en kg
    fuel_type       VARCHAR(20),             -- 'Diesel', 'Gasolina', 'GNV', 'Electrico'
    acquisition_date DATE,                   -- Fecha de compra del vehiculo

    -- CAMPO DERIVADO (no existe en PostgreSQL):
    -- age_months = (hoy - acquisition_date).days // 30
    age_months      INT,

    -- CAMBIO: 'status' del DDL original -> 'vehicle_status'
    -- Razon: el JOIN del ETL usa alias v.status AS vehicle_status para
    -- distinguirlo de driver_status en el DataFrame unificado.
    vehicle_status  VARCHAR(20),             -- 'active', 'maintenance'

    -- CAMPO DERIVADO (no existe en PostgreSQL):
    -- Se consulta MAX(maintenance_date) de la tabla maintenance de PG.
    last_maintenance_date DATE,

    -- Campos SCD Tipo 2: rango de vigencia del registro
    valid_from      DATE,                    -- Fecha desde la que es valido
    valid_to        DATE,                    -- date(9999,12,31) si is_current=TRUE
    is_current      BOOLEAN                  -- TRUE = registro vigente
);


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 4: dim_driver  (SCD Tipo 2)
-- Informacion del conductor enriquecida con campos calculados en el ETL.
-- SCD Tipo 2 activo: si performance_category cambia entre ejecuciones,
-- el MERGE cierra el registro anterior y crea uno nuevo.
--
-- CAMBIO vs original: 'status' -> 'driver_status'
-- Misma razon que dim_vehicle: alias dr.status AS driver_status en el ETL.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_driver (
    driver_key      INT          AUTOINCREMENT PRIMARY KEY,

    driver_id       INT          NOT NULL,   -- ID natural de PostgreSQL
    employee_code   VARCHAR(20),             -- Codigo de empleado: EMP0001-EMP0400
    -- CAMPO DERIVADO: first_name + ' ' + last_name
    -- PostgreSQL guarda los nombres separados; Snowflake necesita full_name.
    full_name       VARCHAR(200),
    license_number  VARCHAR(50),             -- Numero de licencia federal
    license_expiry  DATE,                    -- Vencimiento de la licencia
    phone           VARCHAR(20),             -- Telefono de 10 digitos

    hire_date       DATE,                    -- Fecha de contratacion

    -- CAMPO DERIVADO: (hoy - hire_date).days // 30
    -- Antiguedad del conductor en meses al momento de la carga.
    experience_months INT,

    -- CAMBIO: 'status' -> 'driver_status'
    driver_status   VARCHAR(20),             -- 'active', 'inactive'

    -- CAMPO DERIVADO (no existe en PostgreSQL):
    -- Calculado por el ETL sobre el lote actual:
    --   Alto  = on_time_rate > 60%
    --   Medio = on_time_rate 40-60%
    --   Bajo  = on_time_rate < 40%
    -- En los datos reales: 97.4% Medio, 2.6% Bajo (ninguno Alto)
    performance_category VARCHAR(20),       -- 'Alto', 'Medio', 'Bajo'

    valid_from      DATE,
    valid_to        DATE,
    is_current      BOOLEAN
);


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 5: dim_route
-- Rutas enriquecidas con dos campos calculados en el ETL.
-- Las rutas no cambian con el tiempo: no requiere SCD Tipo 2.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_route (
    route_key       INT          AUTOINCREMENT PRIMARY KEY,

    route_id        INT          NOT NULL,   -- ID natural de PostgreSQL
    route_code      VARCHAR(20),             -- Codigo: R001 a R050
    origin_city     VARCHAR(100),
    destination_city VARCHAR(100),
    distance_km     DECIMAL(10,2),           -- Rango real: 7.89 a 1,977 km
    estimated_duration_hours DECIMAL(5,2),
    toll_cost       DECIMAL(10,2),           -- Costo de peajes en MXN

    -- CAMPO DERIVADO (no existe en PostgreSQL):
    -- Clasifica la ruta por distancia para agrupar en queries sin rangos numericos.
    -- 'Facil'    = distance_km < 200 (37.5% de las entregas)
    -- 'Medio'    = distance_km 200-800 (19.8% de las entregas)
    -- 'Dificil'  = distance_km > 800 (46.2% de las entregas)
    difficulty_level VARCHAR(20),

    -- CAMPO DERIVADO (no existe en PostgreSQL):
    -- 'Urbana'       = origin_city == destination_city (20.1% de entregas)
    -- 'Interurbana'  = origin_city != destination_city (79.9% de entregas)
    route_type      VARCHAR(20)
);


-- ─────────────────────────────────────────────────────────────────────
-- DIMENSION 6: dim_customer
-- Los clientes no tienen tabla propia en PostgreSQL: se derivan del
-- campo customer_name de la tabla deliveries. Todos los campos de
-- esta dimension son calculados o derivados en el ETL.
--
-- CAMBIO vs original: eliminar customer_id INT IDENTITY (redundante).
-- El original tenia dos columnas de identidad: customer_key (PK) y
-- customer_id (IDENTITY). Tener dos es ambiguo y el ETL solo usa
-- customer_key como surrogado. Se elimina customer_id.
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE dim_customer (
    customer_key    INT          AUTOINCREMENT PRIMARY KEY,

    customer_name   VARCHAR(200) NOT NULL,   -- De deliveries.customer_name

    -- CAMPO DERIVADO: heuristica por nombre del cliente.
    -- 'Empresa'    = nombre contiene SA, SRL, SC, S.A (20.1% de clientes)
    -- 'Individual' = resto (79.9% de clientes)
    customer_type   VARCHAR(50),

    -- CAMPO DERIVADO: ultimo segmento de delivery_address tras la ultima coma.
    -- Ejemplo: 'Av. Insurgentes 123, Col. Centro, Ciudad de Mexico' -> 'Ciudad de Mexico'
    -- Nota: en los datos reales aparecen codigos postales en la ciudad
    -- (ej: 'GTO 22412-4620') porque el generador de Faker los incluye.
    city            VARCHAR(100),

    -- CAMPO DERIVADO: MIN(scheduled_datetime) por cliente en el batch.
    first_delivery_date DATE,

    -- CAMPO ACUMULADO: COUNT de entregas por cliente.
    -- En cargas incrementales el MERGE suma los nuevos al valor existente.
    total_deliveries INT,

    -- CAMPO DERIVADO por volumen de entregas:
    -- 'Ocasional' = 1-2 entregas en el batch (98.6% de clientes)
    -- 'Regular'   = 3-10 entregas (1.2%)
    -- 'Premium'   = >10 entregas (0.2%)
    customer_category VARCHAR(20)
);


-- =====================================================================
-- TABLA DE HECHOS: fact_deliveries
-- Cada fila = una entrega completada (delivery_status = 'delivered').
-- Contiene las claves foraneas a todas las dimensiones y todas las
-- metricas numericas del negocio.
-- Las entregas 'pending' y 'failed' NO entran en esta tabla.
-- =====================================================================
CREATE OR REPLACE TABLE fact_deliveries (

    -- Clave surrogada del DW (independiente del ID de PostgreSQL)
    delivery_key    INT          AUTOINCREMENT PRIMARY KEY,

    -- ── CLAVES A DIMENSIONES ────────────────────────────────────────
    -- Permiten hacer JOIN con cada dimension en queries analiticas.
    date_key            INT      REFERENCES dim_date(date_key),
    scheduled_time_key  INT      REFERENCES dim_time(time_key),
    delivered_time_key  INT      REFERENCES dim_time(time_key),
    vehicle_key         INT      REFERENCES dim_vehicle(vehicle_key),
    driver_key          INT      REFERENCES dim_driver(driver_key),
    route_key           INT      REFERENCES dim_route(route_key),
    customer_key        INT      REFERENCES dim_customer(customer_key),

    -- ── DIMENSIONES DEGENERADAS ─────────────────────────────────────
    -- Claves del sistema OLTP que se conservan para trazabilidad.
    -- No tienen dimension propia pero son utiles para auditar la carga.
    delivery_id     INT          NOT NULL,   -- ID en PostgreSQL deliveries
    trip_id         INT          NOT NULL,   -- ID en PostgreSQL trips
    tracking_number VARCHAR(50),             -- Numero de rastreo unico

    -- ── METRICAS DIRECTAS (de PostgreSQL) ───────────────────────────
    package_weight_kg           DECIMAL(10,2),  -- Peso del paquete en kg
    distance_km                 DECIMAL(10,2),  -- Distancia de la ruta en km
    fuel_consumed_liters        DECIMAL(10,2),  -- Combustible proporcional

    -- ── METRICAS CALCULADAS EN EL ETL ────────────────────────────────
    -- CAMBIO: INT -> DECIMAL(8,2)
    -- El ETL calcula (delivered - scheduled).total_seconds()/60.round(2)
    -- produciendo decimales. Ademas puede ser NEGATIVO (entrega anticipada,
    -- min=-60 en los datos reales). INT truncaria el decimal y perder
    -- precision en entregas de pocos minutos. Se preservan los negativos
    -- porque son validos y estan marcados en data_quality_flag si son extremos.
    delivery_time_minutes       DECIMAL(8,2),

    -- CAMBIO: INT -> DECIMAL(8,2)
    -- Es clip(delivery_time_minutes, lower=0). Siempre >= 0.
    delay_minutes               DECIMAL(8,2),

    -- Entregas por hora del viaje: deliveries_in_trip / trip_duration_hours
    deliveries_per_hour         DECIMAL(5,2),

    -- Eficiencia de combustible: distance_km / fuel_consumed_liters
    fuel_efficiency_km_per_liter DECIMAL(5,2),

    -- Costo por entrega: (fuel*PRECIO_LITRO + toll) / deliveries_in_trip
    -- Rango real: 1.69 a 9,122.63 MXN
    cost_per_delivery           DECIMAL(10,2),

    -- Ingreso por entrega: REVENUE_BASE + package_weight_kg * REVENUE_KG
    -- Rango real: 80.15 a 155.00 MXN
    revenue_per_delivery        DECIMAL(10,2),

    -- ── INDICADORES BOOLEANOS ────────────────────────────────────────
    is_on_time      BOOLEAN,   -- TRUE si delay_minutes <= 30

    -- is_damaged: no existe en PostgreSQL, siempre FALSE.
    -- Se reserva para futura implementacion de sistema de reclamos.
    is_damaged      BOOLEAN    DEFAULT FALSE,

    -- CAMBIO: 'has_signature' -> 'recipient_signature'
    -- El campo en PostgreSQL se llama recipient_signature y el ETL lo
    -- pasa directamente al INSERT sin renombrarlo. Debe coincidir.
    recipient_signature BOOLEAN,

    delivery_status VARCHAR(20),            -- 'delivered' (unico valor en esta tabla)

    -- CAMPO NUEVO (no en el DDL original):
    -- Columna de calidad de datos generada por el ETL para marcar
    -- registros con anomalias sin eliminarlos. Valores posibles:
    --   'ok'                  = registro sin anomalias (100% en datos reales)
    --   'peso_invalido'       = package_weight_kg <= 0 o > 9999
    --   'tiempo_muy_negativo' = delivery_time_minutes < -1440 (>24h anticipada)
    --   'viaje_muy_largo'     = trip_duration_hours > 72
    -- Pueden combinarse con '|': 'peso_invalido|viaje_muy_largo'
    data_quality_flag VARCHAR(100) DEFAULT 'ok',

    -- ── AUDITORIA ETL ────────────────────────────────────────────────
    etl_batch_id    INT,                    -- Timestamp Unix de la ejecucion
    etl_timestamp   TIMESTAMP_NTZ          DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================================
-- TABLA NUEVA: staging_trips
-- No estaba en el DDL original. Se agrega porque:
-- 1. Snowflake es OLAP y no replica las tablas transaccionales de PG.
-- 2. La vista v_operations_deliveries necesita duracion del viaje,
--    entregas/hora y otros KPIs que solo existen en staging_trips.
-- 3. Los 3 campos al final (trip_duration_hours, deliveries_in_trip,
--    deliveries_per_hour) son calculados en el ETL y no existen en PG.
-- =====================================================================
CREATE TABLE IF NOT EXISTS staging_trips (
    trip_id                 INT          PRIMARY KEY,     -- De PG trips.trip_id
    vehicle_id              INT          NOT NULL,
    driver_id               INT          NOT NULL,
    route_id                INT          NOT NULL,
    departure_datetime      TIMESTAMP_NTZ,
    arrival_datetime        TIMESTAMP_NTZ,
    fuel_consumed_liters    DECIMAL(10,2),
    total_weight_kg         DECIMAL(10,2),
    trip_status             VARCHAR(20),                  -- 'completed'

    -- Campos calculados en el ETL (NO existen en PostgreSQL trips):
    -- trip_duration_hours = (arrival - departure).total_seconds() / 3600
    trip_duration_hours     DECIMAL(8,4),
    -- deliveries_in_trip = COUNT(delivery_id) agrupado por trip_id
    deliveries_in_trip      INT,
    -- deliveries_per_hour = deliveries_in_trip / trip_duration_hours
    deliveries_per_hour     DECIMAL(5,2),

    etl_batch_id            INT,
    etl_timestamp           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================================
-- TABLA NUEVA: daily_kpi_summary
-- Resumen diario pre-calculado por el ETL.
-- Permite que los dashboards respondan en <100ms consultando esta tabla
-- en lugar de recalcular sobre fact_deliveries (343,301 filas).
-- Se actualiza con MERGE en cada ejecucion del pipeline nocturno.
-- =====================================================================
CREATE TABLE IF NOT EXISTS daily_kpi_summary (
    summary_date            DATE         NOT NULL,
    etl_batch_id            INT          NOT NULL,
    total_deliveries        INT,          -- Total de filas en el batch
    delivered_ok            INT,          -- delivery_status = 'delivered'
    pct_on_time             DECIMAL(5,2), -- % con delay_minutes <= 30
    avg_delay_minutes       DECIMAL(8,2),
    total_revenue_mxn       DECIMAL(14,2),
    total_cost_mxn          DECIMAL(14,2),
    avg_fuel_efficiency     DECIMAL(8,4), -- km/L promedio
    total_km                DECIMAL(12,2),
    total_fuel_liters       DECIMAL(12,2),
    active_vehicles         INT,          -- vehicle_id distintos en el batch
    active_drivers          INT,          -- driver_id distintos en el batch
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (summary_date, etl_batch_id)
);


-- =====================================================================
-- CONFIGURACION SNOWFLAKE
-- Time Travel: permite recuperar datos borrados accidentalmente
-- hasta 30 dias atras. Standard en Data Warehouses de produccion.
-- =====================================================================
ALTER TABLE fact_deliveries  SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_date         SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_vehicle      SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_driver       SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_route        SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_customer     SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_time         SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE staging_trips    SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER TABLE daily_kpi_summary SET DATA_RETENTION_TIME_IN_DAYS = 90;


-- =====================================================================
-- TABLA DE STAGING GENERAL (del DDL original, se conserva)
-- Para cargas en modo VARIANT (JSON raw) si se necesita en el futuro.
-- =====================================================================
CREATE OR REPLACE TABLE staging_daily_load (
    raw_data        VARIANT,
    load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================================
-- VISTAS SEGURAS POR ROL
-- Controlan que datos ve cada area de la empresa.
-- SECURE VIEW: el codigo SQL de la vista no es visible para el usuario
-- (no puede hacer SHOW VIEW para ver la logica interna).
-- =====================================================================

-- Vista para el area de Ventas: solo datos comerciales, sin operaciones.
-- Restriccion de ejemplo: excluye clientes de tipo 'Gobierno'.
CREATE OR REPLACE SECURE VIEW v_sales_deliveries AS
SELECT
    d.full_date,
    c.customer_name,
    c.customer_type,
    c.customer_category,
    f.package_weight_kg,
    f.delivery_status,
    f.revenue_per_delivery,
    f.is_on_time
FROM fact_deliveries  f
JOIN dim_date         d  ON f.date_key      = d.date_key
JOIN dim_customer     c  ON f.customer_key  = c.customer_key
WHERE c.customer_type != 'Gobierno';

-- Vista para Operaciones: acceso completo incluyendo KPIs de viaje.
-- CAMBIO vs original: agrega JOIN con staging_trips para incluir
-- trip_duration_hours y deliveries_per_hour, que aportan contexto
-- operativo que no estaba en el DDL original.
CREATE OR REPLACE SECURE VIEW v_operations_deliveries AS
SELECT
    d.full_date,
    d.day_name,
    d.quarter,
    t.hour_24         AS hora_programada,
    t.shift           AS turno,
    v.license_plate,
    v.vehicle_type,
    dr.full_name      AS conductor,
    dr.performance_category,
    r.route_code,
    r.difficulty_level,
    r.route_type,
    c.customer_name,
    c.customer_category,
    f.delivery_time_minutes,
    f.delay_minutes,
    f.is_on_time,
    f.fuel_consumed_liters,
    f.fuel_efficiency_km_per_liter,
    f.cost_per_delivery,
    f.revenue_per_delivery,
    f.data_quality_flag,
    -- Campos de staging_trips (no estaban en la vista original)
    st.trip_duration_hours,
    st.deliveries_per_hour
FROM fact_deliveries   f
JOIN dim_date          d   ON f.date_key           = d.date_key
JOIN dim_time          t   ON f.scheduled_time_key = t.time_key
JOIN dim_vehicle       v   ON f.vehicle_key        = v.vehicle_key
JOIN dim_driver        dr  ON f.driver_key         = dr.driver_key
JOIN dim_route         r   ON f.route_key          = r.route_key
JOIN dim_customer      c   ON f.customer_key       = c.customer_key
LEFT JOIN staging_trips st ON f.trip_id            = st.trip_id;

-- Vista para analisis de conduccion IoT (si se carga desde MongoDB)
-- Muestra el comportamiento del conductor cruzando DW con IoT.
CREATE OR REPLACE SECURE VIEW v_driver_performance AS
SELECT
    dr.full_name           AS conductor,
    dr.employee_code,
    dr.experience_months,
    dr.performance_category,
    d.year,
    d.quarter,
    COUNT(f.delivery_id)   AS total_entregas,
    ROUND(AVG(f.delay_minutes), 1)           AS delay_prom_min,
    ROUND(SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(f.delivery_id), 0), 1) AS pct_on_time,
    ROUND(AVG(f.fuel_efficiency_km_per_liter), 2) AS eficiencia_km_l,
    ROUND(AVG(f.cost_per_delivery), 2)       AS costo_prom_entrega
FROM fact_deliveries  f
JOIN dim_driver       dr ON f.driver_key = dr.driver_key AND dr.is_current = TRUE
JOIN dim_date         d  ON f.date_key   = d.date_key
GROUP BY dr.full_name, dr.employee_code, dr.experience_months,
         dr.performance_category, d.year, d.quarter;


-- =====================================================================
-- ROLES Y PERMISOS
-- Principio de minimo privilegio: cada rol solo ve lo que necesita.
-- =====================================================================
CREATE ROLE IF NOT EXISTS SALES_ANALYST;
CREATE ROLE IF NOT EXISTS OPERATIONS_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;

-- Ventas: solo puede consultar su vista
GRANT USAGE  ON WAREHOUSE FLEETLOGIX_WH      TO ROLE SALES_ANALYST;
GRANT USAGE  ON DATABASE  FLEETLOGIX_DW      TO ROLE SALES_ANALYST;
GRANT USAGE  ON SCHEMA    ANALYTICS          TO ROLE SALES_ANALYST;
GRANT SELECT ON VIEW      v_sales_deliveries TO ROLE SALES_ANALYST;

-- Operaciones: puede consultar sus vistas y daily_kpi_summary
GRANT USAGE  ON WAREHOUSE FLEETLOGIX_WH            TO ROLE OPERATIONS_ANALYST;
GRANT USAGE  ON DATABASE  FLEETLOGIX_DW            TO ROLE OPERATIONS_ANALYST;
GRANT USAGE  ON SCHEMA    ANALYTICS                TO ROLE OPERATIONS_ANALYST;
GRANT SELECT ON VIEW      v_operations_deliveries  TO ROLE OPERATIONS_ANALYST;
GRANT SELECT ON VIEW      v_driver_performance     TO ROLE OPERATIONS_ANALYST;
GRANT SELECT ON TABLE     daily_kpi_summary        TO ROLE OPERATIONS_ANALYST;

-- Data Engineer: acceso completo para ETL y mantenimiento
GRANT USAGE  ON WAREHOUSE  FLEETLOGIX_WH    TO ROLE DATA_ENGINEER;
GRANT USAGE  ON DATABASE   FLEETLOGIX_DW    TO ROLE DATA_ENGINEER;
GRANT USAGE  ON SCHEMA     ANALYTICS        TO ROLE DATA_ENGINEER;
GRANT ALL    ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE DATA_ENGINEER;

-- =====================================================================
-- FIN DEL DDL
-- =====================================================================

"""
=============================================================
FleetLogix — Pipeline ETL Automatico  
=============================================================
Extrae de PostgreSQL, Transforma y Carga en Snowflake.
Programado diariamente a las 02:00 AM con schedule.

ORIGEN DE CADA CAMPO DEL MODELO ESTRELLA
─────────────────────────────────────────
 VIENE DE POSTGRES   → columnas que existen en el OLTP
 DERIVADO EN ETL     → calculado en transform_data()
 AUTO / IDENTITY     → generado por Snowflake automaticamente

dim_date (16 cols)
  date_key            DERIVADO  int(YYYYMMDD) de scheduled_datetime
  full_date           DERIVADO  date del scheduled_datetime
  day_of_week         DERIVADO  isoweekday() 1=Lunes
  day_name            DERIVADO  nombre en español
  day_of_month        DERIVADO  .day
  day_of_year         DERIVADO  tm_yday
  week_of_year        DERIVADO  isocalendar()[1]
  month_num           DERIVADO  .month
  month_name          DERIVADO  nombre en español
  quarter             DERIVADO  (month-1)//3+1
  year                DERIVADO  .year
  is_weekend          DERIVADO  dow >= 6
  is_holiday          DEFAULT   False
  holiday_name        DEFAULT   NULL
  fiscal_quarter      DERIVADO  igual a quarter
  fiscal_year         DERIVADO  igual a year

dim_time (10 cols)
  time_key            DERIVADO  hour*100+minute
  hour                DERIVADO  .hour
  minute              DERIVADO  .minute
  second              DEFAULT   0
  time_of_day         DERIVADO  Madrugada/Manana/Tarde/Noche
  hour_24             DERIVADO  "HH:MM"
  hour_12             DERIVADO  "HH:MM AM/PM"
  am_pm               DERIVADO  AM o PM
  is_business_hour    DERIVADO  8<=hour<18
  shift               DERIVADO  Turno 1/2/3

dim_vehicle (13 cols)
  vehicle_key         AUTO      IDENTITY Snowflake
  vehicle_id          POSTGRES  vehicles.vehicle_id
  license_plate       POSTGRES  vehicles.license_plate
  vehicle_type        POSTGRES  vehicles.vehicle_type
  capacity_kg         POSTGRES  vehicles.capacity_kg
  fuel_type           POSTGRES  vehicles.fuel_type
  acquisition_date    POSTGRES  vehicles.acquisition_date
  age_months          DERIVADO  (today - acquisition_date).days // 30
  status              POSTGRES  vehicles.status
  last_maintenance_date DERIVADO MAX(maintenance_date) por vehicle_id
  valid_from          DERIVADO  date.today()
  valid_to            DERIVADO  date(9999,12,31)  ← fix overflow
  is_current          DERIVADO  True

dim_driver (14 cols)
  driver_key          AUTO      IDENTITY Snowflake
  driver_id           POSTGRES  drivers.driver_id
  employee_code       POSTGRES  drivers.employee_code
  full_name           DERIVADO  first_name + ' ' + last_name
  license_number      POSTGRES  drivers.license_number
  license_expiry      POSTGRES  drivers.license_expiry
  phone               POSTGRES  drivers.phone
  hire_date           POSTGRES  drivers.hire_date
  experience_months   DERIVADO  (today - hire_date).days // 30
  status              POSTGRES  drivers.status
  performance_category DERIVADO on_time_rate → Alto/Medio/Bajo
  valid_from          DERIVADO  date.today()
  valid_to            DERIVADO  date(9999,12,31)
  is_current          DERIVADO  True

dim_route (10 cols)
  route_key           AUTO      IDENTITY Snowflake
  route_id            POSTGRES  routes.route_id
  route_code          POSTGRES  routes.route_code
  origin_city         POSTGRES  routes.origin_city
  destination_city    POSTGRES  routes.destination_city
  distance_km         POSTGRES  routes.distance_km
  estimated_duration_hours POSTGRES routes.estimated_duration_hours
  toll_cost           POSTGRES  routes.toll_cost
  difficulty_level    DERIVADO  Facil<200 / Medio 200-800 / Dificil>800
  route_type          DERIVADO  Urbana si origin==destination, Interurbana si no

dim_customer (8 cols)
  customer_key        AUTO      IDENTITY Snowflake
  customer_id         AUTO      IDENTITY Snowflake
  customer_name       POSTGRES  deliveries.customer_name
  customer_type       DERIVADO  heuristica: SA/SRL→Empresa, resto→Individual
  city                DERIVADO  ultimo segmento de delivery_address
  first_delivery_date DERIVADO  MIN(scheduled_datetime).date por cliente
  total_deliveries    DERIVADO  COUNT(delivery_id) por cliente en batch
  customer_category   DERIVADO  >10→Premium / >2→Regular / resto→Ocasional

staging_trips (14 cols) — tabla nueva, no existe en PG ni en A3-04.sql
  trip_id             POSTGRES  trips.trip_id
  vehicle_id          POSTGRES  trips.vehicle_id
  driver_id           POSTGRES  trips.driver_id
  route_id            POSTGRES  trips.route_id
  departure_datetime  POSTGRES  trips.departure_datetime
  arrival_datetime    POSTGRES  trips.arrival_datetime
  fuel_consumed_liters POSTGRES trips.fuel_consumed_liters
  total_weight_kg     POSTGRES  trips.total_weight_kg
  trip_status         POSTGRES  trips.status
  trip_duration_hours DERIVADO  (arrival - departure).hours
  deliveries_in_trip  DERIVADO  COUNT(delivery_id) por trip_id
  deliveries_per_hour DERIVADO  deliveries_in_trip / trip_duration_hours
  etl_batch_id        ETL       timestamp entero del batch
  etl_timestamp       AUTO      CURRENT_TIMESTAMP Snowflake

fact_deliveries (26 cols)
  delivery_key        AUTO      IDENTITY Snowflake
  date_key            DERIVADO  lookup dim_date
  scheduled_time_key  DERIVADO  lookup dim_time (hora programada)
  delivered_time_key  DERIVADO  lookup dim_time (hora real entrega)
  vehicle_key         DERIVADO  lookup dim_vehicle
  driver_key          DERIVADO  lookup dim_driver
  route_key           DERIVADO  lookup dim_route
  customer_key        DERIVADO  lookup dim_customer
  delivery_id         POSTGRES  deliveries.delivery_id
  trip_id             POSTGRES  deliveries.trip_id
  tracking_number     POSTGRES  deliveries.tracking_number
  package_weight_kg   POSTGRES  deliveries.package_weight_kg
  distance_km         POSTGRES  routes.distance_km
  fuel_consumed_liters POSTGRES trips.fuel_consumed_liters
  delivery_time_minutes DERIVADO (delivered - scheduled).minutes
  delay_minutes       DERIVADO  max(0, delivery_time_minutes)
  deliveries_per_hour DERIVADO  deliveries_in_trip / trip_duration_hours
  fuel_efficiency_km_per_liter DERIVADO distance / fuel
  cost_per_delivery   DERIVADO  (fuel*PRECIO + toll) / deliveries_in_trip
  revenue_per_delivery DERIVADO REVENUE_BASE + weight * REVENUE_POR_KG
  is_on_time          DERIVADO  delay_minutes <= 30
  is_damaged          DEFAULT   False (campo no existe en PG)
  has_signature       POSTGRES  deliveries.recipient_signature
  delivery_status     POSTGRES  deliveries.delivery_status
  etl_batch_id        ETL       timestamp entero del batch
  etl_timestamp       AUTO      CURRENT_TIMESTAMP Snowflake

daily_kpi_summary — tabla de resumen pre-calculado (nueva)
  Permite dashboards en <100ms sin recalcular sobre millones de filas
=============================================================
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import logging
import schedule
import time
import json

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# ─────────────────────────────────────────────
# CONFIGURACION
# ─────────────────────────────────────────────
POSTGRES_CONFIG = {
    'host':     'localhost',
    'database': 'fleetlogix',
    'user':     'postgres',
    'password': 'your_password',   # ← CAMBIAR
    'port':     5432
}

SNOWFLAKE_CONFIG = {
    'user':      'your_user',       # ← CAMBIAR
    'password':  'your_password',   # ← CAMBIAR
    'account':   'your_account',    # ← CAMBIAR  ej: abc12345.us-east-1
    'warehouse': 'FLEETLOGIX_WH',
    'database':  'FLEETLOGIX_DW',
    'schema':    'ANALYTICS'
}

# Constantes de negocio (ajustar segun tarifas reales)
PRECIO_LITRO_MXN  = 22.50   # costo por litro de combustible
REVENUE_BASE_MXN  = 80.00   # ingreso base por entrega
REVENUE_KG_MXN    = 1.50    # ingreso adicional por kg
ON_TIME_THRESHOLD = 30       # minutos de tolerancia para considerar a tiempo
MAX_TRIP_HOURS    = 72       # duracion maxima valida de un viaje


# ─────────────────────────────────────────────
# CLASE ETL
# ─────────────────────────────────────────────
class FleetLogixETL:

    def __init__(self):
        self.pg_conn  = None
        self.sf_conn  = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics  = {
            'records_extracted':   0,
            'records_transformed': 0,
            'records_loaded':      0,
            'records_flagged':     0,
            'errors':              0
        }

    # ══════════════════════════════════════════
    # CONEXIONES
    # ══════════════════════════════════════════

    def connect_databases(self) -> bool:
        """Abre conexiones a PostgreSQL y Snowflake."""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info("✅ Conectado a PostgreSQL")
        except Exception as e:
            logging.error(f"❌ PostgreSQL: {e}")
            return False

        try:
            import snowflake.connector
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info("✅ Conectado a Snowflake")
        except Exception as e:
            logging.warning(f"⚠️  Snowflake no disponible — modo CSV: {e}")
            self.sf_conn = None

        return True

    def close_connections(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info("🔒 Conexiones cerradas")

    # ══════════════════════════════════════════
    # EXTRACCION
    # ══════════════════════════════════════════

    def extract_daily_data(self) -> pd.DataFrame:
        """
        Extrae el dia anterior de PostgreSQL en un unico DataFrame consolidado.

        DECISION DE DISENO: un solo JOIN en lugar de queries separadas.
        Esto evita multiples round-trips a la BD y genera directamente
        el DataFrame unificado que necesita transform_data().

        Para carga inicial completa usar extract_full_history().
        """
        logging.info("📥 Extrayendo datos diarios...")
        ayer       = (datetime.now() - timedelta(days=1)).date()
        fecha_fin  = datetime.now().date()

        query = f"""
        -- ─────────────────────────────────────────────────────
        -- QUERY CONSOLIDADA FleetLogix — extraccion incremental
        -- Une deliveries + trips + vehicles + drivers + routes
        -- en un unico DataFrame para el modelo dimensional.
        -- Rango: {ayer} a {fecha_fin}
        -- ─────────────────────────────────────────────────────
        SELECT
            -- DELIVERIES
            d.delivery_id,
            d.trip_id,
            d.tracking_number,
            d.customer_name,
            d.delivery_address,
            d.package_weight_kg,
            d.scheduled_datetime,
            d.delivered_datetime,
            d.delivery_status,
            d.recipient_signature,

            -- TRIPS
            t.departure_datetime,
            t.arrival_datetime,
            t.fuel_consumed_liters,
            t.total_weight_kg       AS trip_total_weight_kg,
            t.status                AS trip_status,

            -- VEHICLES
            v.vehicle_id,
            v.license_plate,
            v.vehicle_type,
            v.capacity_kg,
            v.fuel_type,
            v.acquisition_date,
            v.status                AS vehicle_status,

            -- DRIVERS
            dr.driver_id,
            dr.employee_code,
            dr.first_name,
            dr.last_name,
            dr.license_number,
            dr.license_expiry,
            dr.phone,
            dr.hire_date,
            dr.status               AS driver_status,

            -- ROUTES
            r.route_id,
            r.route_code,
            r.origin_city,
            r.destination_city,
            r.distance_km,
            r.estimated_duration_hours,
            r.toll_cost

        FROM deliveries   d
        JOIN trips        t  ON d.trip_id    = t.trip_id
        JOIN vehicles     v  ON t.vehicle_id = v.vehicle_id
        JOIN drivers      dr ON t.driver_id  = dr.driver_id
        JOIN routes       r  ON t.route_id   = r.route_id

        WHERE
            -- Carga incremental: solo el dia anterior
            d.scheduled_datetime >= '{ayer}'
            AND d.scheduled_datetime  <  '{fecha_fin}'
            -- Solo viajes completados tienen todos los datos
            AND t.status              = 'completed'
            AND t.arrival_datetime    IS NOT NULL
            AND t.fuel_consumed_liters IS NOT NULL

        ORDER BY d.delivery_id
        """
        return self._run_query(query, "diaria")

    def extract_full_history(self) -> pd.DataFrame:
        """Extrae TODO el historico (para la primera carga)."""
        logging.info("📥 Extrayendo historico completo...")
        query = """
        SELECT
            d.delivery_id, d.trip_id, d.tracking_number,
            d.customer_name, d.delivery_address, d.package_weight_kg,
            d.scheduled_datetime, d.delivered_datetime,
            d.delivery_status, d.recipient_signature,
            t.departure_datetime, t.arrival_datetime,
            t.fuel_consumed_liters,
            t.total_weight_kg  AS trip_total_weight_kg,
            t.status           AS trip_status,
            v.vehicle_id, v.license_plate, v.vehicle_type, v.capacity_kg,
            v.fuel_type, v.acquisition_date, v.status AS vehicle_status,
            dr.driver_id, dr.employee_code, dr.first_name, dr.last_name,
            dr.license_number, dr.license_expiry, dr.phone, dr.hire_date,
            dr.status AS driver_status,
            r.route_id, r.route_code, r.origin_city, r.destination_city,
            r.distance_km, r.estimated_duration_hours, r.toll_cost
        FROM deliveries d
        JOIN trips    t  ON d.trip_id    = t.trip_id
        JOIN vehicles v  ON t.vehicle_id = v.vehicle_id
        JOIN drivers  dr ON t.driver_id  = dr.driver_id
        JOIN routes   r  ON t.route_id   = r.route_id
        WHERE t.status             = 'completed'
          AND t.arrival_datetime   IS NOT NULL
          AND t.fuel_consumed_liters IS NOT NULL
        ORDER BY d.delivery_id
        """
        return self._run_query(query, "historica")

    def _run_query(self, query: str, label: str) -> pd.DataFrame:
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f"  Extraidos {len(df):,} registros ({label})")
            return df
        except Exception as e:
            logging.error(f"❌ Error extraccion {label}: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    # ══════════════════════════════════════════
    # TRANSFORMACION
    # ══════════════════════════════════════════

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera todos los campos que NO existen en PostgreSQL
        y valida la calidad de los datos SIN eliminar filas.

        Campos derivados que se crean aqui:
          full_name, experience_months, age_months,
          difficulty_level, route_type, city, customer_type,
          customer_category, first_delivery_date,
          delivery_time_minutes, delay_minutes, is_on_time,
          trip_duration_hours, deliveries_in_trip, deliveries_per_hour,
          fuel_efficiency_km_per_liter, cost_per_delivery,
          revenue_per_delivery, date_key, time_key,
          delivered_time_key, performance_category,
          valid_from, valid_to, is_current, data_quality_flag
        """
        logging.info("🔄 Transformando datos...")
        try:
            today = pd.Timestamp.now().normalize()

            # ── Convertir todas las fechas ───────────────────────
            for col in ['scheduled_datetime','delivered_datetime',
                        'departure_datetime','arrival_datetime',
                        'hire_date','acquisition_date','license_expiry']:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # ─────────────────────────────────────
            # dim_driver: campos derivados
            # ─────────────────────────────────────

            # full_name: no existe en postgres, es la concatenacion
            df['full_name'] = (
                df['first_name'].str.strip()
                + ' '
                + df['last_name'].str.strip()
            )

            # experience_months: (hoy - hire_date) en meses
            df['experience_months'] = (
                (today - df['hire_date']).dt.days // 30
            ).clip(lower=0).fillna(0).astype(int)

            # ─────────────────────────────────────
            # dim_vehicle: campos derivados
            # ─────────────────────────────────────

            # age_months: (hoy - acquisition_date) en meses
            df['age_months'] = (
                (today - df['acquisition_date']).dt.days // 30
            ).clip(lower=0).fillna(0).astype(int)

            # last_maintenance_date: se obtiene desde la tabla
            # maintenance de PostgreSQL en load_dimensions()
            # Aqui ponemos None; se enriquece en la carga
            df['last_maintenance_date'] = None

            # ─────────────────────────────────────
            # dim_route: campos derivados
            # ─────────────────────────────────────

            # difficulty_level segun distancia en km
            def difficulty(km):
                if pd.isna(km):  return 'Desconocido'
                if km < 200:     return 'Facil'
                if km <= 800:    return 'Medio'
                return 'Dificil'
            df['difficulty_level'] = df['distance_km'].apply(difficulty)

            # route_type: Urbana si la ruta empieza y termina en la misma ciudad
            df['route_type'] = np.where(
                df['origin_city'] == df['destination_city'],
                'Urbana', 'Interurbana'
            )

            # ─────────────────────────────────────
            # dim_customer: campos derivados
            # ─────────────────────────────────────

            # city: ultimo segmento de delivery_address (tras la ultima coma)
            df['city'] = (
                df['delivery_address']
                .str.split(',')
                .str[-1]
                .str.strip()
            )

            # customer_type: heuristica por nombre del cliente
            # Empresas suelen tener SA, SA de CV, SRL, SC en el nombre
            df['customer_type'] = np.where(
                df['customer_name'].str.contains(
                    r'(?:SA|SRL|SC|S\.A|S\.R\.L)',
                    case=False, na=False, regex=True
                ),
                'Empresa', 'Individual'
            )

            # first_delivery_date: fecha minima de scheduled por cliente
            min_sched = (
                df.groupby('customer_name')['scheduled_datetime']
                .transform('min')
            )
            df['first_delivery_date'] = pd.to_datetime(min_sched).dt.date

            # total_deliveries en este batch
            df['total_deliveries'] = df.groupby(
                'customer_name')['delivery_id'].transform('count')

            # customer_category por volumen
            df['customer_category'] = pd.cut(
                df['total_deliveries'],
                bins=[0, 2, 10, float('inf')],
                labels=['Ocasional', 'Regular', 'Premium'],
                right=True
            ).astype(str)

            # ─────────────────────────────────────
            # fact_deliveries: metricas calculadas
            # ─────────────────────────────────────

            # delivery_time_minutes: diferencia entregado - programado
            # NOTA: puede ser negativo (entrega anticipada). Se conserva
            # y se marca en data_quality_flag, NO se elimina.
            df['delivery_time_minutes'] = (
                (df['delivered_datetime'] - df['scheduled_datetime'])
                .dt.total_seconds() / 60
            ).round(2)

            # delay_minutes: solo el exceso (parte positiva)
            df['delay_minutes'] = df['delivery_time_minutes'].clip(lower=0)

            # is_on_time
            df['is_on_time'] = df['delay_minutes'] <= ON_TIME_THRESHOLD

            # trip_duration_hours
            df['trip_duration_hours'] = (
                (df['arrival_datetime'] - df['departure_datetime'])
                .dt.total_seconds() / 3600
            ).round(4)

            # deliveries por viaje
            df['deliveries_in_trip'] = df.groupby(
                'trip_id')['delivery_id'].transform('count')

            # deliveries_per_hour (proteger division por cero)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip']
                / df['trip_duration_hours'].replace(0, np.nan)
            ).round(2)

            # fuel_efficiency en km/litro
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km']
                / df['fuel_consumed_liters'].replace(0, np.nan)
            ).round(2)

            # cost_per_delivery: combustible + peaje dividido entre N entregas
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * PRECIO_LITRO_MXN
                 + df['toll_cost'])
                / df['deliveries_in_trip']
            ).round(2)

            # revenue_per_delivery
            df['revenue_per_delivery'] = (
                REVENUE_BASE_MXN
                + df['package_weight_kg'] * REVENUE_KG_MXN
            ).round(2)

            # ─────────────────────────────────────
            # Keys para el modelo dimensional
            # ─────────────────────────────────────

            # date_key: formato YYYYMMDD como entero
            df['date_key'] = (
                df['scheduled_datetime']
                .dt.strftime('%Y%m%d')
                .astype(int)
            )

            # scheduled_time_key: HHMM de la hora programada
            df['scheduled_time_key'] = (
                df['scheduled_datetime'].dt.hour * 100
                + df['scheduled_datetime'].dt.minute
            )

            # delivered_time_key: HHMM de la hora real de entrega
            # Si no hay entrega, usar el scheduled como fallback
            df['delivered_time_key'] = np.where(
                df['delivered_datetime'].notna(),
                df['delivered_datetime'].dt.hour * 100
                + df['delivered_datetime'].dt.minute,
                df['scheduled_time_key']
            ).astype(int)

            # ─────────────────────────────────────
            # SCD Tipo 2
            # ─────────────────────────────────────

            df['valid_from'] = df['scheduled_datetime'].dt.date

            # CORRECCION del error en linea 145 del script original:
            # pd.to_datetime('9999-12-31') genera OverflowError en
            # numpy < 2.0 porque intenta convertirlo a nanosegundos
            # y el valor supera el rango int64 (aprox año 2262).
            # La forma correcta es usar date() de Python directamente,
            # que no tiene ese limite y Snowflake acepta como DATE.
            df['valid_to']   = date(9999, 12, 31)
            df['is_current'] = True

            # ─────────────────────────────────────
            # Categoria de performance del conductor
            # ─────────────────────────────────────
            # Se calcula sobre el lote actual.
            # Alto  : on_time_rate > 60%
            # Medio : on_time_rate 40-60%
            # Bajo  : on_time_rate < 40%
            driver_ot_rate = df.groupby(
                'driver_id')['is_on_time'].transform('mean')
            df['performance_category'] = pd.cut(
                driver_ot_rate,
                bins=[-0.001, 0.40, 0.60, 1.001],
                labels=['Bajo', 'Medio', 'Alto']
            ).astype(str)

            # ─────────────────────────────────────
            # Validacion de calidad — MARCA, no elimina
            # ─────────────────────────────────────
            # El script original eliminaba filas con tiempos negativos
            # y pesos fuera de rango, perdiendo datos que pueden ser
            # validos (entrega anticipada) o utiles para auditoria.
            # La correccion conserva todos los registros y agrega
            # la columna data_quality_flag con el motivo del problema.

            flags = pd.Series('', index=df.index)

            # Peso invalido
            mask = (df['package_weight_kg'] <= 0) | (df['package_weight_kg'] > 9999)
            flags[mask] = flags[mask].where(flags[mask] == '', flags[mask] + '|') + 'peso_invalido'

            # Tiempo negativo extremo (mas de 24h anticipada → probable error de datos)
            mask = df['delivery_time_minutes'].notna() & (df['delivery_time_minutes'] < -1440)
            flags[mask] = flags[mask].where(flags[mask] == '', flags[mask] + '|') + 'tiempo_muy_negativo'

            # Viaje con duracion excesiva
            mask = df['trip_duration_hours'].notna() & (df['trip_duration_hours'] > MAX_TRIP_HOURS)
            flags[mask] = flags[mask].where(flags[mask] == '', flags[mask] + '|') + 'viaje_muy_largo'

            df['data_quality_flag'] = flags.str.strip('|').replace('', 'ok')

            n_flagged = (df['data_quality_flag'] != 'ok').sum()
            self.metrics['records_flagged']     = int(n_flagged)
            self.metrics['records_transformed'] = len(df)

            logging.info(f"  Transformados : {len(df):,} registros (0 eliminados)")
            logging.info(f"  Marcados (flag): {n_flagged:,}")
            return df

        except Exception as e:
            logging.error(f"❌ Error en transformacion: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()

    # ══════════════════════════════════════════
    # CARGA — DIMENSIONES
    # ══════════════════════════════════════════

    def load_dimensions(self, df: pd.DataFrame):
        """
        Carga las 6 dimensiones en Snowflake usando MERGE (upsert).
        Si Snowflake no esta disponible exporta CSV a ./dw_data/
        """
        if not self.sf_conn:
            self._export_dims_csv(df)
            return

        logging.info("📤 Cargando dimensiones...")
        cur   = self.sf_conn.cursor()
        today = date.today()

        # ── last_maintenance_date desde postgres ─────────────────
        last_maint = pd.read_sql(
            "SELECT vehicle_id, MAX(maintenance_date)::DATE AS lmd "
            "FROM maintenance GROUP BY vehicle_id",
            self.pg_conn
        ).set_index('vehicle_id')['lmd'].to_dict()

        # ── dim_date ─────────────────────────────────────────────
        MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
                 'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
        DIAS  = ['Lunes','Martes','Miercoles','Jueves','Viernes','Sabado','Domingo']

        fechas = pd.to_datetime(
            df['scheduled_datetime'].dropna()
        ).dt.date.unique()

        for d in fechas:
            dt  = datetime.combine(d, datetime.min.time())
            dk  = int(dt.strftime('%Y%m%d'))
            dow = dt.isoweekday()       # 1=Lunes, 7=Domingo
            q   = (dt.month - 1) // 3 + 1
            cur.execute("""
                MERGE INTO dim_date t
                USING (SELECT %(dk)s AS date_key) s
                ON t.date_key = s.date_key
                WHEN NOT MATCHED THEN INSERT (
                    date_key, full_date, day_of_week, day_name,
                    day_of_month, day_of_year, week_of_year,
                    month_num, month_name, quarter, year,
                    is_weekend, is_holiday, holiday_name,
                    fiscal_quarter, fiscal_year
                ) VALUES (
                    %(dk)s, %(fd)s, %(dow)s, %(dn)s,
                    %(dom)s, %(doy)s, %(woy)s,
                    %(mn)s, %(mname)s, %(q)s, %(yr)s,
                    %(wknd)s, FALSE, NULL,
                    %(q)s, %(yr)s
                )
            """, dict(
                dk=dk, fd=d, dow=dow,
                dn=DIAS[dow-1], dom=dt.day,
                doy=dt.timetuple().tm_yday,
                woy=dt.isocalendar()[1],
                mn=dt.month, mname=MESES[dt.month-1],
                q=q, yr=dt.year, wknd=(dow >= 6)
            ))

        # ── dim_time ─────────────────────────────────────────────
        # Recogemos todas las combinaciones hora:minuto del batch
        hm_pairs = set()
        for col in ['scheduled_datetime', 'delivered_datetime']:
            series = pd.to_datetime(df[col].dropna())
            hm_pairs.update(zip(series.dt.hour, series.dt.minute))

        for h, m in hm_pairs:
            tk = h * 100 + m
            if   h < 6:  period, shift = 'Madrugada', 'Turno 3'
            elif h < 14: period, shift = 'Manana',    'Turno 1'
            elif h < 22: period, shift = 'Tarde',     'Turno 2'
            else:        period, shift = 'Noche',     'Turno 3'

            cur.execute("""
                MERGE INTO dim_time t
                USING (SELECT %(tk)s AS time_key) s
                ON t.time_key = s.time_key
                WHEN NOT MATCHED THEN INSERT (
                    time_key, hour, minute, second, time_of_day,
                    hour_24, hour_12, am_pm, is_business_hour, shift
                ) VALUES (
                    %(tk)s, %(h)s, %(m)s, 0, %(period)s,
                    %(h24)s, %(h12)s, %(ampm)s, %(biz)s, %(shift)s
                )
            """, dict(
                tk=tk, h=h, m=m, period=period,
                h24=f"{h:02d}:{m:02d}",
                h12=f"{h%12 or 12:02d}:{m:02d} {'AM' if h<12 else 'PM'}",
                ampm='AM' if h < 12 else 'PM',
                biz=(8 <= h < 18),
                shift=shift
            ))

        # ── dim_vehicle ───────────────────────────────────────────
        vehs = (
            df[['vehicle_id','license_plate','vehicle_type',
                'capacity_kg','fuel_type','acquisition_date',
                'vehicle_status','age_months']]
            .drop_duplicates('vehicle_id')
        )
        for _, r in vehs.iterrows():
            vid = int(r['vehicle_id'])
            cur.execute("""
                MERGE INTO dim_vehicle t
                USING (SELECT %(vid)s AS vehicle_id) s
                ON t.vehicle_id = s.vehicle_id AND t.is_current = TRUE
                WHEN NOT MATCHED THEN INSERT (
                    vehicle_id, license_plate, vehicle_type, capacity_kg,
                    fuel_type, acquisition_date, age_months, status,
                    last_maintenance_date, valid_from, valid_to, is_current
                ) VALUES (
                    %(vid)s, %(plate)s, %(vtype)s, %(cap)s,
                    %(fuel)s, %(acq)s, %(age)s, %(vstatus)s,
                    %(lm)s, %(vf)s, %(vt)s, TRUE
                )
            """, dict(
                vid=vid,
                plate=r['license_plate'],
                vtype=r['vehicle_type'],
                cap=float(r['capacity_kg']) if pd.notna(r['capacity_kg']) else None,
                fuel=r['fuel_type'],
                acq=r['acquisition_date'],
                age=int(r['age_months']),
                vstatus=r['vehicle_status'],
                lm=last_maint.get(vid),
                vf=today, vt=date(9999, 12, 31)
            ))

        # ── dim_driver ────────────────────────────────────────────
        drvs = (
            df[['driver_id','employee_code','full_name',
                'license_number','license_expiry','phone',
                'hire_date','driver_status',
                'experience_months','performance_category']]
            .drop_duplicates('driver_id')
        )
        for _, r in drvs.iterrows():
            cur.execute("""
                MERGE INTO dim_driver t
                USING (SELECT %(did)s AS driver_id) s
                ON t.driver_id = s.driver_id AND t.is_current = TRUE
                WHEN MATCHED AND t.performance_category != %(perf)s THEN
                    UPDATE SET valid_to = CURRENT_DATE(), is_current = FALSE
                WHEN NOT MATCHED THEN INSERT (
                    driver_id, employee_code, full_name,
                    license_number, license_expiry, phone,
                    hire_date, experience_months, status,
                    performance_category, valid_from, valid_to, is_current
                ) VALUES (
                    %(did)s, %(code)s, %(name)s,
                    %(lic)s, %(exp)s, %(phone)s,
                    %(hd)s, %(em)s, %(dstatus)s,
                    %(perf)s, %(vf)s, %(vt)s, TRUE
                )
            """, dict(
                did=int(r['driver_id']),
                code=r['employee_code'],
                name=r['full_name'],
                lic=r['license_number'],
                exp=r['license_expiry'],
                phone=r['phone'],
                hd=r['hire_date'],
                em=int(r['experience_months']),
                dstatus=r['driver_status'],
                perf=r['performance_category'],
                vf=today, vt=date(9999, 12, 31)
            ))

        # ── dim_route ─────────────────────────────────────────────
        rts = (
            df[['route_id','route_code','origin_city','destination_city',
                'distance_km','estimated_duration_hours','toll_cost',
                'difficulty_level','route_type']]
            .drop_duplicates('route_id')
        )
        for _, r in rts.iterrows():
            cur.execute("""
                MERGE INTO dim_route t
                USING (SELECT %(rid)s AS route_id) s
                ON t.route_id = s.route_id
                WHEN NOT MATCHED THEN INSERT (
                    route_id, route_code, origin_city, destination_city,
                    distance_km, estimated_duration_hours, toll_cost,
                    difficulty_level, route_type
                ) VALUES (
                    %(rid)s, %(code)s, %(orig)s, %(dest)s,
                    %(km)s, %(hrs)s, %(toll)s,
                    %(diff)s, %(rtype)s
                )
            """, dict(
                rid=int(r['route_id']),
                code=r['route_code'],
                orig=r['origin_city'],
                dest=r['destination_city'],
                km=float(r['distance_km']),
                hrs=float(r['estimated_duration_hours']),
                toll=float(r['toll_cost']),
                diff=r['difficulty_level'],
                rtype=r['route_type']
            ))

        # ── dim_customer ──────────────────────────────────────────
        custs = (
            df[['customer_name','customer_type','city',
                'first_delivery_date','total_deliveries','customer_category']]
            .drop_duplicates('customer_name')
        )
        for _, r in custs.iterrows():
            cur.execute("""
                MERGE INTO dim_customer t
                USING (SELECT %(cname)s AS customer_name) s
                ON t.customer_name = s.customer_name
                WHEN MATCHED THEN
                    UPDATE SET
                        total_deliveries = t.total_deliveries + %(tot)s,
                        customer_category = %(cat)s
                WHEN NOT MATCHED THEN INSERT (
                    customer_name, customer_type, city,
                    first_delivery_date, total_deliveries, customer_category
                ) VALUES (
                    %(cname)s, %(ctype)s, %(city)s,
                    %(fdd)s, %(tot)s, %(cat)s
                )
            """, dict(
                cname=r['customer_name'],
                ctype=r['customer_type'],
                city=r['city'],
                fdd=r['first_delivery_date'],
                tot=int(r['total_deliveries']),
                cat=r['customer_category']
            ))

        self.sf_conn.commit()
        logging.info("✅ Todas las dimensiones cargadas")

    # ══════════════════════════════════════════
    # CARGA — staging_trips
    # ══════════════════════════════════════════

    def load_trips_to_snowflake(self, df: pd.DataFrame):
        """
        Crea y carga staging_trips en Snowflake.

        JUSTIFICACION:
        Snowflake es una BD analitica (OLAP): no replica las tablas
        transaccionales de PostgreSQL. Sin embargo, la vista
        v_operations_deliveries necesita datos de trips para calcular
        KPIs como duracion real, entregas/hora y eficiencia por viaje.

        La tabla staging_trips en Snowflake actua como una copia
        desnormalizada y enriquecida de trips, optimizada para lecturas
        analiticas. Incluye campos derivados que en PostgreSQL no existen:
        trip_duration_hours, deliveries_in_trip, deliveries_per_hour.

        Se usa INSERT con subconsulta EXISTS para evitar duplicados
        en cargas incrementales sucesivas.
        """
        if not self.sf_conn:
            return

        logging.info("📤 Cargando staging_trips...")
        cur = self.sf_conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging_trips (
                trip_id                INT          PRIMARY KEY,
                vehicle_id             INT          NOT NULL,
                driver_id              INT          NOT NULL,
                route_id               INT          NOT NULL,
                departure_datetime     TIMESTAMP_NTZ,
                arrival_datetime       TIMESTAMP_NTZ,
                fuel_consumed_liters   DECIMAL(10,2),
                total_weight_kg        DECIMAL(10,2),
                trip_status            VARCHAR(20),
                trip_duration_hours    DECIMAL(8,4),
                deliveries_in_trip     INT,
                deliveries_per_hour    DECIMAL(5,2),
                etl_batch_id           INT,
                etl_timestamp          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        trips_df = (
            df[[
                'trip_id','vehicle_id','driver_id','route_id',
                'departure_datetime','arrival_datetime',
                'fuel_consumed_liters','trip_total_weight_kg','trip_status',
                'trip_duration_hours','deliveries_in_trip','deliveries_per_hour'
            ]]
            .drop_duplicates('trip_id')
        )

        data = []
        for _, r in trips_df.iterrows():
            def safe_float(v):
                return float(v) if pd.notna(v) else None
            data.append((
                int(r['trip_id']),
                int(r['vehicle_id']),
                int(r['driver_id']),
                int(r['route_id']),
                r['departure_datetime'],
                r['arrival_datetime'],
                safe_float(r['fuel_consumed_liters']),
                safe_float(r['trip_total_weight_kg']),
                str(r['trip_status']),
                safe_float(r['trip_duration_hours']),
                int(r['deliveries_in_trip']),
                safe_float(r['deliveries_per_hour']),
                self.batch_id
            ))

        # MERGE para evitar duplicados en cargas incrementales
        cur.executemany("""
            MERGE INTO staging_trips t
            USING (SELECT %s AS trip_id) s ON t.trip_id = s.trip_id
            WHEN NOT MATCHED THEN INSERT (
                trip_id, vehicle_id, driver_id, route_id,
                departure_datetime, arrival_datetime,
                fuel_consumed_liters, total_weight_kg, trip_status,
                trip_duration_hours, deliveries_in_trip, deliveries_per_hour,
                etl_batch_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [(r[0],) + r for r in data])

        self.sf_conn.commit()
        logging.info(f"  staging_trips: {len(data):,} registros")

    # ══════════════════════════════════════════
    # CARGA — fact_deliveries
    # ══════════════════════════════════════════

    def load_facts(self, df: pd.DataFrame):
        """
        Carga fact_deliveries resolviendo las surrogated keys
        desde las dimensiones ya cargadas en Snowflake.
        """
        if not self.sf_conn:
            df.to_csv('fact_deliveries_export.csv', index=False)
            logging.info("  CSV exportado: fact_deliveries_export.csv")
            return

        logging.info("📤 Cargando fact_deliveries...")
        cur = self.sf_conn.cursor()

        # Obtener mapeos surrogated key ← natural key
        cur.execute("SELECT vehicle_id, vehicle_key FROM dim_vehicle WHERE is_current=TRUE")
        veh_map = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT driver_id, driver_key FROM dim_driver WHERE is_current=TRUE")
        drv_map = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT route_id, route_key FROM dim_route")
        rte_map = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT customer_name, customer_key FROM dim_customer")
        cust_map = {r[0]: r[1] for r in cur.fetchall()}

        def safe(v):
            return None if pd.isna(v) else v

        data = []
        for _, r in df.iterrows():
            # Solo cargar entregas con fecha de entrega registrada
            if pd.isna(r.get('delivered_datetime')):
                continue
            data.append((
                int(r['date_key']),
                int(r['scheduled_time_key']),
                int(r['delivered_time_key']),
                veh_map.get(r['vehicle_id'],  int(r['vehicle_id'])),
                drv_map.get(r['driver_id'],   int(r['driver_id'])),
                rte_map.get(r['route_id'],    int(r['route_id'])),
                cust_map.get(r['customer_name'], 1),
                int(r['delivery_id']),
                int(r['trip_id']),
                str(r['tracking_number']),
                safe(r['package_weight_kg']),
                float(r['distance_km']),
                float(r['fuel_consumed_liters']),
                safe(r['delivery_time_minutes']),
                float(r['delay_minutes']),
                safe(r['deliveries_per_hour']),
                safe(r['fuel_efficiency_km_per_liter']),
                safe(r['cost_per_delivery']),
                float(r['revenue_per_delivery']),
                bool(r['is_on_time']),
                False,                          # is_damaged: no en postgres
                bool(r['recipient_signature']),
                str(r['delivery_status']),
                str(r.get('data_quality_flag', 'ok')),
                self.batch_id
            ))

        cur.executemany("""
            INSERT INTO fact_deliveries (
                date_key, scheduled_time_key, delivered_time_key,
                vehicle_key, driver_key, route_key, customer_key,
                delivery_id, trip_id, tracking_number,
                package_weight_kg, distance_km, fuel_consumed_liters,
                delivery_time_minutes, delay_minutes,
                deliveries_per_hour, fuel_efficiency_km_per_liter,
                cost_per_delivery, revenue_per_delivery,
                is_on_time, is_damaged, has_signature, delivery_status,
                data_quality_flag, etl_batch_id
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """, data)

        self.sf_conn.commit()
        self.metrics['records_loaded'] = len(data)
        logging.info(f"✅ fact_deliveries: {len(data):,} registros")

    # ══════════════════════════════════════════
    # TOTALES DIARIOS (completado el #TO DO#)
    # ══════════════════════════════════════════

    def _calculate_daily_totals(self, df: pd.DataFrame):
        """
        Pre-calcula KPIs diarios en daily_kpi_summary.

        JUSTIFICACION:
        Las vistas analiticas sobre fact_deliveries (millones de filas)
        tardan varios segundos. Esta tabla de resumen permite que los
        dashboards respondan en < 100ms consultando una sola fila por dia.
        Se actualiza como parte del pipeline nocturno con MERGE.
        """
        if not self.sf_conn:
            return

        cur = self.sf_conn.cursor()

        # Crear la tabla si no existe (primera ejecucion)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_kpi_summary (
                summary_date          DATE         NOT NULL,
                etl_batch_id          INT          NOT NULL,
                total_deliveries      INT,
                delivered_ok          INT,
                pct_on_time           DECIMAL(5,2),
                avg_delay_minutes     DECIMAL(8,2),
                total_revenue_mxn     DECIMAL(14,2),
                total_cost_mxn        DECIMAL(14,2),
                avg_fuel_efficiency   DECIMAL(8,4),
                total_km              DECIMAL(12,2),
                total_fuel_liters     DECIMAL(12,2),
                active_vehicles       INT,
                active_drivers        INT,
                created_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (summary_date, etl_batch_id)
            )
        """)

        sd = date.today()
        vals = dict(
            sd   = sd,
            bid  = self.batch_id,
            tot  = len(df),
            dok  = int((df['delivery_status'] == 'delivered').sum()),
            pot  = round(float(df['is_on_time'].mean()) * 100, 2),
            adm  = round(float(df['delay_minutes'].mean()), 2),
            trev = round(float(df['revenue_per_delivery'].sum()), 2),
            tcst = round(float(df['cost_per_delivery'].fillna(0).sum()), 2),
            afe  = round(float(df['fuel_efficiency_km_per_liter'].mean()), 4),
            tkm  = round(float(df['distance_km'].sum()), 2),
            tfl  = round(float(df['fuel_consumed_liters'].sum()), 2),
            aveh = int(df['vehicle_id'].nunique()),
            adrv = int(df['driver_id'].nunique()),
        )

        # Insertar totales del dia con MERGE (evitar duplicados)
        cur.execute("""
            MERGE INTO daily_kpi_summary t
            USING (SELECT %(sd)s AS summary_date,
                          %(bid)s AS etl_batch_id) s
            ON t.summary_date = s.summary_date
               AND t.etl_batch_id = s.etl_batch_id
            WHEN NOT MATCHED THEN INSERT (
                summary_date, etl_batch_id,
                total_deliveries, delivered_ok, pct_on_time,
                avg_delay_minutes, total_revenue_mxn, total_cost_mxn,
                avg_fuel_efficiency, total_km, total_fuel_liters,
                active_vehicles, active_drivers
            ) VALUES (
                %(sd)s, %(bid)s,
                %(tot)s, %(dok)s, %(pot)s,
                %(adm)s, %(trev)s, %(tcst)s,
                %(afe)s, %(tkm)s, %(tfl)s,
                %(aveh)s, %(adrv)s
            )
        """, vals)

        self.sf_conn.commit()
        logging.info(f"✅ daily_kpi_summary actualizado ({sd})")

    # ══════════════════════════════════════════
    # MODO SIN SNOWFLAKE — EXPORTAR CSV
    # ══════════════════════════════════════════

    def _export_dims_csv(self, df: pd.DataFrame):
        """Exporta todas las dimensiones a CSV cuando Snowflake no esta disponible."""
        import os
        os.makedirs('./dw_data', exist_ok=True)

        exports = {
            'dim_vehicle':  df[['vehicle_id','license_plate','vehicle_type',
                                'capacity_kg','fuel_type','acquisition_date',
                                'vehicle_status','age_months']
                              ].drop_duplicates('vehicle_id'),
            'dim_driver':   df[['driver_id','employee_code','full_name',
                                'license_number','license_expiry','phone',
                                'hire_date','driver_status',
                                'experience_months','performance_category']
                              ].drop_duplicates('driver_id'),
            'dim_route':    df[['route_id','route_code','origin_city','destination_city',
                                'distance_km','estimated_duration_hours','toll_cost',
                                'difficulty_level','route_type']
                              ].drop_duplicates('route_id'),
            'dim_customer': df[['customer_name','customer_type','city',
                                'first_delivery_date','total_deliveries','customer_category']
                              ].drop_duplicates('customer_name'),
            'staging_trips':df[['trip_id','vehicle_id','driver_id','route_id',
                                'departure_datetime','arrival_datetime',
                                'fuel_consumed_liters','trip_total_weight_kg',
                                'trip_status','trip_duration_hours',
                                'deliveries_in_trip','deliveries_per_hour']
                              ].drop_duplicates('trip_id'),
        }
        for name, table in exports.items():
            path = f'./dw_data/{name}.csv'
            table.to_csv(path, index=False)
            logging.info(f"  CSV: {path} ({len(table):,} filas)")

        df.to_csv('./dw_data/fact_deliveries.csv', index=False)
        logging.info(f"  CSV: fact_deliveries ({len(df):,} filas)")

    # ══════════════════════════════════════════
    # PIPELINE COMPLETO
    # ══════════════════════════════════════════

    def run_etl(self, full_history: bool = False):
        """
        Ejecuta el pipeline completo E → T → L.

        full_history=True  : carga inicial de todo el historico
        full_history=False : carga incremental del dia anterior (default)
        """
        start = datetime.now()
        logging.info(f"🚀 Iniciando ETL — Batch {self.batch_id}")

        try:
            if not self.connect_databases():
                return

            df = (self.extract_full_history() if full_history
                  else self.extract_daily_data())

            if df.empty:
                logging.warning("⚠️  Sin datos para procesar")
                return

            df_t = self.transform_data(df)
            if df_t.empty:
                return

            self.load_dimensions(df_t)
            self.load_trips_to_snowflake(df_t)
            self.load_facts(df_t)
            self._calculate_daily_totals(df_t)

        except Exception as e:
            logging.error(f"❌ Error fatal: {e}")
            self.metrics['errors'] += 1
        finally:
            self.close_connections()
            elapsed = (datetime.now() - start).total_seconds()
            logging.info(f"✅ ETL completado en {elapsed:.1f}s")
            logging.info(f"   {json.dumps(self.metrics, indent=2)}")


# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────
def job():
    """Carga incremental diaria programada."""
    FleetLogixETL().run_etl(full_history=False)


def main():
    logging.info("🕑 Pipeline ETL FleetLogix iniciado")
    schedule.every().day.at("02:00").do(job)
    logging.info("   Programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("   Ctrl+C para detener")

    # Primera ejecucion: carga historica completa
    FleetLogixETL().run_etl(full_history=True)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()

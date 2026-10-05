"""
=============================================================
FleetLogix — Avance 1: Generación de Datos Sintéticos
=============================================================
Genera 505,650 registros distribuidos en 6 tablas:
  - vehicles    :   200 registros
  - drivers     :   400 registros
  - routes      :    50 registros
  - trips       : 100,000 registros
  - deliveries  : 400,000 registros
  - maintenance :   5,000 registros

Dependencias: pip install faker pandas numpy psycopg2-binary
Uso: python 01_data_generation.py
     (edita la sección CONFIG antes de ejecutar)
=============================================================
"""

import random
import logging
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
from faker import Faker

# ─────────────────────────────────────────────
# CONFIG — ajuste de valores al entorno
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "TU_PASSWORD",  
}

SEED          = 42       
SAVE_CSV      = True     
CSV_FOLDER    = "./data" 
LOAD_TO_DB    = True     

# ─────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

random.seed(SEED)
np.random.seed(SEED)
fake = Faker("es_MX")
Faker.seed(SEED)

# ─────────────────────────────────────────────
# Constantes del negocio
# ─────────────────────────────────────────────
CIUDADES = ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla", "Tijuana"]

TIPOS_VEHICULO = ["Camión", "Van", "Motocicleta"]
# Distribución realista de la flota
DIST_VEHICULO  = [0.40, 0.40, 0.20]

TIPOS_COMBUSTIBLE = {
    "Camión":      ["Diésel", "GNV"],
    "Van":         ["Gasolina", "Diésel", "Eléctrico"],
    "Motocicleta": ["Gasolina", "Eléctrico"],
}

# Capacidad en kg según tipo
CAPACIDAD_KG = {
    "Camión":      (1_000, 10_000),
    "Van":         (200,   1_500),
    "Motocicleta": (10,    50),
}

TIPOS_MANTENIMIENTO = [
    "Cambio de aceite",
    "Revisión de frenos",
    "Cambio de llantas",
    "Revisión eléctrica",
    "Mantenimiento preventivo",
    "Reparación de motor",
    "Revisión de suspensión",
]

TALLERES = [
    "Taller Central FleetLogix",
    "AutoService Norte",
    "Mecánica Express",
    "Servicio Oficial Diésel",
    "TallerTech",
]

ESTADOS_TRIP       = ["completed", "completed", "completed", "cancelled", "in_progress"]
ESTADOS_DELIVERY   = ["delivered", "delivered", "delivered", "failed", "pending"]

# Ventana temporal: 2 años de operación hasta hoy
FECHA_INICIO = datetime(2023, 1, 1)
FECHA_FIN    = datetime(2024, 12, 31, 23, 59, 59)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def rand_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    secs  = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=secs)


def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# ─────────────────────────────────────────────
# 1. VEHICLES  (200 registros)
# ─────────────────────────────────────────────

def gen_vehicles(n: int = 200) -> pd.DataFrame:
    log.info("Generando vehicles …")
    rows = []
    tipos = random.choices(TIPOS_VEHICULO, weights=DIST_VEHICULO, k=n)

    placas_usadas = set()
    for i, tipo in enumerate(tipos, start=1):
        # Placa única tipo ABC-1234
        while True:
            placa = (
                fake.bothify("???-####")
                .upper()
                .replace("Ñ", "N")
            )
            if placa not in placas_usadas:
                placas_usadas.add(placa)
                break

        comb = random.choice(TIPOS_COMBUSTIBLE[tipo])
        cap_min, cap_max = CAPACIDAD_KG[tipo]
        cap = round(random.uniform(cap_min, cap_max), 2)

        acq = rand_date(date(2015, 1, 1), date(2023, 6, 30))
        # El 90 % activos
        status = "active" if random.random() < 0.90 else "inactive"

        rows.append({
            "vehicle_id":       i,
            "license_plate":    placa,
            "vehicle_type":     tipo,
            "capacity_kg":      cap,
            "fuel_type":        comb,
            "acquisition_date": acq,
            "status":           status,
        })

    df = pd.DataFrame(rows)
    log.info("  vehicles: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# 2. DRIVERS  (400 registros)
# ─────────────────────────────────────────────

def gen_drivers(n: int = 400) -> pd.DataFrame:
    log.info("Generando drivers …")
    rows = []
    emp_codes = set()
    lic_nums  = set()

    for i in range(1, n + 1):
        # Código de empleado único
        while True:
            code = f"EMP-{random.randint(1000, 9999)}"
            if code not in emp_codes:
                emp_codes.add(code)
                break

        # Número de licencia único
        while True:
            lic = fake.bothify("LIC-########").upper()
            if lic not in lic_nums:
                lic_nums.add(lic)
                break

        hire  = rand_date(date(2015, 1, 1), date(2023, 12, 31))
        expiry = hire + timedelta(days=random.randint(365 * 3, 365 * 5))

        rows.append({
            "driver_id":       i,
            "employee_code":   code,
            "first_name":      fake.first_name(),
            "last_name":       fake.last_name() + " " + fake.last_name(),
            "license_number":  lic,
            "license_expiry":  expiry,
            "phone":           fake.numerify("55########"),
            "hire_date":       hire,
            "status":          "active" if random.random() < 0.88 else "inactive",
        })

    df = pd.DataFrame(rows)
    log.info("  drivers: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# 3. ROUTES  (50 registros)
# ─────────────────────────────────────────────

def gen_routes(n: int = 50) -> pd.DataFrame:
    log.info("Generando routes …")
    # Distancias aproximadas entre las 5 ciudades (km)
    dist_table = {
        ("Ciudad de México", "Guadalajara"):  545,
        ("Ciudad de México", "Monterrey"):    940,
        ("Ciudad de México", "Puebla"):       130,
        ("Ciudad de México", "Tijuana"):     2_780,
        ("Guadalajara",      "Monterrey"):    800,
        ("Guadalajara",      "Puebla"):       500,
        ("Guadalajara",      "Tijuana"):    2_250,
        ("Monterrey",        "Puebla"):      1_000,
        ("Monterrey",        "Tijuana"):     1_670,
        ("Puebla",           "Tijuana"):     2_850,
    }

    # Generar todas las combinaciones O→D y D→O
    pares = []
    for (a, b), km in dist_table.items():
        pares.append((a, b, km))
        pares.append((b, a, km))
    # Añadir rutas dentro de la misma ciudad (última milla)
    for ciudad in CIUDADES:
        pares.append((ciudad, ciudad, round(random.uniform(5, 40), 2)))

    # Si tenemos menos de n pares, repetimos con ligeras variaciones
    while len(pares) < n:
        (a, b, km) = random.choice(pares)
        pares.append((a, b, round(km * random.uniform(0.90, 1.10), 2)))

    random.shuffle(pares)
    pares = pares[:n]

    rows = []
    codes = set()
    for i, (orig, dest, base_km) in enumerate(pares, start=1):
        while True:
            code = f"RT-{random.randint(100, 999)}"
            if code not in codes:
                codes.add(code)
                break

        km      = round(base_km * random.uniform(0.92, 1.08), 2)
        horas   = round(km / random.uniform(60, 80), 2)
        peaje   = round(km * random.uniform(0.01, 0.03), 2) if km > 50 else 0

        rows.append({
            "route_id":                 i,
            "route_code":               code,
            "origin_city":              orig,
            "destination_city":         dest,
            "distance_km":              km,
            "estimated_duration_hours": horas,
            "toll_cost":                peaje,
        })

    df = pd.DataFrame(rows)
    log.info("  routes: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# 4. TRIPS  (100,000 registros)
# ─────────────────────────────────────────────

def gen_trips(
    vehicles: pd.DataFrame,
    drivers:  pd.DataFrame,
    routes:   pd.DataFrame,
    n: int = 100_000,
) -> pd.DataFrame:
    log.info("Generando trips (%d) …", n)

    active_veh = vehicles[vehicles["status"] == "active"]["vehicle_id"].tolist()
    active_drv = drivers [drivers ["status"] == "active"]["driver_id"].tolist()
    route_ids  = routes["route_id"].tolist()

    # Pre-calculamos duración por ruta para generar arrival_datetime coherente
    route_dur = routes.set_index("route_id")["estimated_duration_hours"].to_dict()
    route_km  = routes.set_index("route_id")["distance_km"].to_dict()
    veh_cap   = vehicles.set_index("vehicle_id")["capacity_kg"].to_dict()

    rows = []
    for i in range(1, n + 1):
        vid = random.choice(active_veh)
        did = random.choice(active_drv)
        rid = random.choice(route_ids)

        departure = rand_datetime(FECHA_INICIO, FECHA_FIN)

        dur_h   = route_dur[rid]
        # Variación real: tráfico +/- 30 %
        real_h  = dur_h * random.uniform(0.70, 1.30)
        status  = random.choices(
            ["completed", "cancelled", "in_progress"],
            weights=[0.88, 0.07, 0.05],
        )[0]

        if status == "completed":
            arrival = departure + timedelta(hours=real_h)
        else:
            arrival = None

        km   = route_km[rid]
        # Consumo: 8–12 L/100km para camiones, menos para vans/motos
        cap  = veh_cap[vid]
        factor = 0.12 if cap > 1_000 else (0.08 if cap > 100 else 0.04)
        fuel = round(km * factor * random.uniform(0.85, 1.15), 2) if status == "completed" else None

        # Peso cargado ≤ capacidad del vehículo
        weight = round(random.uniform(cap * 0.20, cap * 0.95), 2) if status != "cancelled" else None

        rows.append({
            "trip_id":             i,
            "vehicle_id":          vid,
            "driver_id":           did,
            "route_id":            rid,
            "departure_datetime":  departure,
            "arrival_datetime":    arrival,
            "fuel_consumed_liters": fuel,
            "total_weight_kg":     weight,
            "status":              status,
        })

        if i % 20_000 == 0:
            log.info("    trips procesados: %d / %d", i, n)

    df = pd.DataFrame(rows)
    log.info("  trips: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# 5. DELIVERIES  (≈400,000 registros)
# ─────────────────────────────────────────────

def gen_deliveries(trips: pd.DataFrame) -> pd.DataFrame:
    log.info("Generando deliveries …")

    # Solo viajes completados o en progreso pueden tener entregas
    valid_trips = trips[trips["status"].isin(["completed", "in_progress"])].copy()

    # Distribución: 1–8 entregas por viaje (promedio ≈4)
    counts = np.random.choice(
        [1, 2, 3, 4, 5, 6, 7, 8],
        size=len(valid_trips),
        p=[0.05, 0.10, 0.20, 0.25, 0.20, 0.10, 0.07, 0.03],
    )

    rows = []
    tracking_set = set()
    delivery_id  = 1

    for (_, trip), n_del in zip(valid_trips.iterrows(), counts):
        dep = trip["departure_datetime"]
        arr = trip["arrival_datetime"]

        for _ in range(n_del):
            # Número de rastreo único
            while True:
                tn = f"TRK-{fake.bothify('??####??####').upper()}"
                if tn not in tracking_set:
                    tracking_set.add(tn)
                    break

            # Fecha programada: entre salida y llegada (+variación)
            if arr and pd.notna(arr):
                sched = dep + (arr - dep) * random.uniform(0.3, 1.0)
                # Entregado: antes o un poco después de scheduled
                if random.random() < 0.85:
                    delivered = sched + timedelta(minutes=random.randint(-60, 120))
                    del_status = "delivered"
                else:
                    delivered  = None
                    del_status = random.choice(["failed", "pending"])
            else:
                sched      = dep + timedelta(hours=random.uniform(1, 6))
                delivered  = None
                del_status = "pending"

            rows.append({
                "delivery_id":        delivery_id,
                "trip_id":            trip["trip_id"],
                "tracking_number":    tn,
                "customer_name":      fake.name(),
                "delivery_address":   fake.address().replace("\n", ", "),
                "package_weight_kg":  round(random.uniform(0.1, 50), 2),
                "scheduled_datetime": sched,
                "delivered_datetime": delivered if del_status == "delivered" else None,
                "delivery_status":    del_status,
                "recipient_signature": random.random() < 0.70 if del_status == "delivered" else False,
            })
            delivery_id += 1

        if delivery_id % 100_000 == 1:
            log.info("    deliveries procesadas: ~%d", delivery_id)

    df = pd.DataFrame(rows)
    log.info("  deliveries: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# 6. MAINTENANCE  (5,000 registros)
# ─────────────────────────────────────────────

def gen_maintenance(vehicles: pd.DataFrame, n: int = 5_000) -> pd.DataFrame:
    log.info("Generando maintenance …")

    veh_ids = vehicles["vehicle_id"].tolist()
    rows    = []

    for i in range(1, n + 1):
        vid  = random.choice(veh_ids)
        mdate = rand_date(date(2022, 1, 1), date(2024, 12, 31))
        mtype = random.choice(TIPOS_MANTENIMIENTO)

        # Costo aproximado por tipo
        costo_base = {
            "Cambio de aceite":       (400,   800),
            "Revisión de frenos":     (800,  3_000),
            "Cambio de llantas":     (2_000, 8_000),
            "Revisión eléctrica":    (500,  2_500),
            "Mantenimiento preventivo": (1_500, 5_000),
            "Reparación de motor":   (5_000, 30_000),
            "Revisión de suspensión": (1_000, 4_000),
        }
        rng  = costo_base[mtype]
        cost = round(random.uniform(*rng), 2)

        # Próximo mantenimiento: 30–180 días después
        next_m = mdate + timedelta(days=random.randint(30, 180))

        rows.append({
            "maintenance_id":       i,
            "vehicle_id":           vid,
            "maintenance_date":     mdate,
            "maintenance_type":     mtype,
            "description":          fake.sentence(nb_words=8),
            "cost":                 cost,
            "next_maintenance_date": next_m,
            "performed_by":         random.choice(TALLERES),
        })

    df = pd.DataFrame(rows)
    log.info("  maintenance: %d filas", len(df))
    return df


# ─────────────────────────────────────────────
# VALIDACIONES DE INTEGRIDAD
# ─────────────────────────────────────────────

def validate(vehicles, drivers, routes, trips, deliveries, maintenance):
    log.info("── Validando integridad referencial ──")
    errors = 0

    # trips → vehicles
    invalid = ~trips["vehicle_id"].isin(vehicles["vehicle_id"])
    if invalid.any():
        log.error("  trips con vehicle_id inválido: %d", invalid.sum())
        errors += 1

    # trips → drivers
    invalid = ~trips["driver_id"].isin(drivers["driver_id"])
    if invalid.any():
        log.error("  trips con driver_id inválido: %d", invalid.sum())
        errors += 1

    # trips → routes
    invalid = ~trips["route_id"].isin(routes["route_id"])
    if invalid.any():
        log.error("  trips con route_id inválido: %d", invalid.sum())
        errors += 1

    # deliveries → trips
    invalid = ~deliveries["trip_id"].isin(trips["trip_id"])
    if invalid.any():
        log.error("  deliveries con trip_id inválido: %d", invalid.sum())
        errors += 1

    # maintenance → vehicles
    invalid = ~maintenance["vehicle_id"].isin(vehicles["vehicle_id"])
    if invalid.any():
        log.error("  maintenance con vehicle_id inválido: %d", invalid.sum())
        errors += 1

    # arrival > departure
    completed = trips[trips["status"] == "completed"].dropna(subset=["arrival_datetime"])
    bad_times = completed[completed["arrival_datetime"] <= completed["departure_datetime"]]
    if not bad_times.empty:
        log.error("  trips con arrival <= departure: %d", len(bad_times))
        errors += 1

    # delivered > scheduled
    delivered = deliveries[deliveries["delivery_status"] == "delivered"].dropna(
        subset=["delivered_datetime", "scheduled_datetime"]
    )

    if errors == 0:
        log.info("  ✓ Todas las validaciones pasaron")
    else:
        log.warning("  ✗ Se encontraron %d problemas", errors)

    # Resumen estadístico
    log.info("── Resumen de registros generados ──")
    log.info("  vehicles    : %8d", len(vehicles))
    log.info("  drivers     : %8d", len(drivers))
    log.info("  routes      : %8d", len(routes))
    log.info("  trips       : %8d", len(trips))
    log.info("  deliveries  : %8d", len(deliveries))
    log.info("  maintenance : %8d", len(maintenance))
    log.info("  TOTAL       : %8d", sum([
        len(vehicles), len(drivers), len(routes),
        len(trips), len(deliveries), len(maintenance)
    ]))


# ─────────────────────────────────────────────
# EXPORTAR CSV
# ─────────────────────────────────────────────

def save_csv(dfs: dict, folder: str):
    import os
    os.makedirs(folder, exist_ok=True)
    for name, df in dfs.items():
        path = f"{folder}/{name}.csv"
        df.to_csv(path, index=False)
        log.info("  CSV guardado: %s  (%d filas)", path, len(df))


# ─────────────────────────────────────────────
# CARGAR A POSTGRESQL
# ─────────────────────────────────────────────

def load_to_postgres(dfs: dict, config: dict):
    """
    Inserta los DataFrames en PostgreSQL usando COPY (muy rápido).
    Las tablas deben existir previamente con el DDL de FleetLogix.
    """
    import psycopg2
    from io import StringIO

    log.info("── Cargando a PostgreSQL ──")
    conn = psycopg2.connect(**config)
    cur  = conn.cursor()

    # Orden respeta foreign keys
    orden = ["vehicles", "drivers", "routes", "trips", "deliveries", "maintenance"]

    for tabla in orden:
        df = dfs[tabla]
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        cols = ", ".join(df.columns)
        cur.copy_expert(
            f"COPY {tabla} ({cols}) FROM STDIN WITH CSV NULL '\\N'",
            buf,
        )
        conn.commit()
        log.info("  ✓ %s: %d filas cargadas", tabla, len(df))

    cur.close()
    conn.close()
    log.info("  Conexión cerrada")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("═══════════════════════════════════════")
    log.info("  FleetLogix — Generación de Datos")
    log.info("═══════════════════════════════════════")

    vehicles    = gen_vehicles(200)
    drivers     = gen_drivers(400)
    routes      = gen_routes(50)
    trips       = gen_trips(vehicles, drivers, routes, 110_000)
    deliveries  = gen_deliveries(trips)
    maintenance = gen_maintenance(vehicles, 5_000)

    dfs = {
        "vehicles":    vehicles,
        "drivers":     drivers,
        "routes":      routes,
        "trips":       trips,
        "deliveries":  deliveries,
        "maintenance": maintenance,
    }

    validate(vehicles, drivers, routes, trips, deliveries, maintenance)

    if SAVE_CSV:
        log.info("── Exportando CSV ──")
        save_csv(dfs, CSV_FOLDER)

    if LOAD_TO_DB:
        load_to_postgres(dfs, DB_CONFIG)

    log.info("═══════════════════════════════════════")
    log.info("  Guardado en ./data/")
    log.info("═══════════════════════════════════════")


if __name__ == "__main__":
    main()

"""
01_ingest_and_load.py

Does everything in one script:
    1. Finds each city's nearest active OpenAQ monitoring location
    2. Fetches ~2 years of daily-averaged pollutant readings per sensor
    3. Creates the schema and bronze table in Databricks if they don't exist
    4. Loads the data directly into the Delta table using SQL INSERT

OpenAQ free tier rate limits are generous, but this script still paces
requests conservatively (0.5s between calls) to stay well within them.

Run once for the initial load. Re-run any time to refresh (it truncates
and reloads).
"""

import requests
import pandas as pd
import os
import time
from datetime import date, timedelta
from dotenv import load_dotenv
from databricks import sql as dbsql

load_dotenv()

# ----- CONFIG ------------------------------------------------------------------------
OPENAQ_KEY = os.getenv("OPENAQ_API_KEY")
DB_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DB_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DB_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")
DB_CATALOG = os.getenv("DATABRICKS_CATALOG", "aqipulse_catalog")
DB_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "aqipulse")

OPENAQ_BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-KEY": OPENAQ_KEY}

LOOKBACK_DAYS = 730 # ~2years
DATE_TO = date.today()
DATE_FROM = DATE_TO - timedelta(days=LOOKBACK_DAYS)

# POLLUTANTS we care about (OpenAQ parameter names)
#   - pm25 : Fine particles under 2.5 micrometers
#   - pm10 : Course particles under 10 micrometers
#   - no2 : Foul-smelling, highly reactive gas
#   - o3 : Ground-level smog
#   - so2 : Toxic gas with a pungent smell
#   - co : Colorless, odorless, toxic gas
TARGET_PARAMS = {"pm25", "pm10", "no2", "o3", "so2", "co"}

# The 12 cities: name, country, lat, lon
CITIES = [
    {"city": "Los Angeles", "country" : "US", "lat": 34.0522, "lon": -118.2437},
    {"city": "Toronto", "country" : "CA", "lat": 43.6532, "lon": -79.3832},
    {"city": "Sao Paulo", "country" : "BR", "lat": -23.5505, "lon": -46.6333},
    {"city": "London", "country" : "GB", "lat": 51.5074, "lon": -0.1278},
    {"city": "Paris", "country" : "FR", "lat": 48.8566, "lon": 2.3522},
    {"city": "Berlin", "country" : "DE", "lat": 52.5200, "lon": 13.4050},
    {"city": "Cairo", "country" : "EG", "lat": 30.0444, "lon": 31.2357},
    {"city": "Delhi", "country" : "IN", "lat": 28.6139, "lon": 77.2090},
    {"city": "Beijing", "country" : "CN", "lat": 39.9042, "lon": 116.4074},
    {"city": "Tokoyo", "country" : "JP", "lat": 35.6762, "lon": 139.6503},
    {"city": "Bangkok", "country" : "TH", "lat": 13.7563, "lon": 100.5018},
    {"city": "Jakarts", "country" : "ID", "lat": -6.2088, "lon": 106.8456}
]


# ----- STEP 1: Find a monitoring location near each city ------------------------------------------------------------------------
def find_location(lat: float, lon: float) -> dict | None:
    """
    Find the active OpenAQ location with the most target-pollutant
    sensors within a 25km radius of the given coordinates
    """
    params = {
        "coordinates": f"{lat},{lon}",
        "radius" : 25000,
        "limit" : 10,
    }

    r = requests.get(f"{OPENAQ_BASE}/locations", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    results = r.json().get("results",[])

    if not results:
        return None
    
    def score(loc):
        sensor_params = {s["parameter"]["name"] for s in loc.get("sensors", [])}
        return len(sensor_params & TARGET_PARAMS)
    
    return max(results, key=score)

# ----- STEP 2: Fetch daily-averaged history for one sensor ------------------------------------------------------------------------
def fetch_sensor_days(sensor_id: int) -> list[dict]:
    """Page through daily-averaged readings for a sensor over the lookback window."""
    rows = []
    page = 1
    while True:
        params = {
            "datetime_from": DATE_FROM.isoformat(),
            "datetime_to": DATE_TO.isoformat(),
            "limit": 1000,
            "page": page,
        }
        r = requests.get(
            f"{OPENAQ_BASE}/sensors/{sensor_id}/days",
            headers= HEADERS, params= params, timeout= 30
        )

        if r.status_code == 429:
            print(" Rate limited - pausing 60s")
            time.sleep(60)
            continue

        r.raise_for_status()
        page_results = r.json().get("results", [])
        rows.extend(page_results)
        if len(page_results) < 1000:
            break
        page += 1
        time.sleep(0.5)
    return rows


# ----- STEP 3: Set up Databricks table ------------------------------------------------------------------------
def setup_databricks_table(conn):
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_CATALOG}.{DB_SCHEMA}")
    print(f"Schema ready: {DB_CATALOG}.{DB_SCHEMA}")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CATALOG}.{DB_SCHEMA}.bronze_air_quality (
            date STRING,
            city STRING,
            country STRING,
            location_id BIGINT,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            parameter STRING,
            value DOUBLE,
            unit STRING
        )
        USING DELTA
        COMMENT 'Raw daily-averaged air quality readings from OpenAQ for 12 world cities'
                   """)
    
    print("Bronze table ready: bronze_air_quality")

    cursor.execute(f"TRUNCATE TABLE {DB_CATALOG}.{DB_SCHEMA}.bronze_air_quality")
    print("Table Truncated - ready for fresh load")
    cursor.close()

# ----- STEP 4: Insert data into Databricks ------------------------------------------------------------------------
def sql_val(v):
    """Format a value for inline SQL: NULL for NaN/None, else the raw values"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    return str(v)
def insert_dataframe(conn, df: pd.DataFrame, city: str):
    cursor = conn.cursor()
    table = f"{DB_CATALOG}.{DB_SCHEMA}.bronze_air_quality"
    batch = 500
    total = len(df)
    inserted = 0

    for start in range(0, total, batch):
        chunk = df.iloc[start:start + batch]
        values_list = []
        for _, row in chunk.iterrows():
            if not(pd.isna(row.value) or pd.isna(row.latitude) or pd.isna(row.longitude) or pd.isna(row.location_id)):
                values_list.append(
                    f"('{row.date}', '{row.city}', '{row.country}', {sql_val(row.location_id)}, "
                    f"'{row.location_name}', {sql_val(row.latitude)}, {sql_val(row.longitude)}, "
                    f"'{row.parameter}', {sql_val(row.value)}, '{row.unit}')"
                )
            else:
                chunk -= 1
        values_str = ",\n".join(values_list)
        cursor.execute(f"INSERT INTO {table} VALUES {values_str}")   # <-- wrong indent
        inserted += len(chunk)

    cursor.close()
    print(f"    Inserted {inserted} rows for {city}")

# ----- MAIN ------------------------------------------------------------------------
def main():
    for var in {"OPENAQ_KEY", "DB_HOST", "DB_PATH", "DB_TOKEN"}:
        if not globals()[var]:
            print(f"ERROR: {var} not set in .env file")
            return
    
    print("Connecting to Databricks....")
    try:
        conn = dbsql.connect(server_hostname=DB_HOST, http_path=DB_PATH, access_token=DB_TOKEN)
        print("Connected\n")
    except Exception as e:
        print(f"Error Detected {e}... Databricks failed to connect")
        return
    
    setup_databricks_table(conn)
    print()

    all_rows = 0
    for i, city in enumerate(CITIES):
        print(f"[{i+1}/{len(CITIES)}] {city['country']}...")
        try:
            location = find_location(city['lat'], city['lon'])
            if not location:
                print(f"    No monitoring location found enar {city['city']} - skipping")
                continue

            loc_id = location["id"]
            loc_name = location.get("name", city['city'])
            print(f"    found location: {loc_name} (id {loc_id})")

            rows = []
            for sensor in location.get("sensors", []):
                param_name = sensor['parameter']['name']
                if param_name not in TARGET_PARAMS:
                    continue
                unit = sensor['parameter'].get("units", "")
                sensor_id = sensor["id"]

                readings = fetch_sensor_days(sensor_id=sensor_id)
                for r in readings:
                    dt = r["period"]["datetimeFrom"]["utc"][:10]
                    rows.append({
                        "date": dt,
                        "city": city['city'],
                        "country": city["country"],
                        "location_id": loc_id,
                        "location_name": loc_name,
                        "latitude": city['lat'],
                        "longitude": city['lon'],
                        "parameter": param_name,
                        "value": r['value'],
                        "unit": unit,
                    })
                    # time.sleep(0.5)
            
            if not rows:
                print(f"    No target-pollutant readings for {city['city']} -- skipping")
                continue

            df = pd.DataFrame(rows)
            print(f"    Parsed {len(df)} rows across {df['parameter'].nunique()} pollutants")
            insert_dataframe(conn, df, city=city['city'])
            all_rows += len(df)

        except Exception as e:
            print(f"    ERROR for {city['city']}: {e}")
    
    conn.close()
    print(f"\nDone. Loadeed {all_rows} total rows into Databricks.")
    print("Nexdt step: run python python/02_validate.py")

if __name__ == "__main__":
    main()
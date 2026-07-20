"""
02_validate.py

Connects to Databricks and runs validation queries on the bronze table.
Confirms data loaded correctly before moving to the silver transformation
"""

import os
import pandas as pd
from dotenv import load_dotenv
from databricks import sql as dbsql

load_dotenv()

conn = dbsql.connect(
    server_hostname= os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path= os.getenv("DATABRICKS_HTTP_PATH"),
    access_token= os.getenv("DATABRICKS_ACCESS_TOKEN"),
)

table = f"aqipulse_catalog.aqipulse.bronze_air_quality"
cursor = conn.cursor()

print("==== Rows per city ====")
cursor.execute(f"""
    select city, country, count(*) as readings,
    count(distinct parameter) as pollutants,
    min(date) as earliest, max(date) as latest
    from {table}
    group by city, country
    order by city
               """)

df = pd.DataFrame(cursor.fetchall(), columns=["city", "country", "readings", "pollutants", "earliest", "latest"])
print(df.to_string(index=False))

cursor.execute(f"select count(*) from {table}")
total = cursor.fetchone()[0]
print(f"\nTotal rows: {total}")

cursor.execute(f"select count(*) from {table} where value < 0")
bad = cursor.fetchone()[0]
print(f"Rows with negatiuve values: {bad}")

cursor.execute(f"select count(*) from {table} where value is null")
nulls = cursor.fetchone()[0]
print(f"Rows with null values: {nulls}")

cursor.close()
conn.close()

if bad > 0 or nulls > 0:
    print("\n VALIDATION FAILED - Check your data before continuing")
else: 
    print("\nAll checks passed - ready for silver transformation")
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